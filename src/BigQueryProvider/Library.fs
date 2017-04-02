module BigQueryProvider

module ProcessHelper = 
  open System.Diagnostics

  type ProcessResult = { exitCode : int; stdout : string; stderr : string }
  let executeProcess exec cmdLine = 
    let psi = ProcessStartInfo(exec, cmdLine)
    psi.UseShellExecute <- false
    psi.RedirectStandardOutput <- true
    psi.RedirectStandardError <- true
    psi.CreateNoWindow <- true        
    let p = System.Diagnostics.Process.Start(psi) 
    let output = System.Text.StringBuilder()
    let error = System.Text.StringBuilder()
    p.OutputDataReceived.Add(fun args -> output.Append(args.Data) |> ignore)
    p.ErrorDataReceived.Add(fun args -> error.Append(args.Data) |> ignore)
    p.BeginErrorReadLine()
    p.BeginOutputReadLine()
    p.WaitForExit()
    { exitCode = p.ExitCode; stdout = output.ToString(); stderr = error.ToString() }

module BigQueryAnalyze = 

  let executeCmdBq = ProcessHelper.executeProcess "bq"

  let analyzeQueryRaw queryStr = 
    let cmdLine = sprintf "query --format=json --dry_run=true --use_legacy_sql=false '%s'" queryStr
    executeCmdBq cmdLine

module Library = 
  let hello num = 
    let query = """
    SELECT
  name
  , cnt
FROM `tomas.dflow`
WHERE cnt = @cnt
LIMIT 30
"""
    let res = BigQueryAnalyze.analyzeQueryRaw query
    printfn "%A" res
    42

open ProviderImplementation.ProvidedTypes
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open System.Reflection
open FSharp.Data.SqlClient


module DesignTime = 
  let createCommandCtors (cmdProvidedType: ProvidedTypeDefinition) =
    [ 
      let ctor = ProvidedConstructor([], InvokeCode = fun _ -> <@@ "My internal state" :> obj @@>)
      yield ctor :> MemberInfo
    ]

  let createOutputType (cmdProvidedType: ProvidedTypeDefinition) (commandText: string) = 
    let recordType = ProvidedTypeDefinition("Row", baseType = Some typeof<obj>, HideObjectMethods = true)

    let propertyName = "Wat"
    let propertyType = typeof<string>
    let property = ProvidedProperty(propertyName, propertyType)
    property.GetterCode <- fun args -> <@@ (unbox<DynamicRecord> %%args.[0]).[propertyName] @@>

    let ctorParameter = ProvidedParameter(propertyName, propertyType)  

    let properties = [property :> MemberInfo]
    let ctor = 
      ProvidedConstructor([ctorParameter])
    ctor.InvokeCode <- fun args ->
            let pairs =  Seq.zip args properties //Because we need original names in dictionary
                        |> Seq.map (fun (arg,p) -> <@@ (%%Expr.Value(p.Name):string), %%Expr.Coerce(arg, typeof<obj>) @@>)
                        |> List.ofSeq
            <@@
                let pairs : (string * obj) [] = %%Expr.NewArray(typeof<string * obj>, pairs)
                DynamicRecord (dict pairs)
            @@> 

    (ctor:>MemberInfo)::properties
    |> recordType.AddMembers

    recordType

  let createExecute (cmdProvidedType: ProvidedTypeDefinition) (commandText: string) providedOutputType : MemberInfo list = 
    [
      let m = ProvidedMethod("execute", [], providedOutputType)
      
      let execute = <@@ fun x -> commandText @@>
      m.InvokeCode <- fun exprArgs ->
        let mapping = 
            <@@ 
                fun (values: obj[]) -> 
                    let data = System.Collections.Generic.Dictionary()
                    let names: string[] = [|"Wat"|]
                    for i = 0 to names.Length - 1 do 
                        data.Add(names.[i], values.[i])
                    DynamicRecord( data) |> box 
            @@>
        <@@
//            let ps: (string * obj)[] = %%paramValues
//            [|"yolo"|] |> %%mapping
            // let data = System.Collections.Generic.Dictionary()
            let result = BigQueryAnalyze.analyzeQueryRaw commandText
            (%%mapping) [|result.stdout :> obj|]
            // let names: string[] = [|"Wat"|]
            // for i = 0 to names.Length - 1 do 
            //     data.Add(names.[i], result |> box)
            // DynamicRecord(data) |> box 
            // let result = (%%execute) ()
            // ps |> %%mapOutParamValues
            // result
        @@>

      yield m :> MemberInfo
    ]

type SomeType = {
  Wat: string
}
let createSome wat = {Wat = wat}

[<TypeProvider>]
[<CompilerMessageAttribute("This API supports the BigQueryProvider infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type BigQueryCommandProvider (config: TypeProviderConfig) as this = 
  inherit TypeProviderForNamespaces()
  let ns = "BigQueryProvider.Provided" //this.GetType().Namespace
  let assembly = Assembly.LoadFrom( config.RuntimeAssembly)
  let providerType = ProvidedTypeDefinition(assembly, ns, "BigQueryCommandProvider", Some typeof<obj>, HideObjectMethods = true)

  // let myProp = ProvidedProperty("MyProperty", typeof<string>, IsStatic = true,
  //                                 GetterCode = fun _ -> <@@ "Hello world" @@>)
  // do
  //   providerType.AddMember(myProp)

  do
    providerType.DefineStaticParameters(
            parameters = [ 
                ProvidedStaticParameter("CommandText", typeof<string>) 
            ],             
            instantiationFunction = (fun typeName [|commandText|] ->
                let value = this.CreateRootType(typeName, unbox commandText)
                value
            ) 
        )

  do
    this.AddNamespace(ns, [providerType])

  member this.CreateRootType(typeName, commandText) = 

    let rootType = ProvidedTypeDefinition(assembly, ns, typeName, Some typeof<obj>, HideObjectMethods = false)

    let providedOutputType = DesignTime.createOutputType rootType commandText

    DesignTime.createCommandCtors rootType
    |> List.append (DesignTime.createExecute rootType commandText providedOutputType)
    |> List.append ([providedOutputType])
    |> rootType.AddMembers
    rootType

//     let res = BigQueryAnalyze.analyzeQueryRaw commandText

//     let returnType = ProvidedTypeDefinition("Record", baseType = Some typeof<obj>, HideObjectMethods = true)

//     let property = ProvidedProperty("Wat", typeof<int>)
//     property.GetterCode <- fun args -> <@@ (unbox<DynamicRecord> %%args.[0]).["Wat"] @@>

//     let returnTypeCtor = ProvidedConstructor([ProvidedParameter("Wat", typeof<int>)])
//     returnTypeCtor.InvokeCode <- 
//       fun args -> 
//         <@@
//           let pairs : (string * obj) [] = [|"Wat", 5 :> obj|] //%%Expr.NewArray(typeof<string * obj>, pairs)
//           DynamicRecord (dict pairs)
//         @@> 

//     returnType.AddMember property
//     returnType.AddMember returnTypeCtor
//     rootType.AddMember returnType
    
// //                 let prop = ProvidedMethod("execute", 
// //                   [ProvidedParameter("s", typeof<bool>)],
// //                   typedefof<_ seq>.MakeGenericType(returnType),
// //                   InvokeCode = fun [this;s] -> <@@ Seq.empty @@>
// // //                  InvokeCode = fun [this;s] -> <@@ (%%s:bool) |> not |> string @@>
// //                   )
// //                 rootType.AddMember(prop)


    
//     let exec = ProvidedMethod("execute",
//       [],
//       returnType,
//       InvokeCode = fun [] -> 
//         <@@ 
//             let pairs : (string * obj) [] = [|"Wat", 5 :> obj|] //%%Expr.NewArray(typeof<string * obj>, pairs)
//             let dr = DynamicRecord (dict pairs)

//             dr @@>)

//     rootType.AddMember exec
    

//     let prop = ProvidedMethod("execute2", 
//       [ProvidedParameter("s", typeof<bool>)],
//       typeof<string>,
// //                  InvokeCode = fun [this;s] -> <@@ Seq.empty @@>
//       InvokeCode = fun [this;s] -> <@@ (%%s:bool) |> not |> string @@>
//       )
//     rootType.AddMember(prop)                  

//     let ctor2 = ProvidedConstructor(
//         [ProvidedParameter("InnerState", typeof<string>)],
//         InvokeCode = fun args -> 
//           printfn "%A" args
//           let x = (BigQueryAnalyze.analyzeQueryRaw (commandText :?> string)).stdout
//           <@@ (createSome (x + ((%%args.[0]:string)))) :> obj @@>)

//     let ctor = ProvidedConstructor([], InvokeCode = fun _ -> <@@ "My internal state" :> obj @@>)
//     rootType.AddMember(ctor)
//     rootType.AddMember(ctor2)

// //                rootType.AddMember(rootType)

//     rootType



[<assembly:TypeProviderAssembly>]
do ()