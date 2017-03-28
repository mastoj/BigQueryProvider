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
                ProvidedStaticParameter("PropName", typeof<string>) 
                ProvidedStaticParameter("CommandText", typeof<string>) 
            ],             
            instantiationFunction = (fun typeName [|propName; commandText|] ->
                let rootType = ProvidedTypeDefinition(assembly, ns, typeName, Some typeof<obj>, HideObjectMethods = false)

//                let value = lazy this.CreateRootType(typeName, unbox args.[0])
//                let cmdProvidedType = ProvidedTypeDefinition(assembly, ns, typeName, None, HideObjectMethods = true)
                // let ctor = ProvidedConstructor([], InvokeCode = fun args -> <@@ "My internal state" :> obj @@>)
                // rootType.AddMember(ctor)

                let res = BigQueryAnalyze.analyzeQueryRaw (commandText :?> string)

                let getCommandText() = commandText :?> string

                let prop = ProvidedProperty(propName :?> string, typeof<SomeType>, GetterCode = fun _ -> let x = res.stdout in <@@ createSome x :> obj @@>)
                rootType.AddMember(prop)

                let ctor2 = ProvidedConstructor(
                    [ProvidedParameter("InnerState", typeof<string>)],
                    InvokeCode = fun args -> <@@ (createSome %%(args.[0])) :> obj @@>)

                let ctor = ProvidedConstructor([], InvokeCode = fun _ -> <@@ "My internal state" :> obj @@>)
                rootType.AddMember(ctor)
                rootType.AddMember(ctor2)
                rootType
            ) 
        )

  do
    this.AddNamespace(ns, [providerType])


[<assembly:TypeProviderAssembly>]
do ()