module BigQueryProvider.DesignTime

open FSharp.Data.SqlClient
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open ProviderImplementation.ProvidedTypes
open System.Reflection
open BigQueryHelper
open SchemaHandling
open SchemaHandling.Parsing

let createCommandCtors (cmdProvidedType: ProvidedTypeDefinition) =
    [
        let ctor = ProvidedConstructor([], InvokeCode = fun _ -> <@@ "My internal state" :> obj @@>)
        yield ctor :> MemberInfo
    ]

let internal createOutputType (rootType:ProvidedTypeDefinition) (schema: Schema) =
    let recordType = ProvidedTypeDefinition("Row", baseType = Some typeof<obj>, HideObjectMethods = true)

    let createProperty (field: Field) =
        match field with
        | Value(index, name, fieldType, mode) ->
        let propertyType =
            match fieldType with
            | String -> typeof<string>
            | Integer -> typeof<int>
            | Float -> typeof<double>
            | Boolean -> typeof<bool>
            | _ -> raise (exn "FieldType not supported yet")
        let property = ProvidedProperty(name, propertyType)
        property.GetterCode <- fun args -> <@@ (unbox<DynamicRecord> %%args.[0]).[name] @@>
        property
        | _ -> raise (exn "Value type not supported yet")

    let properties =
        schema.Fields
        |> List.map (createProperty >> (fun p -> p :> MemberInfo))

    let propertyName = "Wat"
    let propertyType = typeof<string>
    let property = ProvidedProperty(propertyName, propertyType)
    property.GetterCode <- fun args -> <@@ (unbox<DynamicRecord> %%args.[0]).[propertyName] @@>

    let ctorParameter = ProvidedParameter(propertyName, propertyType)

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

let execImpl commandText (row: obj[]) =
    let schema =
        (BigQueryHelper.analyzeQueryRaw commandText).stdout
        |> parseQueryMeta
    let data = System.Collections.Generic.Dictionary()
    schema.Fields
    |> List.iter (fun y ->
        match y with
        | Value(index, name, fieldType, mode) ->
            data.Add((string)name, row.[(int)index])
        | _ -> ())
    DynamicRecord(data)

let createExecute (commandText: string) providedOutputType : MemberInfo list =
    [
        let m = ProvidedMethod("execute", [], providedOutputType)
        m.InvokeCode <- fun exprArgs ->
            <@@
                let result = analyzeQueryRaw commandText
                execImpl commandText ([|result.stdout :> obj|])
            @@>

        yield m :> MemberInfo
    ]