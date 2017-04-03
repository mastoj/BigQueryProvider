namespace BigQueryProvider

open ProviderImplementation.ProvidedTypes
open Microsoft.FSharp.Core.CompilerServices
open Microsoft.FSharp.Quotations
open System.Reflection
open FSharp.Data.SqlClient
open BigQueryProvider
open ProcessHelper

[<TypeProvider>]
[<CompilerMessageAttribute("This API supports the BigQueryProvider infrastructure and is not intended to be used directly from your code.", 101, IsHidden = true)>]
type BigQueryCommandProvider (config: TypeProviderConfig) as this = 
  inherit TypeProviderForNamespaces()
  let nameSpace = this.GetType().Namespace
  let assembly = Assembly.LoadFrom( config.RuntimeAssembly)
  do 
    printfn "Assembly name: %A" assembly.FullName 
  let providerType = ProvidedTypeDefinition(assembly, nameSpace, "BigQueryCommandProvider", Some typeof<obj>, HideObjectMethods = true)

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
    this.AddNamespace(nameSpace, [providerType])

  member this.CreateRootType(typeName, commandText) = 

    let rootType = ProvidedTypeDefinition(assembly, nameSpace, typeName, Some typeof<obj>, HideObjectMethods = false)
      
    let schema = 
        commandText
        |> BigQueryHelper.analyzeQueryRaw
        |> (fun y -> y.stdout)
        |> SchemaHandling.Parsing.parseQueryMeta

    let providedOutputType = DesignTime.createOutputType rootType schema

    DesignTime.createCommandCtors rootType
    |> List.append (DesignTime.createExecute rootType commandText providedOutputType)
    |> List.append ([providedOutputType])
    |> rootType.AddMembers
    rootType

[<assembly:TypeProviderAssembly>]
do ()