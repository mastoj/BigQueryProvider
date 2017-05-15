#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

open BigQueryProvider
open BigQueryHelper
open ProcessHelper
open System
open FSharp.Data
open FSharp.Data.SqlClient
open FSharp.Data.JsonExtensions
open SchemaHandling

[<Literal>]
let query = """SELECT 1 as x, actor_attributes.gravatar_id, actor_attributes as attrs FROM `bigquery-public-data.samples.github_nested` LIMIT 5"""
//type X = BigQueryCommandProvider<query>
// let x = X()

// let res = x.execute()
// printfn "%A" (res)


let token = Auth.authenticate()

let environmentRes = executeProcess "gcloud" "config list core/project --format='value(core.project)'"
let environment = environmentRes.stdout
let res2 = QueryExecute.executeQuery token environment query

let schema =
    query
    |> BigQueryHelper.analyzeQueryRaw
    |> SchemaHandling.Parsing.parseQueryMeta

// let parseValue (value:JsonValue) = 
//     let guidStr = (value?v).AsString()
//     System.Guid.Parse(guidStr)

open System.Collections.Generic
let rec parseRecord (fields: Field list) (r:JsonValue) = 
    let dict = new Dictionary<string, obj>()
    let values = [for value in r?f -> value]
    fields
    |> List.iter (parseField dict values)
    DynamicRecord(dict :> IDictionary<string, obj>)

and parseField (dict: Dictionary<string, obj>) (values: JsonValue list) (field: Field) = 
    match field with
    | Value (fieldIndex, fieldName, fieldType, fieldMode) -> parseValue dict values (fieldIndex, fieldName, fieldType, fieldMode)
    | Record (fieldIndex, fieldName, fields, fieldMode) -> 
        
        dict.[fieldName] <- (parseRecord fields (values.[fieldIndex]?v)) :> obj

and parseValue (dict: Dictionary<string, obj>) values ((fieldIndex, fieldName, fieldType, fieldMode) as meta)= 
    printfn "Field index: %A" meta
    match fieldType with
    | String -> (values.[fieldIndex]?v.AsString()) :> obj
    | Float -> (values.[fieldIndex]?v.AsFloat()) :> obj
    | Integer -> (values.[fieldIndex]?v.AsInteger()) :> obj
    | Boolean -> (values.[fieldIndex]?v.AsBoolean()) :> obj
    |> (fun v -> dict.[fieldName] <- v)
    //[for value in values -> parseValue value]

let rows = res2?rows

let parseStuff = [for row in rows -> parseRecord schema.Fields row]

printfn "%A" parseStuff