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

let rec parseRecord (fields: Field list) (r:JsonValue) = 
    let values = [for value in r?f -> value]
    fields
    |> List.map (parseField values)

and parseField (values: JsonValue list) (field: Field) = 
    match field with
    | Value (fieldIndex, fieldName, fieldType, fieldMode) -> parseValue values (fieldIndex, fieldName, fieldType, fieldMode)
    | Record (fieldIndex, fieldName, fields, fieldMode) -> (fieldName, parseRecord fields (values.[fieldIndex]?v)) :> obj

and parseValue values ((fieldIndex, fieldName, fieldType, fieldMode) as meta)= 
    printfn "Field index: %A" meta
    match fieldType with
    | String -> (fieldName, values.[fieldIndex]?v.AsString()) :> obj
    | Float -> (fieldName, values.[fieldIndex]?v.AsFloat()) :> obj
    | Integer -> (fieldName, values.[fieldIndex]?v.AsInteger()) :> obj
    | Boolean -> (fieldName, values.[fieldIndex]?v.AsBoolean()) :> obj
    //[for value in values -> parseValue value]

let rows = res2?rows

let parseStuff = [for row in rows -> parseRecord schema.Fields row]

printfn "%A" parseStuff