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

// Example queries
// struct in array: SELECT [STRUCT(1 AS a, 'abc' AS b),STRUCT(2 AS a, 'abcd' AS b)] as recarr, actor_attributes.blog, actor_attributes.gravatar_id, actor_attributes as attrs FROM `bigquery-public-data.samples.github_nested` LIMIT 5
// with args: SELECT @a IS TRUE AS x, @b + 1 AS y, "foo" = @c AS z, ["tomas", "jansson"] as w, STRUCT("wat" as t, 69 as u) as v, [STRUCT(3, "allo" as g), STRUCT(5 as a, "yolo")] as u, STRUCT(["a"] as h) as t

[<Literal>]
let query = """SELECT [STRUCT(1 AS a, 'abc' AS b),STRUCT(2 AS a, 'abcd' AS b)] as recarr, actor_attributes.blog, actor_attributes.gravatar_id, actor_attributes as attrs FROM `bigquery-public-data.samples.github_nested` LIMIT 5"""
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
let rec parseRecord (fields: Field list) (row:JsonValue) = 
    let dict = new Dictionary<string, obj>()
    let values = [for value in row?f -> value]
    fields
    |> List.iter (parseField dict values)
    DynamicRecord(dict :> IDictionary<string, obj>)

and parseField (dict: Dictionary<string, obj>) (values: JsonValue list) (field: Field) = 
    match field with
    | Value (fieldIndex, fieldName, fieldType, fieldMode) -> 
        match fieldMode with
        | Nullable
        | NonNullable ->
            parseValue dict (values.[fieldIndex]?v) (fieldType)
        | Repeated ->
            [for arrV in values.[fieldIndex]?v -> arrV]
            |> List.map (fun arrV -> parseValue dict (arrV?v) fieldType)
            :> obj
        | _ -> raise (exn "Not implemented yet")
        |> (fun v -> dict.[fieldName] <- v) 
    | Record (fieldIndex, fieldName, fields, fieldMode) -> 
        match fieldMode with
        | Nullable
        | NonNullable ->
            parseRecord fields (values.[fieldIndex]?v) :> obj
        | Repeated ->
            [for arrV in values.[fieldIndex]?v -> arrV]
            |> List.map (fun arrv -> parseRecord fields (arrv?v) :> obj)
            :> obj
        |> (fun v -> dict.[fieldName] <- v)


and parseValue (dict: Dictionary<string, obj>) (value:JsonValue) fieldType = 
    match fieldType with
    | String -> value.AsString() :> obj
    | Float -> value.AsFloat() :> obj
    | Integer -> value.AsInteger() :> obj
    | Boolean -> value.AsBoolean() :> obj
    //[for value in values -> parseValue value]

let rows = res2?rows

printfn "Schema: %A" schema
let parseStuff = [for row in rows -> parseRecord schema.Fields row]

printfn "%A" parseStuff