#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

open BigQueryProvider
open BigQueryHelper
open System
open FSharp.Data
open FSharp.Data.JsonExtensions

[<Literal>]
let query = """SELECT actor_attributes.gravatar_id FROM `bigquery-public-data.samples.github_nested` LIMIT 5"""
type X = BigQueryCommandProvider<query>
let x = X()

let res = x.execute()
printfn "%A" (res)


let token = Auth.authenticate()
let res2 = QueryExecute.executeQuery token "uc-prox-production" query

let parseValue (value:JsonValue) = 
    let guidStr = (value?v).AsString()
    System.Guid.Parse(guidStr)

let parseRow (r:JsonValue) = 
    let values = r?f
    [for value in values -> parseValue value]

let rows = res2?rows

let parseStuff = [for row in rows -> parseRow row]

printfn "%A" parseStuff