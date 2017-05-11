#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

open BigQueryProvider
open System
open FSharp.Data

[<Literal>]
let query = """SELECT identifier as yolo FROM `uc-prox-production.venue_visits_core.visit` LIMIT 5"""
type X = BigQueryCommandProvider<query>
let x = X()

let res = x.execute()
let a = res.yolo
printfn "%A" (res)
