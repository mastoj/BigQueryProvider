#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

open BigQueryProvider
open System

[<Literal>]
let query = """SELECT 1 as martin, name, "tomas" as another_name FROM `tomas.dflow`"""
type X = BigQueryCommandProvider<query>
let x = X()

let res = x.execute()
let a = res.martin
let b = res.name
printfn "%A" (res)
