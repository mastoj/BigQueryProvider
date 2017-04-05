#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

//BigQueryCommandProvider.MyProperty

open BigQueryProvider
open System
//type X = BigQueryCommandProvider<CommandText = "tomas">

//let x = BigQueryCommandProvider<"Tomas">
//let x = new BigQueryCommandProvider<CommandText = "asdsa">()

//type X = BigQueryCommandProvider<"SELECT name FROM `uc-prox-development.tomas.dflow`">
type X = BigQueryCommandProvider<"""SELECT "tomas" as name""">
let x = X()
// let z = x.execute()
// printfn "%A" z
// printfn "%A" (x.execute2 true)
//x
let res = x.execute()
printfn "%A" (res)
//type X = BigQueryProvider.

//type X = BigQueryCommandProvider<"tomas">
//let x = X()

