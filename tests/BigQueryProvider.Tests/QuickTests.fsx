#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

//BigQueryCommandProvider.MyProperty

open BigQueryProvider.Provided
open System
//type X = BigQueryCommandProvider<CommandText = "tomas">

//let x = BigQueryCommandProvider<"Tomas">
//let x = new BigQueryCommandProvider<CommandText = "asdsa">()

type X = BigQueryCommandProvider<"SELECT name FROM `uc-prox-development.tomas.dflow`">

let x = X("as")
printfn "%A" (x.execute)

//type X = BigQueryProvider.

//type X = BigQueryCommandProvider<"tomas">
//let x = X()
