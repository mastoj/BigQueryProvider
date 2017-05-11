#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#r "../../src/BigQueryProvider/bin/Release/BigQueryProvider.dll"

open BigQueryProvider
open System
open FSharp.Data

// [<Literal>]
// let query = """SELECT identifier as yolo FROM `uc-prox-production.venue_visits_core.visit` LIMIT 5"""
// type X = BigQueryCommandProvider<query>
// let x = X()

// let res = x.execute()
// let a = res.yolo
// printfn "%A" (res)



// ClientId =
//   "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com";
//  ClientSecret = "d-FL95Q19q7MQmFpd7hHD0Ty";
//  RefreshToken = "1/zyi_jAHH3ePLpqlieVFtELbBAgjiLeBDBT-_HWpUfRA";
//  Type = "authorized_user";

let authUrl = "https://www.googleapis.com/oauth2/v4/token"
let requestBody = 
    [
    "grant_type", "refresh_token"
    "client_id", "764086051850-6qr4p6gpi6hn506pt8ejuq83di341hur.apps.googleusercontent.com"
    "client_secret", "d-FL95Q19q7MQmFpd7hHD0Ty"
    "refresh_token", "1/zyi_jAHH3ePLpqlieVFtELbBAgjiLeBDBT-_HWpUfRA"
    ]

let response = Http.RequestString(authUrl, body = HttpRequestBody.FormValues requestBody)