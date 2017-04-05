#r "../../packages/Newtonsoft.Json/lib/net45/Newtonsoft.Json.dll"
#r "../../packages/FSharp.Data/lib/net40/FSharp.Data.dll"

open System
open FSharp.Data
open Newtonsoft.Json
open System.IO

let projectId = "uc-prox-production"

[<Literal>]
let dataSetSample = """
{
  "kind": "bigquery#datasetList",
  "etag": "\"wWvNncJfeAdSHVaIWRpICxBS7AM/CMmBj3t9m1m047bVlGrpPo-H59k\"",
  "datasets": [
    {
      "kind": "bigquery#dataset",
      "id": "uc-prox-production:audience_analytics",
      "datasetReference": {
        "datasetId": "audience_analytics",
        "projectId": "uc-prox-production"
      }
    },
    {
      "kind": "bigquery#dataset",
      "id": "uc-prox-production:convertro",
      "datasetReference": {
        "datasetId": "convertro",
        "projectId": "uc-prox-production"
      }
    }]}"""

type Dataset = JsonProvider<dataSetSample>

let getDatasets project = 
    let url = sprintf "https://www.googleapis.com/bigquery/v2/projects/%s/datasets" project
    let response = 
        Http.RequestString(url, 
            headers = 
                [
                    "Authorization", "Bearer ya29.Ci-uA5stRKgNigQ5OryhXTkF731TXAhZOZ_60kq5vQZq5mJp8RhNdufuT-QYBEA2JQ"
                ])
    Dataset.Parse(response)

[<Literal>]
let tablesSample = """
{
  "kind": "bigquery#tableList",
  "etag": "\"wWvNncJfeAdSHVaIWRpICxBS7AM/uwhpiBsMw719A6_Z1-CZIo9-DAM\"",
  "nextPageToken": "gimbal_visits_auto_2016_11_24_visits_00005",
  "tables": [
    {
      "kind": "bigquery#table",
      "id": "uc-prox-production:venue_visits_import.areametrics_beacon_proc",
      "tableReference": {
        "projectId": "uc-prox-production",
        "datasetId": "venue_visits_import",
        "tableId": "areametrics_beacon_proc"
      },
      "type": "TABLE"
    },
    {
      "kind": "bigquery#table",
      "id": "uc-prox-production:venue_visits_import.areametrics_beacon_raw",
      "tableReference": {
        "projectId": "uc-prox-production",
        "datasetId": "venue_visits_import",
        "tableId": "areametrics_beacon_raw"
      },
      "type": "TABLE"
    }]}"""

type Tables = JsonProvider<tablesSample>

module Auth = 
  open Newtonsoft.Json
  open System.IO
  let parseJson<'a> str = Newtonsoft.Json.JsonConvert.DeserializeObject<'a>(str)

  type AuthFile = 
    {
      [<JsonProperty("client_id")>]ClientId: string
      [<JsonProperty("client_secret")>]ClientSecret: string
      [<JsonProperty("refresh_token")>]RefreshToken: string
      [<JsonProperty("type")>]Type: string
    }

  type AuthToken = 
    {
      [<JsonProperty("access_token")>]AccessToken: string
      [<JsonProperty("token_type")>]TokenType: string
      [<JsonProperty("expires_in")>]ExpiresIn: string
      [<JsonProperty("id_token")>]IdToken: string    
    }

  let getWellknownFileContent() = 
    let relativePath = ".config/gcloud/application_default_credentials.json"
    let homePath = Environment.GetEnvironmentVariable("HOME")
    let path = sprintf "%s/%s" homePath relativePath
    File.ReadAllText(path)

  let refreshToken authFile = 
    let authUrl = "https://www.googleapis.com/oauth2/v4/token"
    let requestBody = 
      [
        "grant_type", "refresh_token"
        "client_id", authFile.ClientId
        "client_secret", authFile.ClientSecret
        "refresh_token", authFile.RefreshToken
      ]
    let response = Http.RequestString(authUrl, body = HttpRequestBody.FormValues requestBody)
    response

open Auth
let authenticate() = 
  getWellknownFileContent()
  |> parseJson<AuthFile>
  |> refreshToken
  |> parseJson<AuthToken>
  |> (fun i -> i.AccessToken)

let projectName = "uc-prox-production"

let authToken = authenticate()

//POST 
//Will return the jobConfigResult belw
type QueryConfig = 
    {
      [<JsonProperty("query")>]Query: QueryConfig
      [<JsonProperty("useLegacySql")>]UseLegacySQL: bool
    }

type JobConfig = {
      [<JsonProperty("query")>]Query: QueryConfig
      [<JsonProperty("dryRun")>]DryRun: bool    
}



[<Literal>]
let jobConfigResult = """
{
    "status": {
        "state": "DONE"
    },
    "kind": "bigquery#job",
    "statistics": {
        "query": {
            "statementType": "SELECT",
            "totalBytesBilled": "0",
            "totalBytesProcessed": "0",
            "cacheHit": false,
            "undeclaredQueryParameters": [
                {
                    "parameterType": {
                        "type": "BOOL"
                    },
                    "name": "a"
                },
                {
                    "parameterType": {
                        "type": "INT64"
                    },
                    "name": "b"
                },
                {
                    "parameterType": {
                        "type": "STRING"
                    },
                    "name": "c"
                }
            ],
            "schema": {
                "fields": [
                    {
                        "type": "BOOLEAN",
                        "name": "x",
                        "mode": "NULLABLE"
                    },
                    {
                        "type": "INTEGER",
                        "name": "y",
                        "mode": "NULLABLE"
                    },
                    {
                        "type": "BOOLEAN",
                        "name": "z",
                        "mode": "NULLABLE"
                    },
                    {
                        "type": "STRING",
                        "name": "w",
                        "mode": "REPEATED"
                    },
                    {
                        "fields": [
                            {
                                "type": "STRING",
                                "name": "t",
                                "mode": "NULLABLE"
                            },
                            {
                                "type": "INTEGER",
                                "name": "u",
                                "mode": "NULLABLE"
                            }
                        ],
                        "type": "RECORD",
                        "name": "v",
                        "mode": "NULLABLE"
                    },
                    {
                        "fields": [
                            {
                                "type": "INTEGER",
                                "name": "_field_1",
                                "mode": "NULLABLE"
                            },
                            {
                                "type": "STRING",
                                "name": "g",
                                "mode": "NULLABLE"
                            }
                        ],
                        "type": "RECORD",
                        "name": "u",
                        "mode": "REPEATED"
                    },
                    {
                        "fields": [
                            {
                                "type": "STRING",
                                "name": "h",
                                "mode": "REPEATED"
                            }
                        ],
                        "type": "RECORD",
                        "name": "t",
                        "mode": "NULLABLE"
                    }
                ]
            }
        },
        "creationTime": "1490729738377",
        "totalBytesProcessed": "0"
    },
    "jobReference": {
        "projectId": "uc-prox-production"
    },
    "etag": "\"smpMas70-D1-zV2oEH0ud6qY21c/IWCngCv5ww2vMSLRv2GxsJsBwwU\"",
    "configuration": {
        "query": {
            "createDisposition": "CREATE_IF_NEEDED",
            "query": "SELECT @a IS TRUE AS x, @b + 1 AS y, \"foo\" = @c AS z, [\"tomas\", \"jansson\"] as w, STRUCT(\"wat\" as t, 69 as u) as v, [STRUCT(3, \"allo\" as g), STRUCT(5 as a, \"yolo\")] as u, STRUCT([\"a\"] as h) as t;",
            "writeDisposition": "WRITE_TRUNCATE",
            "destinationTable": {
                "projectId": "uc-prox-production",
                "tableId": "anon311d2e18c944c7a5c91ab469c91b33527a239a06",
                "datasetId": "_2a855e87bf6147c55e896dcce917ee0deb1bc026"
            },
            "useLegacySql": false
        },
        "dryRun": true
    },
    "user_email": "tomas.jansson@unacast.com"
}"""

type JobConfigResult = JsonProvider<jobConfigResult>

let getQueryInfo authToken project query = 
    let url = sprintf "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project
    let response = 
        Http.RequestString(url, 
            headers = 
                [
                    "Authorization", (sprintf "Bearer %s" authToken)
                ],
                body = HttpRequestBody.Json //Insert the serialized job stuff here)
    let tables = Tables.Parse(response)
    tables
let someTables = getTables authToken projectName "venue_visits_import"
