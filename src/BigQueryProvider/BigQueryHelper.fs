module BigQueryProvider.BigQueryHelper 

module Auth = 
    open Newtonsoft.Json
    open System.IO
    open System
    open FSharp.Data
    
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
        raise (exn (sprintf "%A" authFile))
        // let response = Http.RequestString(authUrl, body = HttpRequestBody.FormValues requestBody)
        // response

    let authenticate() = 
        getWellknownFileContent()
        |> parseJson<AuthFile>
        |> refreshToken
        |> parseJson<AuthToken>
        |> (fun i -> i.AccessToken)

module QueryAnalyze = 
    open Newtonsoft.Json
    open FSharp.Data

    type Query = 
        {
            [<JsonProperty("query")>]Query: string
            [<JsonProperty("useLegacySql")>]UseLegacySQL: bool
        }

    type QueryConfig = 
        {
            [<JsonProperty("query")>]Query: Query
            [<JsonProperty("dryRun")>]DryRun: bool    
        }

    type JobConfig = 
        {
            [<JsonProperty("configuration")>]Config: QueryConfig
        }

    let getQueryInfo authToken project query = 
        let url = sprintf "https://www.googleapis.com/bigquery/v2/projects/%s/jobs" project
        let job = Newtonsoft.Json.JsonConvert.SerializeObject({Config = {Query = {UseLegacySQL = false; Query = query}; DryRun = true}})
        Http.Request(url,
            headers = 
                [
                    "Authorization", (sprintf "Bearer %s" authToken)
                    "Content-Type", "application/json"
                ],
                body = HttpRequestBody.TextRequest job)

let executeCmdBq = ProcessHelper.executeProcess "bq"

let analyzeQueryRaw queryStr = 
    let projectName = "uc-prox-production"
    let authToken = Auth.authenticate()
    "yolo"
    // printfn "Here we are? %A" authToken
    // let response = QueryAnalyze.getQueryInfo authToken projectName queryStr
    // printfn "Are we here?"
    // let getBodyText body = 
    //     match body with
    //     | FSharp.Data.HttpResponseBody.Text t -> t
    //     | _ -> raise (exn "dunno")
    // let bodyText = getBodyText response.Body
    // match response.StatusCode with
    // | 400 -> raise (exn (sprintf "|%s|" bodyText))
    // | 200 -> bodyText