module BigQueryProvider.BigQueryHelper 
open FSharp.Data.SqlClient
open FSharp.Data
open FSharp.Data.JsonExtensions

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
        let response = Http.RequestString(authUrl, body = HttpRequestBody.FormValues requestBody)
        response

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
                
module QueryExecute = 
    open System.Collections.Generic
    open FSharp.Data
    open FSharp.Data.JsonExtensions
    open SchemaHandling

    let template = """
    {
        "kind": "",
        "query": "%query%",
        "queryParameters": [],
        "useLegacySql": false
    }"""

    let executeQuery authToken project commandText = 
        let queryUrl = sprintf "https://www.googleapis.com/bigquery/v2/projects/%s/queries" project
        let query = template.Replace("%query%", commandText)
        let res = Http.RequestString(
            queryUrl,
            headers = 
                [
                    "Authorization", (sprintf "Bearer %s" authToken)
                    "Content-Type", "application/json"
                ],
                body = HttpRequestBody.TextRequest query)
        
        let json = FSharp.Data.JsonValue.Parse(res)
        json

    let rec parseRecord (fields: Field list) (row:JsonValue) = 
        let dict = new Dictionary<string, obj>()
        let values = [for value in row?f -> value]
        fields
        |> List.iter (parseField dict values)
        DynamicRecord(dict :> IDictionary<string, obj>) :> obj

    and parseField (dict: Dictionary<string, obj>) (values: JsonValue list) (field: Field) =
        let fieldIndex, fieldName, fieldMode, parser = 
            match field with
            | Value (fieldIndex, fieldName, fieldType, fieldMode) -> 
                fieldIndex, fieldName, fieldMode, (parseValue fieldType)
            | Record (fieldIndex, fieldName, fields, fieldMode) -> 
                fieldIndex, fieldName, fieldMode, (parseRecord fields)

        let value = values.[fieldIndex]?v
        match fieldMode with
        | Nullable ->
            if value = JsonValue.Null
            then None
            else value |> parser |> Some
            :> obj
        | NonNullable -> parser value
        | Repeated -> 
            [for arrV in value -> arrV?v]
            |> List.map parser
            :> obj
        |> (fun v -> dict.[fieldName] <- v)

    and parseValue fieldType (value:JsonValue) = 
        match fieldType with
        | String -> value.AsString() :> obj
        | Float -> value.AsFloat() :> obj
        | Integer -> value.AsInteger() :> obj
        | Boolean -> value.AsBoolean() :> obj
        //[for value in values -> parseValue value]


let executeCmdBq = ProcessHelper.executeProcess "bq"

let analyzeQueryRaw queryStr = 
    let projectName = "uc-prox-production"
    let authToken = Auth.authenticate()
    let response = QueryAnalyze.getQueryInfo authToken projectName queryStr
    let getBodyText body = 
        match body with
        | FSharp.Data.HttpResponseBody.Text t -> t
        | _ -> raise (exn "dunno")
    let bodyText = getBodyText response.Body
    match response.StatusCode with
    | 400 -> raise (exn (sprintf "|%s|" bodyText))
    | 200 -> bodyText

let executeQuery commandText = 
    let projectName = "uc-prox-production"
    let authToken = Auth.authenticate()
    let res = QueryExecute.executeQuery authToken projectName commandText
    let rows = res?rows
    let schema =
        commandText
        |> analyzeQueryRaw
        |> SchemaHandling.Parsing.parseQueryMeta
    [for row in rows -> QueryExecute.parseRecord schema.Fields row]
