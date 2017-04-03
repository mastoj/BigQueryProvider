module BigQueryProvider.SchemaHandling

open Newtonsoft.Json
open Newtonsoft.Json.Linq

type ParameterType = 
    | Bool
    | Int64
    | String

type Parameter = {
    Name: string
    ParameterType: ParameterType
}

type FieldMode =
    | Nullable
    | NonNullable
    | Repeated

type FieldName = string
type FieldIndex = int

type FieldType =
    | Boolean
    | String
    | Integer
    | Record

type Field =
    | Value of FieldIndex * FieldName * FieldType * FieldMode
    | Record of FieldIndex * FieldName * Field list * FieldMode

type Schema = 
    {
        Fields: Field list
        Parameters: Parameter list
    }

module Parsing = 
    let private parseParameterType = function
        | "BOOL" -> Bool
        | "STRING" -> ParameterType.String
        | "INT64" -> Int64
        | x -> raise (exn ("Unsupported type: " + x))

    let private parseParameter (jObj:JToken) = 
        let parameters = 
            match jObj.["undeclaredQueryParameters"] with
            | null -> []
            | x -> x |> Seq.toList
        parameters
        |> Seq.toList
        |> List.map (fun jToken ->
            {
                ParameterType = parseParameterType (jToken.["parameterType"].Value<string>("type"))
                Name = jToken.Value<string>("name")
            })

    let private parseName (jObj:JToken) = 
        jObj.Value<string>("name")

    let private parseType (jObj:JToken) : FieldType= 
        match jObj.Value<string>("type") with
        | "INTEGER" -> Integer
        | "STRING" -> String
        | "BOOLEAN" -> Boolean

    let private parseMode (jObj:JToken) = 
        match jObj.Value<string>("mode") with
        | "NULLABLE" -> Nullable
        | "REPEATED" -> Repeated

    let rec private parseField index (jObj:JToken) =
        match jObj.Value<string>("type") with
        | "RECORD" -> parseRecord index jObj
        | x -> parseValue index jObj
    and parseRecord index (jObj:JToken) = 
        let fields = jObj.["fields"] |> Seq.toList |> List.mapi parseField
        let fieldName = parseName jObj
        let mode = parseMode jObj
        Record(index, fieldName, fields, mode)
    and parseValue index (jObj:JToken) =
        let fieldName = parseName jObj
        let mode = parseMode jObj
        let fieldType = parseType jObj
        Value(index, fieldName, fieldType, mode)
        
    let parseSchema (jObj:JToken) = 
        jObj.["schema"].["fields"]
        |> Seq.toList
        |> List.mapi parseField

    let parseQueryMeta json = 
        let jObj = JObject.Parse(json).["statistics"].["query"]
        {
            Fields = parseSchema jObj
            Parameters = parseParameter jObj
        }
