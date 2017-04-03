module BigQueryProvider.BigQueryHelper 

let executeCmdBq = ProcessHelper.executeProcess "bq"

let analyzeQueryRaw queryStr = 
    let cmdLine = sprintf "query --format=json --dry_run=true --use_legacy_sql=false '%s'" queryStr
    executeCmdBq cmdLine
