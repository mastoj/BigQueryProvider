module BigQueryProvider.BigQueryHelper 

let executeCmdBq = ProcessHelper.executeProcess "bq"

let analyzeQueryRaw queryStr = 
    {
        stdout = """{"status":{"state":"DONE"},"kind":"bigquery#job","statistics":{"query":{"cacheHit":true,"statementType":"SELECT","totalBytesBilled":"0","totalBytesProcessed":"0","schema":{"fields":[{"type":"STRING","name":"name","mode":"NULLABLE"}]}},"creationTime":"1491396821408","totalBytesProcessed":"0"},"jobReference":{"projectId":"uc-prox-production"},"etag":"\"smpMas70-D1-zV2oEH0ud6qY21c/HTDNf5GMh53UFC08Uiwdsdb5vlw\"","configuration":{"query":{"createDisposition":"CREATE_IF_NEEDED","query":"SELECT \"tomas\" as name","writeDisposition":"WRITE_TRUNCATE","destinationTable":{"projectId":"uc-prox-production","tableId":"anond907caa3c3f50337ff414e7c16fa3281e4de1bcc","datasetId":"_2a855e87bf6147c55e896dcce917ee0deb1bc026"},"useLegacySql":false},"dryRun":true},"user_email":"tomas.jansson@unacast.com"}"""
        stderr = ""
        exitCode = 0

    } : ProcessHelper.ProcessResult
    // let cmdLine = sprintf "query --format=json --dry_run=true --use_legacy_sql=false '%s'" queryStr
    // executeCmdBq cmdLine
