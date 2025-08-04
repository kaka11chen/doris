// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_warm_up_select") {
    def ttlProperties = """ PROPERTIES("file_cache_ttl_seconds"="12000") """
    
    if (!isCloudMode()) {
        return
    }

    def s3BucketName = getS3BucketName()
    def s3WithProperties = """ WITH S3 (
        |"AWS_ACCESS_KEY" = "${getS3AK()}",
        |"AWS_SECRET_KEY" = "${getS3SK()}",
        |"AWS_ENDPOINT" = "${getS3Endpoint()}",
        |"AWS_REGION" = "${getS3Region()}",
        |"provider" = "${getS3Provider()}")
        |PROPERTIES(
        |"exec_mem_limit" = "8589934592",
        |"load_parallelism" = "3")""".stripMargin()

    sql "use @regression_cluster_name0"

    def table = "customer"
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
    // create table if not exists
    sql (new File("""${context.file.parent}/ddl/${table}.sql""").text + ttlProperties)
    sql """ alter table ${table} set ("disable_auto_compaction" = "true") """ // no influence from compaction

    sleep(10000)

    def load_customer_once = {
        def uniqueID = Math.abs(UUID.randomUUID().hashCode()).toString()
        def loadLabel = table + "_" + uniqueID
        // load data from cos
        def loadSql = new File("""${context.file.parent}/ddl/${table}_load.sql""").text.replaceAll("\\\$\\{s3BucketName\\}", s3BucketName)
        loadSql = loadSql.replaceAll("\\\$\\{loadLabel\\}", loadLabel) + s3WithProperties
        sql loadSql

        // check load state
        while (true) {
            def stateResult = sql "show load where Label = '${loadLabel}'"
            def loadState = stateResult[stateResult.size() - 1][2].toString()
            if ("CANCELLED".equalsIgnoreCase(loadState)) {
                throw new IllegalStateException("load ${loadLabel} failed.")
            } else if ("FINISHED".equalsIgnoreCase(loadState)) {
                break
            }
            sleep(5000)
        }
    }

    // Load some data first
    load_customer_once()
    
    // Test WARM UP SELECT functionality
    logger.info("Testing WARM UP SELECT functionality")
    
    // Test basic WARM UP SELECT 
    def warmupResult = sql "WARM UP SELECT * FROM ${table} LIMIT 100"
    logger.info("WARM UP SELECT result: ${warmupResult}")
    
    // The result should be empty (no actual data returned)
    assertTrue(warmupResult.isEmpty(), "WARM UP SELECT should not return any data rows")
    
    // Test WARM UP SELECT with WHERE clause
    sql "WARM UP SELECT c_custkey, c_name FROM ${table} WHERE c_custkey < 1000"
    
    // Test WARM UP SELECT with aggregation
    sql "WARM UP SELECT COUNT(*) FROM ${table}"
    
    // Test WARM UP SELECT with ORDER BY
    sql "WARM UP SELECT c_custkey, c_name FROM ${table} ORDER BY c_custkey LIMIT 50"
    
    // Test WARM UP SELECT with multiple columns
    sql "WARM UP SELECT c_custkey, c_name, c_address, c_phone FROM ${table} WHERE c_nationkey = 1"
    
    logger.info("All WARM UP SELECT tests completed successfully")

    // Clean up
    sql new File("""${context.file.parent}/ddl/${table}_delete.sql""").text
}
