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

suite("warmup_select", "p0,warmup,query") {

    def setup_test_table = {
        sql "DROP TABLE IF EXISTS test_warmup_table"
        sql """
            CREATE TABLE test_warmup_table (
                id INT,
                name VARCHAR(50),
                age INT,
                salary DOUBLE
            )
            DISTRIBUTED BY HASH(id) BUCKETS 1
            PROPERTIES("replication_num" = "1")
        """
        
        sql """
            INSERT INTO test_warmup_table VALUES
            (1, 'Alice', 25, 50000.0),
            (2, 'Bob', 30, 60000.0),
            (3, 'Charlie', 35, 70000.0),
            (4, 'David', 40, 80000.0),
            (5, 'Eve', 45, 90000.0)
        """
    }

    def cleanup_test_table = {
        sql "DROP TABLE IF EXISTS test_warmup_table"
    }

    def test_basic_warmup = {
        // Enable file cache for warm up functionality
        sql "set enable_file_cache=true"
        sql "set disable_file_cache=false"
        
        // Basic warm up select
        sql "WARM UP SELECT * FROM test_warmup_table"
        
        // Warm up with WHERE clause
        sql "WARM UP SELECT id, name FROM test_warmup_table WHERE age > 30"
        
        // Warm up with ORDER BY clause
        sql "WARM UP SELECT * FROM test_warmup_table ORDER BY salary DESC"
    }

    def test_warmup_negative_cases = {
        // Enable file cache for warm up functionality
        sql "set enable_file_cache=true"
        sql "set disable_file_cache=false"
        
        // These should fail as warm up select doesn't support these operations
        try {
            sql "WARM UP SELECT * FROM test_warmup_table LIMIT 5"
            assert false : "Expected ParseException for LIMIT clause"
        } catch (Exception e) {
            // Expected to fail
            println "LIMIT clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT id, COUNT(*) FROM test_warmup_table GROUP BY id"
            assert false : "Expected ParseException for GROUP BY clause"
        } catch (Exception e) {
            // Expected to fail
            println "GROUP BY clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT * FROM test_warmup_table t1 JOIN test_warmup_table t2 ON t1.id = t2.id"
            assert false : "Expected ParseException for JOIN clause"
        } catch (Exception e) {
            // Expected to fail
            println "JOIN clause correctly rejected for WARM UP SELECT"
        }
        
        try {
            sql "WARM UP SELECT * FROM test_warmup_table UNION SELECT * FROM test_warmup_table"
            assert false : "Expected ParseException for UNION clause"
        } catch (Exception e) {
            // Expected to fail
            println "UNION clause correctly rejected for WARM UP SELECT"
        }
    }

    // Run the tests
    try {
        setup_test_table()
        test_basic_warmup()
        test_warmup_negative_cases()
        println "All warm up select tests passed!"
    } finally {
        cleanup_test_table()
    }
}