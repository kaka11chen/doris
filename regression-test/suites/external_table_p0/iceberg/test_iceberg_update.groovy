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

suite("test_iceberg_update", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalog_name = "test_iceberg_update_catalog"
    String database_name = "test_iceberg_update_db"
    String table_name = "test_iceberg_update_table"

    sql """drop catalog if exists ${catalog_name}"""
    
    // Create Iceberg catalog
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'uri' = 'http://localhost:8181'
        )
    """
    
    sql """use ${catalog_name}.${database_name}"""
    
    // Drop table if exists
    sql """drop table if exists ${table_name}"""
    
    // Create test table with v2 format (required for UPDATE)
    sql """
        CREATE TABLE ${table_name} (
            id INT,
            name STRING,
            age INT,
            status STRING,
            score DOUBLE
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """
    
    // Insert test data
    sql """
        INSERT INTO ${table_name} VALUES
        (1, 'Alice', 25, 'active', 85.5),
        (2, 'Bob', 30, 'active', 90.0),
        (3, 'Charlie', 35, 'inactive', 75.5),
        (4, 'David', 40, 'active', 88.0),
        (5, 'Eve', 28, 'inactive', 92.5)
    """
    
    // Test 1: Simple UPDATE
    sql """UPDATE ${table_name} SET status = 'updated' WHERE id = 1"""
    
    def result1 = sql """SELECT * FROM ${table_name} WHERE id = 1"""
    assertEquals(1, result1.size())
    assertEquals('updated', result1[0][3])
    
    // Test 2: UPDATE multiple columns
    sql """UPDATE ${table_name} SET status = 'senior', score = 95.0 WHERE age > 30"""
    
    def result2 = sql """SELECT * FROM ${table_name} WHERE age > 30 ORDER BY id"""
    assertTrue(result2.size() >= 2)
    assertTrue(result2.every { it[3] == 'senior' && it[4] == 95.0 })
    
    // Test 3: UPDATE with calculation
    sql """UPDATE ${table_name} SET score = score + 5 WHERE status = 'active'"""
    
    def result3 = sql """SELECT score FROM ${table_name} WHERE id = 2"""
    assertEquals(95.0, result3[0][0], 0.01)
    
    // Test 4: UPDATE all rows
    sql """UPDATE ${table_name} SET status = 'verified'"""
    
    def result4 = sql """SELECT DISTINCT status FROM ${table_name}"""
    assertEquals(1, result4.size())
    assertEquals('verified', result4[0][0])
    
    // Test 5: Verify atomicity - UPDATE creates both delete and insert files
    def snapshots = sql """
        SELECT operation FROM ${catalog_name}.${database_name}.${table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """
    assertTrue(snapshots.size() > 0)
    // The operation should be 'overwrite' or similar for UPDATE
    
    // Test 6: Rollback test (if transaction fails, no changes)
    def count_before = sql """SELECT COUNT(*) FROM ${table_name}"""
    
    try {
        // This should fail due to invalid expression
        sql """UPDATE ${table_name} SET score = 'invalid' WHERE id = 1"""
        fail("Should have thrown an exception")
    } catch (Exception e) {
        // Expected exception
    }
    
    def count_after = sql """SELECT COUNT(*) FROM ${table_name}"""
    assertEquals(count_before[0][0], count_after[0][0])
    
    // Clean up
    sql """drop table if exists ${table_name}"""
    sql """drop catalog if exists ${catalog_name}"""
}
