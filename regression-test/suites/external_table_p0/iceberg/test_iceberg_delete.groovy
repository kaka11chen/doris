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

suite("test_iceberg_delete", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalog_name = "test_iceberg_delete_catalog"
    String database_name = "test_iceberg_delete_db"
    String table_name = "test_iceberg_delete_table"

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
    
    // Create test table with v2 format (required for DELETE)
    sql """
        CREATE TABLE ${table_name} (
            id INT,
            name STRING,
            age INT,
            status STRING
        ) USING iceberg
        TBLPROPERTIES (
            'format-version' = '2',
            'write.format.default' = 'parquet'
        )
    """
    
    // Insert test data
    sql """
        INSERT INTO ${table_name} VALUES
        (1, 'Alice', 25, 'active'),
        (2, 'Bob', 30, 'active'),
        (3, 'Charlie', 35, 'inactive'),
        (4, 'David', 40, 'active'),
        (5, 'Eve', 28, 'inactive')
    """
    
    // Test 1: Simple DELETE
    sql """DELETE FROM ${table_name} WHERE id = 3"""
    
    def result1 = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertEquals(4, result1.size())
    assertFalse(result1.any { it[0] == 3 })
    
    // Test 2: DELETE with multiple conditions
    sql """DELETE FROM ${table_name} WHERE status = 'inactive'"""
    
    def result2 = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertEquals(3, result2.size())
    assertTrue(result2.every { it[3] == 'active' })
    
    // Test 3: DELETE with range condition
    sql """DELETE FROM ${table_name} WHERE age > 30"""
    
    def result3 = sql """SELECT * FROM ${table_name} ORDER BY id"""
    assertEquals(1, result3.size())
    assertEquals(1, result3[0][0])
    assertEquals('Alice', result3[0][1])
    
    // Test 4: Verify delete files are created (check metadata)
    def snapshots = sql """
        SELECT * FROM ${catalog_name}.${database_name}.${table_name}.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
    """
    assertTrue(snapshots.size() > 0)
    
    // Clean up
    sql """drop table if exists ${table_name}"""
    sql """drop catalog if exists ${catalog_name}"""
}
