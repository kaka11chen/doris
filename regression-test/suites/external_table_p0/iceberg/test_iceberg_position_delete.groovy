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

suite("test_iceberg_position_delete", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalog_name = "test_iceberg_position_delete"
    String db_name = "test_db"
    String table_name = "test_position_delete_table"

    sql """drop catalog if exists ${catalog_name};"""

    // Create Iceberg catalog
    sql """
        create catalog if not exists ${catalog_name} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "hadoop",
            "warehouse" = "${context.config.otherConfigs.get("icebergWarehousePath")}"
        );
    """

    sql """use ${catalog_name}.${db_name}"""

    // Create test table with format version 2 (required for DELETE operations)
    sql """
        CREATE TABLE IF NOT EXISTS ${table_name} (
            id INT,
            name STRING,
            age INT,
            city STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version' = '2')
    """

    // Insert test data
    sql """
        INSERT INTO ${table_name} VALUES 
        (1, 'Alice', 25, 'Beijing'),
        (2, 'Bob', 30, 'Shanghai'),
        (3, 'Charlie', 35, 'Guangzhou'),
        (4, 'David', 40, 'Shenzhen'),
        (5, 'Eve', 28, 'Hangzhou')
    """

    // Verify initial data
    qt_select_all """SELECT * FROM ${table_name} ORDER BY id"""

    // Test 1: Delete single row by ID
    sql """DELETE FROM ${table_name} WHERE id = 1"""
    
    qt_after_delete_one """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Verify count
    def count_after_one = sql """SELECT COUNT(*) FROM ${table_name}"""
    assert count_after_one[0][0] == 4

    // Test 2: Delete multiple rows with condition
    sql """DELETE FROM ${table_name} WHERE age > 30"""
    
    qt_after_delete_multiple """SELECT * FROM ${table_name} ORDER BY id"""
    
    // Verify count
    def count_after_multiple = sql """SELECT COUNT(*) FROM ${table_name}"""
    assert count_after_multiple[0][0] == 2

    // Test 3: Verify Position Delete files are created
    // Query Iceberg metadata to check delete files
    def delete_files = sql """
        SELECT * FROM ${catalog_name}.${db_name}.${table_name}\$delete_files
    """
    
    logger.info("Delete files count: " + delete_files.size())
    assert delete_files.size() > 0 : "Position Delete files should be created"

    // Test 4: Verify data consistency after DELETE
    def remaining_data = sql """SELECT id, name FROM ${table_name} ORDER BY id"""
    assert remaining_data.size() == 2
    assert remaining_data[0][0] == 2  // Bob
    assert remaining_data[1][0] == 5  // Eve

    // Cleanup
    sql """DROP TABLE IF EXISTS ${table_name}"""
    sql """drop catalog if exists ${catalog_name}"""

    logger.info("Iceberg Position Delete test completed successfully")
}
