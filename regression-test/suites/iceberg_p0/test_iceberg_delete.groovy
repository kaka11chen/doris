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
        logger.info("disable iceberg delete test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_delete_catalog"
    String db_name = "test_iceberg_delete_db"

    // Create Iceberg catalog
    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            'io-impl'='org.apache.iceberg.aws.s3.S3FileIO',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.region' = 'us-east-1'
        );
    """

    // Create database if not exists
    sql """use ${catalog_name}"""
    sql """CREATE DATABASE IF NOT EXISTS ${db_name}"""
    sql """use ${db_name}"""

    // Test 1: Basic Position Delete
    def table_name_pos_delete = "test_position_delete"
    sql """DROP TABLE IF EXISTS ${table_name_pos_delete}"""
    sql """
        CREATE TABLE ${table_name_pos_delete} (
            id INT,
            name STRING,
            age INT
        ) USING ICEBERG
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
    """

    // Insert test data
    sql """INSERT INTO ${table_name_pos_delete} VALUES (1, 'Alice', 30)"""
    sql """INSERT INTO ${table_name_pos_delete} VALUES (2, 'Bob', 25)"""
    sql """INSERT INTO ${table_name_pos_delete} VALUES (3, 'Charlie', 35)"""

    // Verify initial count
    def count1 = sql """SELECT COUNT(*) FROM ${table_name_pos_delete}"""
    assertEquals(3, count1[0][0])

    // Perform position delete
    sql """DELETE FROM ${table_name_pos_delete} WHERE id = 1"""

    // Verify count after delete
    def count2 = sql """SELECT COUNT(*) FROM ${table_name_pos_delete}"""
    assertEquals(2, count2[0][0])

    // Verify remaining data
    def result1 = sql """SELECT id FROM ${table_name_pos_delete} ORDER BY id"""
    assertEquals(2, result1.size())
    assertEquals(2, result1[0][0])
    assertEquals(3, result1[1][0])

    // Perform another delete
    sql """DELETE FROM ${table_name_pos_delete} WHERE age > 30"""

    // Verify final count
    def count3 = sql """SELECT COUNT(*) FROM ${table_name_pos_delete}"""
    assertEquals(1, count3[0][0])

    // Test 2: Delete with complex WHERE clause
    def table_name_complex = "test_complex_delete"
    sql """DROP TABLE IF EXISTS ${table_name_complex}"""
    sql """
        CREATE TABLE ${table_name_complex} (
            id INT,
            category STRING,
            value DOUBLE
        ) USING ICEBERG
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
    """

    // Insert test data
    sql """INSERT INTO ${table_name_complex} VALUES (1, 'A', 10.5)"""
    sql """INSERT INTO ${table_name_complex} VALUES (2, 'B', 20.5)"""
    sql """INSERT INTO ${table_name_complex} VALUES (3, 'A', 30.5)"""
    sql """INSERT INTO ${table_name_complex} VALUES (4, 'C', 40.5)"""

    // Delete with AND condition
    sql """DELETE FROM ${table_name_complex} WHERE category = 'A' AND value > 15.0"""

    def count4 = sql """SELECT COUNT(*) FROM ${table_name_complex}"""
    assertEquals(3, count4[0][0])

    // Verify remaining data
    def result2 = sql """SELECT id FROM ${table_name_complex} WHERE category = 'A'"""
    assertEquals(1, result2.size())
    assertEquals(1, result2[0][0])

    // Test 3: Partitioned table DELETE
    def table_name_partitioned = "test_partitioned_delete"
    sql """DROP TABLE IF EXISTS ${table_name_partitioned}"""
    sql """
        CREATE TABLE ${table_name_partitioned} (
            id INT,
            dt STRING,
            value INT
        ) USING ICEBERG
        PARTITIONED BY (dt)
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
    """

    // Insert partitioned data
    sql """INSERT INTO ${table_name_partitioned} VALUES (1, '2024-01-01', 100)"""
    sql """INSERT INTO ${table_name_partitioned} VALUES (2, '2024-01-01', 200)"""
    sql """INSERT INTO ${table_name_partitioned} VALUES (3, '2024-01-02', 300)"""
    sql """INSERT INTO ${table_name_partitioned} VALUES (4, '2024-01-02', 400)"""

    // Delete from specific partition
    sql """DELETE FROM ${table_name_partitioned} WHERE dt = '2024-01-01'"""

    def count5 = sql """SELECT COUNT(*) FROM ${table_name_partitioned}"""
    assertEquals(2, count5[0][0])

    // Verify remaining data is from correct partition
    def result3 = sql """SELECT dt FROM ${table_name_partitioned}"""
    assertEquals(2, result3.size())
    assertEquals('2024-01-02', result3[0][0])
    assertEquals('2024-01-02', result3[1][0])

    // Test 4: DELETE all rows
    def table_name_delete_all = "test_delete_all"
    sql """DROP TABLE IF EXISTS ${table_name_delete_all}"""
    sql """
        CREATE TABLE ${table_name_delete_all} (
            id INT,
            name STRING
        ) USING ICEBERG
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
    """

    sql """INSERT INTO ${table_name_delete_all} VALUES (1, 'test')"""
    sql """INSERT INTO ${table_name_delete_all} VALUES (2, 'test2')"""

    // Delete all rows
    sql """DELETE FROM ${table_name_delete_all} WHERE 1=1"""

    def count6 = sql """SELECT COUNT(*) FROM ${table_name_delete_all}"""
    assertEquals(0, count6[0][0])

    // Test 5: Multiple DELETEs in sequence
    def table_name_multiple = "test_multiple_deletes"
    sql """DROP TABLE IF EXISTS ${table_name_multiple}"""
    sql """
        CREATE TABLE ${table_name_multiple} (
            id INT,
            status STRING
        ) USING ICEBERG
        TBLPROPERTIES (
            'format-version'='2',
            'write.format.default'='parquet'
        )
    """

    // Insert test data
    for (int i = 1; i <= 10; i++) {
        sql """INSERT INTO ${table_name_multiple} VALUES (${i}, 'active')"""
    }

    // Perform multiple deletes
    sql """DELETE FROM ${table_name_multiple} WHERE id <= 3"""
    sql """DELETE FROM ${table_name_multiple} WHERE id >= 8"""
    sql """DELETE FROM ${table_name_multiple} WHERE id = 5"""

    def count7 = sql """SELECT COUNT(*) FROM ${table_name_multiple}"""
    assertEquals(4, count7[0][0])

    // Verify remaining IDs
    def result4 = sql """SELECT id FROM ${table_name_multiple} ORDER BY id"""
    assertEquals(4, result4.size())
    assertEquals(4, result4[0][0])
    assertEquals(6, result4[1][0])
    assertEquals(7, result4[2][0])
    assertEquals(8, result4[3][0])

    // Cleanup
    sql """DROP TABLE IF EXISTS ${table_name_pos_delete}"""
    sql """DROP TABLE IF EXISTS ${table_name_complex}"""
    sql """DROP TABLE IF EXISTS ${table_name_partitioned}"""
    sql """DROP TABLE IF EXISTS ${table_name_delete_all}"""
    sql """DROP TABLE IF EXISTS ${table_name_multiple}"""
    
    sql """DROP DATABASE IF EXISTS ${db_name}"""
    sql """DROP CATALOG IF EXISTS ${catalog_name}"""
}
