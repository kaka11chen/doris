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

suite("test_iceberg_update_delete_basic", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_update_delete_basic"
    String dbName = "test_update_delete_basic_db"
    String tableName = "test_update_delete_basic_tbl"
    String tableNamePartition = "test_update_delete_basic_tbl_par"
    String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    sql """drop catalog if exists ${catalogName}"""
    sql """
        create catalog if not exists ${catalogName} properties (
            "type" = "iceberg",
            "iceberg.catalog.type" = "rest",
            "uri" = "http://${externalEnvIp}:${restPort}",
            "s3.access_key" = "admin",
            "s3.secret_key" = "password",
            "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
            "s3.region" = "us-east-1"
        )
    """

    sql """switch ${catalogName}"""
    sql """create database if not exists ${dbName}"""
    sql """use ${dbName}"""

    sql """drop table if exists ${tableName}"""
    sql """
        CREATE TABLE ${tableName} (
            id INT,
            name STRING,
            age INT
        ) ENGINE=iceberg
    """

    sql """
        INSERT INTO ${tableName} VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35)
    """

    qt_q01 """UPDATE ${tableName} SET name = 'Updated' WHERE id = 1"""
    qt_order_q02 """SELECT * FROM ${tableName}"""
    // assertEquals(1, updated.size())
    // assertEquals("Updated", updated[0][0])

    qt_q03 """DELETE FROM ${tableName} WHERE id = 2"""
    qt_order_q04 """SELECT * FROM ${tableName}"""
    // assertEquals(2, countAfterDelete[0][0])

    qt_order_q05 """
        SELECT * FROM ${catalogName}.${dbName}.${tableName}\$delete_files
    """
    // assertTrue(deleteFiles.size() > 0)

    sql """drop table if exists ${tableNamePartition}"""
    sql """
        CREATE TABLE ${tableNamePartition} (
            id INT,
            name STRING,
            age INT,
            dt DATE
        ) ENGINE=iceberg
        PARTITION BY LIST (DAY(dt)) ()
    """

    sql """
        INSERT INTO ${tableNamePartition} VALUES
        (10, 'Ann', 20, '2024-01-01'),
        (11, 'Ben', 21, '2024-01-02'),
        (12, 'Cat', 22, '2024-01-03')
    """

    qt_q06 """UPDATE ${tableNamePartition} SET name = 'UpdatedP' WHERE id = 10"""
    qt_order_q07 """SELECT * FROM ${tableNamePartition}"""

    qt_q08 """DELETE FROM ${tableNamePartition} WHERE id = 11"""
    qt_order_q09 """SELECT * FROM ${tableNamePartition}"""

    qt_order_q10 """
        SELECT * FROM ${catalogName}.${dbName}.${tableNamePartition}\$delete_files
    """

    sql """drop table if exists ${tableName}"""
    sql """drop table if exists ${tableNamePartition}"""
    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
