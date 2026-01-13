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

suite("test_iceberg_merge_into_basic", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_merge_into_basic"
    String dbName = "test_merge_into_basic_db"
    String tableName = "test_merge_into_basic_tbl"
    String tableNamePartition = "test_merge_into_basic_tbl_par"
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

    qt_q01 """
        MERGE INTO ${tableName} t
        USING (
            SELECT 1 AS id, 'Alice_new' AS name, 26 AS age, 'U' AS flag
            UNION ALL
            SELECT 2, 'Bob', 30, 'D'
            UNION ALL
            SELECT 4, 'Dora', 28, 'I'
        ) s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            name = s.name,
            age = s.age
        WHEN NOT MATCHED THEN INSERT (id, name, age)
        VALUES (s.id, s.name, s.age)
    """

    qt_order_q02 """SELECT * FROM ${tableName}"""
    // assertEquals(3, rows.size())
    // assertEquals([1, "Alice_new", 26], rows[0])
    // assertEquals([3, "Charlie", 35], rows[1])
    // assertEquals([4, "Dora", 28], rows[2])

    qt_order_q03 """
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
        (1, 'Alice', 25, '2024-01-01'),
        (2, 'Bob', 30, '2024-01-02'),
        (3, 'Charlie', 35, '2024-01-03')
    """

    qt_q04 """
        MERGE INTO ${tableNamePartition} t
        USING (
            SELECT 1 AS id, 'Alice_new' AS name, 26 AS age, DATE '2024-01-01' AS dt, 'U' AS flag
            UNION ALL
            SELECT 2, 'Bob', 30, DATE '2024-01-02', 'D'
            UNION ALL
            SELECT 4, 'Dora', 28, DATE '2024-01-04', 'I'
        ) s
        ON t.id = s.id
        WHEN MATCHED AND s.flag = 'D' THEN DELETE
        WHEN MATCHED THEN UPDATE SET
            name = s.name,
            age = s.age
        WHEN NOT MATCHED THEN INSERT (id, name, age, dt)
        VALUES (s.id, s.name, s.age, s.dt)
    """

    qt_order_q05 """SELECT * FROM ${tableNamePartition}"""

    qt_order_q06 """
        SELECT * FROM ${catalogName}.${dbName}.${tableNamePartition}\$delete_files
    """

    sql """drop table if exists ${tableName}"""
    sql """drop table if exists ${tableNamePartition}"""
    sql """drop database if exists ${dbName} force"""
    sql """drop catalog if exists ${catalogName}"""
}
