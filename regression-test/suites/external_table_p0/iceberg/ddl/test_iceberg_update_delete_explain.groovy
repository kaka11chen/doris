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

suite("test_iceberg_update_delete_explain", "p0,external,iceberg,external_docker,external_docker_iceberg") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("Iceberg test is disabled")
        return
    }

    String catalogName = "test_iceberg_update_delete_explain"
    String dbName = "test_update_delete_explain_db"
    String tableName = "test_update_delete_explain_tbl"
    String tableNamePartition = "test_update_delete_explain_tbl_par"
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

    sql """INSERT INTO ${tableName} VALUES (1, 'Alice', 25), (2, 'Bob', 30)"""

    def updateExplain = sql """EXPLAIN DISTRIBUTED PLAN UPDATE ${tableName} SET name = 'Updated' WHERE id = 1"""
    String updateText = updateExplain.collect { it[0].toString() }.join("\n").toUpperCase()
    assertTrue(updateText.contains("EXCHANGE"))
    assertTrue(updateText.contains("ICEBERG MERGE SINK"))
    assertTrue(updateText.contains("MERGE_PARTITIONED"))
    assertTrue(updateText.contains("INSERT=RR"))
    assertTrue(updateText.contains("__DORIS_ICEBERG_ROWID_COL__"))

    def deleteExplain = sql """EXPLAIN DISTRIBUTED PLAN DELETE FROM ${tableName} WHERE id = 2"""
    String deleteText = deleteExplain.collect { it[0].toString() }.join("\n").toUpperCase()
    assertTrue(deleteText.contains("EXCHANGE"))
    assertTrue(deleteText.contains("ICEBERG DELETE SINK"))
    assertTrue(deleteText.contains("__DORIS_ICEBERG_ROWID_COL__"))

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

    sql """INSERT INTO ${tableNamePartition} VALUES (10, 'Ann', 20, '2024-01-01'), (11, 'Ben', 21, '2024-01-02')"""

    def updateExplainPartition = sql """
        EXPLAIN DISTRIBUTED PLAN UPDATE ${tableNamePartition} SET name = 'UpdatedP' WHERE id = 10
    """
    String updatePartitionText = updateExplainPartition.collect { it[0].toString() }.join("\n").toUpperCase()
    assertTrue(updatePartitionText.contains("EXCHANGE"))
    assertTrue(updatePartitionText.contains("ICEBERG MERGE SINK"))
    assertTrue(updatePartitionText.contains("MERGE_PARTITIONED"))
    assertTrue(updatePartitionText.contains("__DORIS_ICEBERG_ROWID_COL__"))

    def deleteExplainPartition = sql """
        EXPLAIN DISTRIBUTED PLAN DELETE FROM ${tableNamePartition} WHERE id = 11
    """
    String deletePartitionText = deleteExplainPartition.collect { it[0].toString() }.join("\n").toUpperCase()
    assertTrue(deletePartitionText.contains("EXCHANGE"))
    assertTrue(deletePartitionText.contains("ICEBERG DELETE SINK"))
    assertTrue(deletePartitionText.contains("__DORIS_ICEBERG_ROWID_COL__"))
}
