# Iceberg DELETE/UPDATE POC 实现说明

## 概述

本 POC 实现了 Apache Doris 对 Iceberg 表的 DELETE 和 UPDATE 操作支持。实现基于 Iceberg 的 DeleteFile 机制，避免了重写整个数据文件。

## 已实现组件

### 1. 数据结构扩展

#### TIcebergCommitData (gensrc/thrift/DataSinks.thrift)
扩展了 Thrift 结构以支持 DeleteFile 元数据：
- `equality_field_ids`: 预留字段（当前仅支持 Position Delete）
- `referenced_data_file_path`: Position delete 引用的数据文件路径
- `partition_spec_id`: 分区规范 ID
- `partition_data_json`: 分区数据 JSON

### 2. 命令层

#### DeleteCommandContext
删除操作上下文，仅支持 Position Delete：
- `POSITION_DELETE`: 基于文件路径和行位置删除

#### IcebergDeleteCommand
DELETE 命令处理器，负责：
- 验证表格式版本（>= v2）
- 生成包含删除信息的查询计划
- 调用 IcebergDeleteExecutor 执行删除

#### IcebergUpdateCommand
UPDATE 命令处理器，实现为 DELETE + INSERT：
- 生成删除旧行的 DeleteFile
- 插入更新后的新行
- 在同一事务中原子性提交

### 3. 执行层

#### IcebergDeleteExecutor
删除操作执行器：
- 继承 BaseExternalTableInsertExecutor
- 调用 IcebergTransaction.beginDelete() 开始删除事务
- 调用 IcebergTransaction.finishDelete() 提交 DeleteFile

### 4. 事务管理

#### IcebergTransaction 扩展
添加了删除操作支持：
- `beginDelete()`: 初始化删除事务，验证表版本
- `finishDelete()`: 完成删除，使用 RowDelta API 提交
- `updateManifestAfterDelete()`: 将 DeleteFile 添加到 Manifest

### 5. 写入辅助

#### IcebergWriterHelper 扩展
添加了 DeleteFile 转换功能：
- `convertToDeleteFiles()`: 将 TIcebergCommitData 转换为 DeleteFile
- 仅支持 Position Delete
- 处理分区信息和元数据

### 6. Planner 集成

#### DeleteFromCommand 扩展
添加了 Iceberg 表路由：
```java
// 检测 Iceberg 表并路由到 IcebergDeleteCommand
if (table instanceof IcebergExternalTable) {
    IcebergDeleteCommand icebergDeleteCommand = new IcebergDeleteCommand(...);
    icebergDeleteCommand.run(ctx, executor);
    return;
}
```

#### UpdateCommand 扩展
添加了 Iceberg 表路由：
```java
// 检测 Iceberg 表并路由到 IcebergUpdateCommand
if (table instanceof IcebergExternalTable) {
    IcebergUpdateCommand icebergUpdateCommand = new IcebergUpdateCommand(...);
    icebergUpdateCommand.run(ctx, executor);
    return;
}
```

## 实现原理

### DELETE 操作流程

```
SQL: DELETE FROM iceberg_table WHERE id = 1

1. Parser 解析生成 DeleteFromCommand
2. DeleteFromCommand 检测到 Iceberg 表，路由到 IcebergDeleteCommand
3. IcebergDeleteCommand:
   - 验证表格式版本 >= 2
   - 生成查询计划（包含删除条件）
   - 使用 Position Delete
4. IcebergDeleteExecutor:
   - 调用 IcebergTransaction.beginDelete()
   - 扫描并收集需要删除的行信息
   - BE 生成 DeleteFile 并返回元数据
5. IcebergTransaction.finishDelete():
   - 将 TIcebergCommitData 转换为 DeleteFile
   - 使用 RowDelta API 添加 DeleteFile
   - 提交事务
```

### UPDATE 操作流程

```
SQL: UPDATE iceberg_table SET col1 = value1 WHERE id = 1

1. Parser 解析生成 UpdateCommand
2. UpdateCommand 检测到 Iceberg 表，路由到 IcebergUpdateCommand
3. IcebergUpdateCommand:
   - 验证表格式版本 >= 2
   - 生成 DELETE 计划（删除旧行）
   - 生成 INSERT 计划（插入新行）
4. 执行 DELETE 部分：
   - 生成 DeleteFile 标记旧行
5. 执行 INSERT 部分：
   - 写入包含更新值的新数据文件
6. 在同一事务中原子性提交
```

### DeleteFile 生成

#### Position Delete
```
1. 扫描数据文件，记录满足条件的行
2. 为每个匹配行记录：
   - file_path: 数据文件路径
   - pos: 行在文件中的位置
3. BE 写入 Position Delete 文件
4. DeleteFile 格式：
   - Schema: (file_path: string, pos: long)
   - Data: (file_path, pos) 对
   - Metadata: referenced_data_file_path
```

## 待完善功能

### 1. $row_id 列支持（高优先级）
- 在 IcebergPageSourceProvider 中添加 $row_id 列生成
- $row_id 包含：file_path, row_position, partition_spec_id, partition_data
- 参考 Trino 的 MergeRowIdTransform 实现

### 2. BE 端 DeleteFile 写入器（高优先级）
需要实现：
- `IcebergDeleteFileWriter`: 写入 Position Delete
- 支持 Parquet/ORC 格式
- 返回 DeleteFile 元数据到 FE

### 3. UPDATE 原子性（中优先级）
- 确保 DELETE + INSERT 在同一事务中
- 实现回滚机制
- 添加冲突检测

### 4. 查询计划优化（中优先级）
- Position Delete 计划优化
- 基于成本的决策（行数、文件数等）
- 支持 rewrite 策略（大规模删除）

### 5. 测试（高优先级）
- 单元测试：各组件功能测试
- 集成测试：端到端测试
- 并发测试：多事务并发

## 使用示例

### DELETE 操作
```sql
-- Simple DELETE
DELETE FROM iceberg_catalog.db.table WHERE id = 1;

-- DELETE with complex condition
DELETE FROM iceberg_catalog.db.table 
WHERE date >= '2024-01-01' AND status = 'inactive';
```

### UPDATE 操作
```sql
-- Simple UPDATE
UPDATE iceberg_catalog.db.table 
SET status = 'active', updated_at = now() 
WHERE id = 1;

-- UPDATE with JOIN (暂未支持)
UPDATE iceberg_catalog.db.table1 t1
SET t1.value = t2.value
FROM table2 t2
WHERE t1.id = t2.id;
```

## 配置参数

### 性能调优
```sql
-- 设置批量删除阈值（超过则使用 rewrite）
SET delete_rewrite_threshold = 10000;

-- DeleteFile 大小控制
SET delete_file_max_size = '128MB';
```

## 参考资料

- Iceberg 规范: https://iceberg.apache.org/spec/
- Trino Iceberg 实现: `doris-master3/Trino_Iceberg_Update_Delete_原理与实现详解.md`
- Iceberg RowDelta API: https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/RowDelta.html

## 开发路线图

### Phase 1: 核心功能（已完成）
- [x] 数据结构扩展
- [x] 命令层实现
- [x] 执行器实现
- [x] 事务管理扩展
- [x] Planner 集成

### Phase 2: 完善功能（进行中）
- [~] $row_id 列支持
- [ ] BE 端 DeleteFile 写入器
- [ ] UPDATE 原子性保证
- [ ] 测试用例

### Phase 3: 优化（未开始）
- [ ] 自动模式选择
- [ ] 性能优化
- [ ] Rewrite 策略
- [ ] 监控和诊断

## 常见问题

### Q1: 为什么需要表格式版本 >= 2？
A: Iceberg v2+ 引入了 DeleteFile 机制，v1 不支持。

### Q2: 支持哪种 Delete 方式？
A: 当前仅支持 Position Delete（需要行位置）。

### Q3: DELETE 操作会立即物理删除数据吗？
A: 不会。DELETE 生成 DeleteFile 标记删除，物理删除由 Iceberg 的 compaction 完成。

### Q4: 如何回滚 DELETE 操作？
A: Iceberg 支持时间旅行，可以回滚到之前的快照。

## 贡献指南

欢迎贡献！请遵循以下步骤：

1. Fork 仓库
2. 创建功能分支
3. 提交测试用例
4. 提交 Pull Request

## 联系方式

如有问题，请联系开发团队或提交 Issue。
