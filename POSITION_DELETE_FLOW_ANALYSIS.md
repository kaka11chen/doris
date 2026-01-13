# Position Delete 完整流程分析

## 📋 概述

本文档详细分析 Doris 中 Iceberg Position Delete 的完整调用流程，并标注出当前实现中缺失的部分。

## 🔄 完整流程图

```
┌─────────────────────────────────────────────────────────────┐
│                    1. SQL 解析与路由                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
    DELETE FROM iceberg_table WHERE condition
                            ↓
    ┌──────────────────────────────────────────┐
    │ DeleteFromCommand.run()                  │
    │ └─ 判断是否为 IcebergExternalTable        │
    │    └─ 是 → IcebergDeleteCommand.run()    │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              2. FE 端 - 查询计划构建                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergDeleteCommand.run()               │
    │ 1. 验证表格式版本 (>= v2)                 │ ✅ 已实现
    │ 2. 调用 completeQueryPlan()               │ ✅ 已实现
    │ 3. 创建 InsertIntoTableCommand            │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergDeleteCommand.completeQueryPlan() │
    │ 1. Position Delete 模式:                  │
    │    └─ 添加 $row_id 列到投影               │ ✅ 已实现
    │       STRUCT<file_path, row_position,    │
    │              partition_spec_id,           │
    │              partition_data>              │
    │ 2. Equality Delete 模式:                  │
    │    └─ 添加 equality 列到投影              │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ InsertIntoTableCommand.run()             │
    │ └─ 创建 IcebergDeleteExecutor            │ ⚠️  问题：如何判断是 DELETE？
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              3. FE 端 - 执行器初始化                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergDeleteExecutor 构造函数            │
    │ └─ 传入 DeleteCommandContext              │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergDeleteExecutor.beforeExec()       │
    │ └─ IcebergTransaction.beginDelete()      │ ✅ 已实现
    │    └─ 创建 Iceberg transaction            │ ✅ 已实现
    │    └─ 验证表格式版本                       │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              4. BE 端 - 数据扫描与 $row_id 生成              │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergTableReader.init_row_filters()    │
    │ 1. 检查 _need_row_id_column 标志          │ ✅ 已实现
    │ 2. 自动调用 set_current_file_info()      │ ✅ 已实现
    │    └─ 从 table_desc 提取:                 │
    │       - file_path (original_file_path)   │ ✅ 已实现
    │       - partition_spec_id (默认 0)        │ ⚠️  暂时硬编码
    │       - partition_data_json (空)          │ ⚠️  暂时硬编码
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergTableReader.get_next_block()      │
    │ 1. 读取数据块                             │ ✅ 已实现
    │ 2. 应用 WHERE 过滤条件                    │ ✅ 已实现
    │ 3. 如果 _need_row_id_column:             │
    │    └─ 调用 _append_row_id_column()       │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergTableReader._append_row_id_column()│
    │ 为每一行生成 STRUCT:                       │
    │ {                                        │
    │   file_path: "/path/to/data.parquet",   │ ✅ 已实现
    │   row_position: 12345,                   │ ✅ 已实现
    │   partition_spec_id: 0,                  │ ✅ 已实现
    │   partition_data: "{...}"                │ ✅ 已实现
    │ }                                        │
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         5. BE 端 - Position Delete File 写入                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ ❌ 缺失：VIcebergDeleteFileWriter        │
    │                                          │
    │ 应该实现的功能:                           │
    │ 1. 从查询结果中提取 $row_id 列             │ ❌ 未实现
    │ 2. 按 file_path 分组                      │ ❌ 未实现
    │ 3. 写入 Parquet 格式:                     │ ❌ 未实现
    │    Schema: (file_path: STRING,           │
    │             pos: BIGINT)                 │
    │ 4. 生成 TIcebergCommitData               │ ❌ 未实现
    │    └─ file_content = POSITION_DELETES    │
    │    └─ referenced_data_file_path          │
    │    └─ partition_spec_id                  │
    │    └─ partition_data_json                │
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ ❌ 缺失：VIcebergDeleteFileWriter.close()│
    │                                          │
    │ 应该实现的功能:                           │
    │ 1. 完成 Parquet 文件写入                  │ ❌ 未实现
    │ 2. 收集 delete file 元数据:               │ ❌ 未实现
    │    - file_path (delete file 路径)        │
    │    - file_size                           │
    │    - record_count                        │
    │ 3. 将元数据封装到 TIcebergCommitData      │ ❌ 未实现
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ ❌ 缺失：发送 TIcebergCommitData 到 FE   │
    │                                          │
    │ 类似 INSERT 操作:                         │
    │ VOlapTableSink / VHiveTableSink 会将     │ ❌ 未实现
    │ commit data 发送回 FE。                   │
    │                                          │
    │ 需要为 DELETE 实现类似机制。              │
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         6. FE 端 - 收集 Commit Data                          │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergTransaction.updateIcebergCommitData()│
    │ └─ 收集 BE 返回的 TIcebergCommitData      │ ✅ 已实现
    │    到 commitDataList                      │ ✅ 已实现
    └──────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│         7. FE 端 - 提交事务                                  │
└─────────────────────────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergDeleteExecutor.doBeforeCommit()   │
    │ 1. 调用 extractRowIdData()                │ ✅ 已实现
    │    └─ 解析 commitDataList                 │ ✅ 已实现
    │    └─ 按 file_path 分组                   │ ✅ 已实现
    │ 2. 调用 writePositionDeleteFile()        │ ⚠️  仅日志记录
    │    └─ (目前只是日志，实际由              │
    │        IcebergWriterHelper 处理)         │
    └──────────────────────────────────────────┘
                            ↓
    ┌──────────────────────────────────────────┐
    │ IcebergTransaction.finishDelete()        │
    │ 1. 调用 IcebergWriterHelper.             │ ✅ 已实现
    │    convertToDeleteFiles()                 │
    │    └─ 将 TIcebergCommitData 转换为       │ ✅ 已实现
    │       Iceberg DeleteFile 对象             │
    │ 2. 创建 RowDelta                          │ ✅ 已实现
    │ 3. 添加 delete files                      │ ✅ 已实现
    │ 4. 提交事务                               │ ✅ 已实现
    └──────────────────────────────────────────┘

## 🔴 关键缺失部分

### 1. **BE 端 Delete File 写入器 - 最关键的缺失**

目前 **最大的问题** 是 BE 端没有实际写入 Position Delete 文件的组件。

**缺失的组件**：
```cpp
// be/src/vec/sink/writer/viceberg_delete_file_writer.h
// be/src/vec/sink/writer/viceberg_delete_file_writer.cpp

class VIcebergDeleteFileWriter {
public:
    // 从 Block 中提取 $row_id 列并写入
    Status write_block(Block* block);
    
    // 完成写入，返回 TIcebergCommitData
    Status close(TIcebergCommitData* commit_data);
    
private:
    // Parquet writer for delete file
    std::unique_ptr<VParquetTransformer> _parquet_writer;
    
    // Group rows by file_path
    std::map<std::string, std::vector<int64_t>> _positions_by_file;
};
```

### 2. **Sink 节点 - 连接查询与写入器**

**缺失的组件**：
```java
// UnboundIcebergDeleteSink.java - 逻辑计划节点
// PhysicalIcebergDeleteSink.java - 物理计划节点
```

**当前问题**：
- `IcebergDeleteCommand` 创建了 `InsertIntoTableCommand`
- 但 `InsertIntoTableCommand` 不知道这是一个 DELETE 操作
- 需要一个专门的 Sink 来处理 DELETE

**可能的解决方案**：

**方案 A - 扩展现有 Sink**：
```java
// 在 IcebergTableSink 中添加 DELETE 模式
public class IcebergTableSink extends TableSink {
    private final DMLCommandType commandType; // INSERT, DELETE, UPDATE
    
    @Override
    public PhysicalSink toPhysicalSink() {
        if (commandType == DMLCommandType.DELETE) {
            return new PhysicalIcebergDeleteSink(...);
        }
        return new PhysicalIcebergTableSink(...);
    }
}
```

**方案 B - 创建专门的 DeleteSink**：
```java
// 新建专门用于 DELETE 的 Sink
public class IcebergDeleteSink extends TableSink {
    private final DeleteCommandContext deleteCtx;
    
    @Override
    public PhysicalSink toPhysicalSink() {
        return new PhysicalIcebergDeleteSink(deleteCtx);
    }
}
```

### 3. **如何将 DeleteCommandContext 传递到 Sink**

**当前问题**：
```java
// IcebergDeleteCommand.java:119
InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(
        deleteQueryPlan,  // 查询计划
        Optional.empty(), // ❓ 如何传递 deleteCtx？
        ...);
```

**InsertIntoTableCommand 不知道这是一个 DELETE 操作**。

**可能的解决方案**：

1. **在 LogicalPlan 中添加 DeleteSink 节点**：
```java
// 在 completeQueryPlan() 中：
LogicalPlan planWithDeleteSink = new UnboundIcebergDeleteSink(
    deleteCtx,
    completeQueryPlan
);
```

2. **或者使用 TableIf 的属性**：
```java
// 在 IcebergExternalTable 中临时存储 DeleteCommandContext
icebergTable.setTempAttribute("deleteContext", deleteCtx);
```

### 4. **BE 端的数据流向**

**当前问题**：
- `IcebergTableReader` 生成了 `$row_id` 列 ✅
- 但这个 `$row_id` 列去哪里了？ ❌
- 没有组件来消费这个列并写入 delete file ❌

**应该的流程**：
```
IcebergTableReader (生成 $row_id)
       ↓
   [过滤操作]
       ↓
VIcebergDeleteSink (BE 端 Sink)
       ↓
VIcebergDeleteFileWriter
       ↓
写入 Position Delete Parquet 文件
       ↓
生成 TIcebergCommitData
       ↓
发送回 FE
```

### 5. **Thrift 定义完整性**

需要确认 `TIcebergCommitData` 是否包含所有必要字段：

```thrift
// DataSinks.thrift
struct TIcebergCommitData {
    // ... 现有字段 ...
    
    // Position Delete 专用:
    7: optional TFileContent file_content;              // ✅ 已添加
    8: optional string referenced_data_file_path;       // ✅ 已添加
    9: optional list<i64> positions;                    // ❓ 需要添加？
    10: optional i32 partition_spec_id;                 // ✅ 已添加
    11: optional string partition_data_json;            // ✅ 已添加
}
```

**需要检查**：
- `positions` 字段是否已添加？这是 BE 向 FE 传递行位置的关键。

## 🎯 最小可行方案（MVP）

要让 Position Delete 工作，**最少**需要实现：

### Phase 1: BE 端写入器（核心）

```cpp
// be/src/vec/sink/viceberg_delete_sink.h
class VIcebergDeleteSink : public DataSink {
public:
    Status send(Block* block) override;
    Status close() override;
    
private:
    void extract_row_id_column(Block* block);
    Status write_delete_file();
    std::unique_ptr<VIcebergDeleteFileWriter> _writer;
};
```

### Phase 2: FE 端 Sink 节点

```java
// UnboundIcebergDeleteSink.java
public class UnboundIcebergDeleteSink extends UnboundTableSink {
    private final DeleteCommandContext deleteCtx;
    
    @Override
    public Plan bind(CascadesContext context) {
        return new PhysicalIcebergDeleteSink(...);
    }
}
```

### Phase 3: 连接起来

修改 `IcebergDeleteCommand.completeQueryPlan()` 添加 DeleteSink：
```java
// 在查询计划最后添加 UnboundIcebergDeleteSink
return new UnboundIcebergDeleteSink(
    deleteCtx,
    projectPlan
);
```

## 📊 实现优先级

| 优先级 | 组件 | 状态 | 重要性 |
|-------|------|------|--------|
| P0 | VIcebergDeleteFileWriter | ❌ 未实现 | 🔴 **阻塞** |
| P0 | VIcebergDeleteSink | ❌ 未实现 | 🔴 **阻塞** |
| P0 | TIcebergCommitData.positions | ❓ 待确认 | 🔴 **阻塞** |
| P1 | UnboundIcebergDeleteSink | ❌ 未实现 | 🟡 高 |
| P1 | PhysicalIcebergDeleteSink | ❌ 未实现 | 🟡 高 |
| P2 | partition_spec_id 正确传递 | ⚠️  硬编码 | 🟢 中 |
| P2 | partition_data_json 正确传递 | ⚠️  硬编码 | 🟢 中 |

## 🔍 调试建议

1. **启用详细日志**：
   ```sql
   SET enable_nereids_planner = true;
   SET enable_profile = true;
   ```

2. **查看生成的执行计划**：
   ```sql
   EXPLAIN DELETE FROM iceberg_table WHERE id = 1;
   ```
   
   检查是否有 `$row_id` 列的投影。

3. **检查 BE 端数据流**：
   在 `IcebergTableReader::_append_row_id_column()` 添加日志，
   确认 `$row_id` 列是否正确生成。

4. **检查是否有 Sink**：
   查看执行计划中是否有 DELETE Sink 节点。
   如果没有，说明 Sink 部分完全缺失。

## 📝 总结

**当前完成度：约 60%**

✅ **已完成**：
- FE 端命令路由和查询计划构建
- `$row_id` 元数据列定义
- BE 端 `$row_id` 列生成
- FE 端事务管理和 commit data 收集

❌ **缺失（阻塞）**：
- **BE 端 Delete File 写入器** - 最关键
- **BE 端 Delete Sink 节点**
- **FE 端 Delete Sink 计划节点**
- **Sink 与写入器的连接**

⚠️ **需要增强**：
- `partition_spec_id` 和 `partition_data_json` 的正确传递
- `TIcebergCommitData.positions` 字段
- 完整的端到端测试

## 🚀 下一步行动

1. **立即实施**：创建 `VIcebergDeleteFileWriter`
2. **然后**：创建 `VIcebergDeleteSink`
3. **接着**：创建 FE 端 `UnboundIcebergDeleteSink` 和 `PhysicalIcebergDeleteSink`
4. **最后**：连接整个流程并测试
