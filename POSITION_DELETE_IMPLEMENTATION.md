# Position Delete 完整实现链路

参考 Trino 的实现，完整串联 Doris 的 Position Delete 链路。

## 1. 架构对比

### Trino 的实现链路
```
SQL DELETE
  ↓
转换为 MERGE (包含 $row_id 列)
  ↓
扫描阶段：注入 $row_id (file_path, pos, partition_spec_id, partition_data)
  ↓
执行阶段：IcebergMergeSink 收集删除信息到 FileDeletion
  ↓
写入阶段：PositionDeleteWriter 写入 Position Delete 文件
  ↓
提交阶段：RowDelta.addDeletes() 提交 DeleteFile
```

### Doris 的实现链路（需要补充）
```
SQL DELETE
  ↓
IcebergDeleteCommand (Nereids)
  ↓
生成查询计划（需要注入 $row_id 列）  ← 缺失
  ↓
IcebergDeleteExecutor 执行
  ↓
BE 扫描：读取数据 + 生成 $row_id     ← 缺失
  ↓
BE 收集：将匹配行的 $row_id 发送到 FE ← 缺失
  ↓
FE 写入 DeleteFile：VIcebergDeleteFileWriter  ✅ 已有
  ↓
提交：IcebergTransaction.finishDelete()  ✅ 已有
```

## 2. 关键缺失环节

### 2.1 $row_id 元数据列（FE 端）

**文件**: `fe/fe-core/.../IcebergMetadataColumn.java` （新建）

```java
public enum IcebergMetadataColumn {
    FILE_PATH("$file_path", Type.STRING),
    ROW_POSITION("$row_position", Type.BIGINT),
    PARTITION_SPEC_ID("$partition_spec_id", Type.INT),
    PARTITION_DATA("$partition_data", Type.STRING),
    ROW_ID("$row_id", Type.STRUCT); // 包含上面4个字段的结构体
    
    private final String columnName;
    private final Type type;
    
    public static boolean isMetadataColumn(String name) {
        return name.startsWith("$");
    }
}
```

### 2.2 查询计划中注入 $row_id

**文件**: `IcebergDeleteCommand.completeQueryPlan()`

```java
private LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery, 
                                      DeleteCommandContext deleteCtx) {
    // 1. 在扫描阶段注入 $row_id 列
    LogicalPlan scanWithRowId = injectRowIdColumn(logicalQuery, ctx);
    
    // 2. Position Delete only: 投影 $row_id
    List<NamedExpression> projectItems = Lists.newArrayList(
        new UnboundSlot(IcebergMetadataColumn.ROW_ID.getColumnName())
    );
    return new LogicalProject<>(projectItems, scanWithRowId);
}

private LogicalPlan injectRowIdColumn(LogicalPlan plan, ConnectContext ctx) {
    // 找到 UnboundRelation（表扫描）
    LogicalPlan newPlan = plan.rewriteUp(node -> {
        if (node instanceof UnboundRelation) {
            UnboundRelation relation = (UnboundRelation) node;
            // 添加 $row_id 元数据列到扫描列表
            return relation.withMetadataColumns(
                ImmutableList.of(IcebergMetadataColumn.ROW_ID.getColumnName())
            );
        }
        return node;
    });
    return newPlan;
}
```

### 2.3 BE 端：扫描时生成 $row_id

**文件**: `be/src/vec/exec/format/table/iceberg_reader.cpp`

需要在 `IcebergTableReader::get_next_block_inner()` 中添加 `$row_id` 列的生成：

```cpp
Status IcebergTableReader::get_next_block_inner(Block* block, size_t* read_rows, bool* eof) {
    // 1. 读取数据
    RETURN_IF_ERROR(_file_format_reader->get_next_block(block, read_rows, eof));
    
    // 2. 如果查询需要 $row_id 列，添加元数据列
    if (_need_row_id_column) {
        RETURN_IF_ERROR(_append_row_id_column(block));
    }
    
    // 3. 应用 Delete Filter（现有逻辑）
    if (!_delete_rows.empty()) {
        RETURN_IF_ERROR(_filter_block(block));
    }
    
    return Status::OK();
}

Status IcebergTableReader::_append_row_id_column(Block* block) {
    size_t num_rows = block->rows();
    
    // 创建 $row_id 结构体列：(file_path, row_position, partition_spec_id, partition_data)
    auto row_id_column = ColumnStruct::create();
    
    // 字段1: file_path - 当前数据文件路径（所有行相同）
    auto file_path_column = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i) {
        file_path_column->insert_data(_current_file_path.data(), _current_file_path.size());
    }
    row_id_column->insert_data(file_path_column);
    
    // 字段2: row_position - 当前行在文件中的位置
    auto row_pos_column = ColumnInt64::create();
    for (size_t i = 0; i < num_rows; ++i) {
        row_pos_column->insert_value(_current_row_position + i);
    }
    row_id_column->insert_data(row_pos_column);
    
    // 字段3: partition_spec_id - 分区规范ID
    auto spec_id_column = ColumnInt32::create();
    for (size_t i = 0; i < num_rows; ++i) {
        spec_id_column->insert_value(_partition_spec_id);
    }
    row_id_column->insert_data(spec_id_column);
    
    // 字段4: partition_data - 分区数据JSON
    auto partition_data_column = ColumnString::create();
    for (size_t i = 0; i < num_rows; ++i) {
        partition_data_column->insert_data(_partition_data_json.data(), 
                                          _partition_data_json.size());
    }
    row_id_column->insert_data(partition_data_column);
    
    // 将 $row_id 列添加到 Block
    block->insert(ColumnWithTypeAndName(
        std::move(row_id_column), 
        _row_id_type, 
        "$row_id"
    ));
    
    // 更新行位置计数器
    _current_row_position += num_rows;
    
    return Status::OK();
}
```

**需要添加的成员变量**:
```cpp
class IcebergTableReader {
private:
    bool _need_row_id_column = false;
    std::string _current_file_path;
    int64_t _current_row_position = 0;
    int32_t _partition_spec_id = 0;
    std::string _partition_data_json;
    TypeDescriptor _row_id_type; // STRUCT<file_path:STRING, ...>
};
```

### 2.4 DELETE 执行流程中收集 Position Delete 数据

**方案 A: 在 BE 端收集（推荐，类似 Trino）**

修改 `IcebergDeleteExecutor` 和 BE 端的处理：

1. **BE 端收集删除信息**

```cpp
// be/src/vec/exec/vscan_node.cpp 或类似位置
// 在扫描匹配 WHERE 条件的行时，收集 $row_id 信息

Status VIcebergScanNode::_process_delete_rows(Block* block) {
    // 1. 提取 $row_id 列（最后一列）
    const auto& row_id_column = block->get_by_position(block->columns() - 1);
    
    // 2. 按文件路径分组收集删除位置
    for (size_t i = 0; i < block->rows(); ++i) {
        auto row_id_struct = row_id_column.column->get_data_at(i);
        
        // 解析 $row_id 结构体
        std::string file_path = extract_file_path(row_id_struct);
        int64_t row_position = extract_row_position(row_id_struct);
        
        // 添加到删除映射
        _delete_info[file_path].push_back(row_position);
    }
    
    return Status::OK();
}

// 在扫描完成后，将删除信息发送给 FE
Status VIcebergScanNode::_finish_delete_collection() {
    TIcebergDeleteInfo delete_info;
    
    for (const auto& [file_path, positions] : _delete_info) {
        TFilePositionDeletes file_deletes;
        file_deletes.file_path = file_path;
        file_deletes.positions = positions;
        delete_info.file_deletes.push_back(file_deletes);
    }
    
    // 通过现有机制发送给 FE（类似 commit data）
    _send_delete_info_to_fe(delete_info);
    
    return Status::OK();
}
```

2. **FE 端接收并写入 Position Delete 文件**

```java
// IcebergDeleteExecutor.java
@Override
protected void doBeforeCommit() throws UserException {
    IcebergExternalTable dorisTable = (IcebergExternalTable) table;
    
    // 1. 从 BE 收集的删除信息
    List<TIcebergDeleteInfo> deleteInfos = collectDeleteInfoFromBackends();
    
    // 2. 按文件分组并写入 Position Delete 文件
    for (TIcebergDeleteInfo deleteInfo : deleteInfos) {
        for (TFilePositionDeletes fileDeletes : deleteInfo.getFileDeletes()) {
            // 创建 Position Delete Writer
            VIcebergDeleteFileWriter writer = VIcebergDeleteFileWriterFactory.create_writer(
                TFileContent.POSITION_DELETES,
                generateDeleteFilePath(fileDeletes.getFilePath()),
                TFileFormatType.FORMAT_PARQUET,
                Collections.emptyList() // Position Delete 不需要 equality field ids
            );
            
            // 写入删除记录（file_path, pos）
            Block deleteBlock = buildPositionDeleteBlock(
                fileDeletes.getFilePath(), 
                fileDeletes.getPositions()
            );
            writer.write(deleteBlock);
            
            // 关闭并获取提交数据
            TIcebergCommitData commitData = new TIcebergCommitData();
            writer.close(commitData);
            
            // 添加到事务
            transaction.addCommitData(commitData);
        }
    }
    
    // 3. 完成删除
    IcebergTransaction transaction = (IcebergTransaction) transactionManager.getTransaction(txnId);
    this.loadedRows = transaction.getUpdateCnt();
    transaction.finishDelete(nameMapping);
}

private Block buildPositionDeleteBlock(String filePath, List<Long> positions) {
    // 构建包含 (file_path, pos) 的 Block
    Block block = new Block();
    
    // file_path 列（所有行相同，可以用 RLE 优化）
    ColumnString filePathColumn = new ColumnString();
    for (int i = 0; i < positions.size(); i++) {
        filePathColumn.insertData(filePath);
    }
    block.insert(new ColumnWithTypeAndName(filePathColumn, TypeDescriptor.STRING, "file_path"));
    
    // pos 列
    ColumnInt64 posColumn = new ColumnInt64();
    for (Long pos : positions) {
        posColumn.insertValue(pos);
    }
    block.insert(new ColumnWithTypeAndName(posColumn, TypeDescriptor.BIGINT, "pos"));
    
    return block;
}
```

**方案 B: 在 FE 端收集（简化版本）**

直接在 FE 的 DELETE 计划中处理：

```java
// IcebergDeleteCommand.java
private LogicalPlan completeQueryPlan(...) {
    // 1. 扫描带 WHERE 条件的数据，投影 $row_id
    LogicalPlan scanWithRowId = injectRowIdColumn(logicalQuery, ctx);
    
    // 2. 将 $row_id 作为输出列
    return new LogicalProject<>(
        Lists.newArrayList(new UnboundSlot("$row_id")),
        scanWithRowId
    );
}

// 执行时，查询结果就是需要删除的行的 $row_id 列表
// 然后直接调用 VIcebergDeleteFileWriter 写入
```

## 3. 需要新增的 Thrift 定义

**文件**: `gensrc/thrift/DataSinks.thrift`

```thrift
// Position Delete 信息
struct TFilePositionDeletes {
    1: required string file_path
    2: required list<i64> positions
    3: optional i32 partition_spec_id
    4: optional string partition_data_json
}

// Delete 信息汇总
struct TIcebergDeleteInfo {
    1: required list<TFilePositionDeletes> file_deletes
}
```

## 4. 实现优先级

### Phase 1: 最小可行实现（MVP）
1. ✅ `IcebergMetadataColumn` - 定义元数据列
2. ✅ 在 `IcebergDeleteCommand` 中注入 $row_id 到查询计划
3. ✅ 在 BE `IcebergTableReader` 中生成 $row_id 列
4. ✅ 简单收集机制：FE 端接收 $row_id 结果集

### Phase 2: 完整实现
1. ✅ BE 端高效收集（按文件分组、使用 Bitmap）
2. ✅ Thrift 协议传递删除信息
3. ✅ 优化：RLE 编码、批量写入
4. ✅ 分区支持

### Phase 3: 性能优化
1. ✅ 使用 Roaring64Bitmap 压缩行位置
2. ✅ 并行写入多个 DeleteFile
3. ✅ 异步提交
4. ✅ DeleteFile 大小控制和分割

## 5. 测试用例

### 5.1 基础功能测试
```sql
-- 创建 v2 格式的 Iceberg 表
CREATE TABLE iceberg_test (
    id INT,
    name STRING,
    age INT
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 插入测试数据
INSERT INTO iceberg_test VALUES (1, 'Alice', 25), (2, 'Bob', 30), (3, 'Charlie', 35);

-- Position Delete 测试
DELETE FROM iceberg_test WHERE id = 2;

-- 验证：Bob 应该被删除
SELECT * FROM iceberg_test; -- 应该返回 Alice 和 Charlie

-- 验证：检查 DeleteFile 是否生成
SELECT * FROM iceberg_test.delete_files;
```

### 5.2 性能测试
```sql
-- 大规模删除测试
DELETE FROM iceberg_test WHERE age > 25; -- 删除大量行

-- 验证 Position Delete 文件大小和数量
-- 检查是否使用了压缩和批处理
```

## 6. 关键差异：Doris vs Trino

| 特性 | Trino | Doris (本设计) |
|------|-------|----------------|
| 操作入口 | MERGE 语句 | DELETE 语句 |
| $row_id 注入 | PageSourceProvider | IcebergTableReader |
| 收集机制 | IcebergMergeSink | VIcebergScanNode |
| 写入器 | PositionDeleteWriter | VIcebergDeleteFileWriter |
| 事务 API | RowDelta | IcebergTransaction |

## 7. 下一步行动

1. **立即实现**：
   - [ ] 创建 `IcebergMetadataColumn` 枚举类
   - [ ] 修改 `IcebergDeleteCommand.completeQueryPlan()` 注入 $row_id
   - [ ] 在 `IcebergTableReader` 中添加 `_append_row_id_column()` 方法

2. **短期实现**（1-2周）：
   - [ ] 实现 BE→FE 删除信息传递机制
   - [ ] 完善 `IcebergDeleteExecutor.doBeforeCommit()`
   - [ ] 添加基础测试用例

3. **中期优化**（2-4周）：
   - [ ] 使用 Roaring64Bitmap 压缩
   - [ ] 并行写入优化
   - [ ] 大规模测试

## 8. 参考资料

- Trino IcebergMergeSink: `trino/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergMergeSink.java`
- Trino PositionDeleteWriter: `trino/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/delete/PositionDeleteWriter.java`
- Trino MergeRowIdTransform: `trino/plugin/trino-iceberg/src/main/java/io/trino/plugin/iceberg/IcebergPageSourceProvider.java`
- Iceberg RowDelta API: Apache Iceberg 官方文档

---

**状态**: 设计完成，待实现  
**优先级**: P0（核心功能）  
**预计工作量**: 2-3周
