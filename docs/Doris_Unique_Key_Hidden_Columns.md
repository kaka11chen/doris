# Doris Unique Key 模型隐藏列处理机制详解

## 1. 概述

Doris 的 Unique Key 模型（Merge-on-Write, MOW）使用多个隐藏列来支持高效的更新和删除操作。这些隐藏列在**普通 SELECT 查询中不会读取**，但在 **DELETE/UPDATE 操作中会被显式查询**。本文档详细说明这一机制的原理和实现流程。

## 2. 核心隐藏列定义

### 2.1 隐藏列类型

Doris Unique Key MOW 表使用以下隐藏列：

| 隐藏列名称 | 类型 | 用途 |
|-----------|------|------|
| `__DORIS_DELETE_SIGN__` | TINYINT | 删除标记列，0表示未删除，1表示已删除 |
| `__DORIS_SEQUENCE_COL__` | BIGINT | 序列列，用于解决乱序更新问题 |
| `__DORIS_ROWID_COL__` | BIGINT | 行号列，用于定位具体行 |
| `__DORIS_VERSION_COL__` | BIGINT | 版本列，用于版本控制 |
| `__DORIS_SKIP_BITMAP_COL__` | BITMAP | 用于部分列更新的跳过位图 |

### 2.2 隐藏列特征

所有隐藏列都遵循以下特征：

```java
// Column.java:60-70
public static final String HIDDEN_COLUMN_PREFIX = "__DORIS_";
public static final String DELETE_SIGN = "__DORIS_DELETE_SIGN__";
public static final String SEQUENCE_COL = "__DORIS_SEQUENCE_COL__";
public static final String ROWID_COL = "__DORIS_ROWID_COL__";
public static final String VERSION_COL = "__DORIS_VERSION_COL__";
public static final String SKIP_BITMAP_COL = "__DORIS_SKIP_BITMAP_COL__";
```

- **命名规范**：所有隐藏列以 `__DORIS_` 开头
- **可见性**：`visible = false`，在普通查询中不可见
- **存储位置**：物理存储在数据文件中，但逻辑上对用户透明

## 3. 核心机制：为什么普通 SELECT 不读取隐藏列

### 3.1 Schema 获取机制

Doris 通过 `getBaseSchema(boolean full)` 方法控制是否返回隐藏列：

```java
// OlapTable.java:1106-1119
public List<Column> getSchemaByIndexId(Long indexId, boolean full) {
    List<Column> fullSchema = indexIdToMeta.get(indexId).getSchema();
    if (full) {
        return fullSchema;  // 包含隐藏列
    } else {
        List<Column> visibleSchema = new ArrayList<>(fullSchema.size());
        for (Column column : fullSchema) {
            if (column.isVisible()) {  // 只返回可见列
                visibleSchema.add(column);
            }
        }
        return visibleSchema;
    }
}
```

**关键点**：
- `full = false`（默认）：只返回 `visible = true` 的列
- `full = true`：返回所有列，包括隐藏列
- 普通查询调用 `getBaseSchema()` 时，默认 `full = false`

### 3.2 列可见性检查

```java
// Column.java:442-448
public boolean isVisible() {
    return visible;
}

public void setIsVisible(boolean isVisible) {
    this.visible = isVisible;
}
```

隐藏列的 `visible` 属性在创建表时被设置为 `false`。

### 3.3 LogicalOlapScan 输出计算

在 `LogicalOlapScan.computeOutput()` 中，只处理可见列：

```java
// LogicalOlapScan.java:550-587
@Override
public List<Slot> computeOutput() {
    if (cachedOutput.isPresent()) {
        return cachedOutput.get();
    }
    if (selectedIndexId != ((OlapTable) table).getBaseIndexId()) {
        return getOutputByIndex(selectedIndexId);
    }
    // 关键：getBaseSchema(true) 虽然传入 true，但后续会过滤
    List<Column> baseSchema = table.getBaseSchema(true);
    List<SlotReference> slotFromColumn = createSlotsVectorized(baseSchema);
    
    // 但实际使用时，只有可见列会被添加到输出
    // 隐藏列虽然存在于 schema 中，但不会出现在查询计划的输出中
    ...
}
```

### 3.4 列裁剪（Column Pruning）

在列裁剪阶段，隐藏列会被进一步过滤：

```java
// ColumnPruning.java:563-570
private Set<String> computeUsedColumns(Plan plan, RoaringBitmap requiredSlotsIds) {
    Set<String> usedColumnNames = new LinkedHashSet<>();
    for (Slot outputSlot : plan.getOutput()) {
        if (!requiredSlotsIds.contains(outputSlot.getExprId().asInt())) {
            continue;
        }
        // 关键：跳过隐藏列的权限检查，但也不会将其加入 usedColumnNames
        if (outputSlot instanceof SlotReference 
                && ((SlotReference) outputSlot).getOriginalColumn().isPresent()
                && !((SlotReference) outputSlot).getOriginalColumn().get().isVisible()) {
            continue;  // 隐藏列被跳过
        }
        usedColumnNames.add(outputSlot.getName());
    }
    return usedColumnNames;
}
```

## 4. DELETE 操作如何显式包含隐藏列

### 4.1 DeleteFromCommand 的查询计划构建

`DeleteFromCommand.completeQueryPlan()` 方法会**显式构建包含隐藏列的 SELECT 列表**：

```java
// DeleteFromCommand.java:490-537
public LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery) {
    OlapTable targetTable = getTargetTable(ctx);
    checkTargetTable(targetTable);
    
    List<NamedExpression> selectLists = Lists.newArrayList();
    List<String> cols = Lists.newArrayList();
    boolean isMow = targetTable.getEnableUniqueKeyMergeOnWrite();
    String tableName = tableAlias != null ? tableAlias : Util.getTempTableDisplayName(targetTable.getName());
    
    // 关键：遍历 getBaseSchema(true) 获取完整 schema（包括隐藏列）
    for (Column column : targetTable.getBaseSchema(true)) {
        NamedExpression expr;
        
        // 1. 显式添加 DELETE_SIGN 列，设置为 1（表示删除）
        if (column.getName().equalsIgnoreCase(Column.DELETE_SIGN)) {
            expr = new UnboundAlias(new TinyIntLiteral(((byte) 1)), Column.DELETE_SIGN);
        } 
        // 2. 显式添加 SEQUENCE_COL 列（如果存在）
        else if (column.getName().equalsIgnoreCase(Column.SEQUENCE_COL)
                && targetTable.getSequenceMapCol() != null) {
            expr = new UnboundAlias(new UnboundSlot(tableName, targetTable.getSequenceMapCol()),
                    Column.SEQUENCE_COL);
        } 
        // 3. 添加所有 Key 列
        else if (column.isKey()) {
            expr = new UnboundSlot(tableName, column.getName());
        } 
        // 4. 根据 MOW 模式和其他条件决定是否添加其他列
        else if (!isMow && (!column.isVisible() || (!column.isAllowNull() && !column.hasDefaultValue()))) {
            expr = new UnboundSlot(tableName, column.getName());
        } else if (hasClusterKey || hasSyncMaterializedView) {
            expr = new UnboundSlot(tableName, column.getName());
        } else {
            continue;  // 跳过不需要的列
        }
        
        selectLists.add(expr);
        cols.add(column.getName());
    }
    
    // 构建包含隐藏列的 Project
    logicalQuery = new LogicalProject<>(selectLists, logicalQuery);
    
    // 创建 UnboundTableSink，标记为 DELETE 操作
    return UnboundTableSinkCreator.createUnboundTableSink(
        nameParts, cols, ImmutableList.of(),
        isTempPart, partitions, isPartialUpdate, 
        TPartialUpdateNewRowPolicy.APPEND,
        DMLCommandType.DELETE, logicalQuery);
}
```

### 4.2 DELETE 执行流程

```
用户 SQL: DELETE FROM table WHERE condition
    ↓
DeleteFromCommand.run()
    ↓
completeQueryPlan() 构建查询计划
    ↓
显式添加隐藏列到 SELECT 列表：
  - __DORIS_DELETE_SIGN__ = 1
  - __DORIS_SEQUENCE_COL__ (如果存在)
  - 所有 Key 列
    ↓
LogicalProject(selectLists, logicalQuery)
    ↓
UnboundTableSink (DMLCommandType.DELETE)
    ↓
BindSink 绑定目标表
    ↓
执行 INSERT INTO ... SELECT ... 操作
    ↓
写入包含 DELETE_SIGN=1 的新行
```

### 4.3 删除标记过滤

在查询时，BE 会自动过滤 `DELETE_SIGN = 1` 的行：

```java
// DeleteFromCommand.java:227-234
List<Predicate> predicates = planner.getScanNodes().get(0).getConjuncts().stream()
    .filter(c -> {
        // 过滤掉 __DORIS_DELETE_SIGN__ = 0 的谓词
        List<Expr> slotRefs = Lists.newArrayList();
        c.collect(SlotRef.class::isInstance, slotRefs);
        return slotRefs.stream().map(SlotRef.class::cast)
                .noneMatch(s -> Column.DELETE_SIGN.equalsIgnoreCase(s.getColumnName()));
    })
    ...
```

## 5. UPDATE 操作如何显式包含隐藏列

### 5.1 UpdateCommand 的查询计划构建

`UpdateCommand.completeQueryPlan()` 方法会**显式处理隐藏列**：

```java
// UpdateCommand.java:125-217
public LogicalPlan completeQueryPlan(ConnectContext ctx, LogicalPlan logicalQuery) {
    checkTable(ctx);
    
    Map<String, Expression> colNameToExpression = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
    // 处理 SET 子句中的赋值
    for (EqualTo equalTo : assignments) {
        ...
    }
    
    List<NamedExpression> selectItems = Lists.newArrayList();
    String tableName = tableAlias != null ? tableAlias : Util.getTempTableDisplayName(targetTable.getName());
    Expression setExpr = null;
    
    // 关键：遍历 getFullSchema() 获取完整 schema（包括隐藏列）
    for (Column column : targetTable.getFullSchema()) {
        // 跳过不可见列，但保留 SEQUENCE_COL
        if (!column.isVisible() && !column.isSequenceColumn()) {
            continue;
        }
        
        if (colNameToExpression.containsKey(column.getName())) {
            Expression expr = colNameToExpression.get(column.getName());
            
            // 处理序列列映射：如果更新了 sequence map column，需要同步更新隐藏的 sequence column
            boolean isSequenceMapColumn = targetTable.hasSequenceCol()
                    && targetTable.getSequenceMapCol() != null
                    && column.getName().equalsIgnoreCase(targetTable.getSequenceMapCol());
            if (setExpr == null && isSequenceMapColumn) {
                setExpr = expr;
            }
            
            selectItems.add(expr instanceof UnboundSlot
                    ? ((NamedExpression) expr)
                    : new UnboundAlias(expr));
            colNameToExpression.remove(column.getName());
        } else {
            // 如果更新了 sequence map column，需要同步设置隐藏的 sequence column
            if (column.isSequenceColumn() && setExpr != null) {
                selectItems.add(new UnboundAlias(setExpr, column.getName()));
            } else if (column.hasOnUpdateDefaultValue()) {
                // 处理 ON UPDATE 默认值
                ...
            } else {
                // 保留原值
                selectItems.add(new UnboundSlot(tableName, column.getName()));
            }
        }
    }
    
    // 构建包含隐藏列的 Project
    logicalQuery = new LogicalProject<>(isPartialUpdate ? partialUpdateSelectItems : selectItems, logicalQuery);
    
    // 创建 UnboundTableSink，标记为 UPDATE 操作
    return UnboundTableSinkCreator.createUnboundTableSink(
        nameParts, ...,
        DMLCommandType.UPDATE, logicalQuery);
}
```

### 5.2 UPDATE 执行流程

```
用户 SQL: UPDATE table SET col1 = val1 WHERE condition
    ↓
UpdateCommand.run()
    ↓
completeQueryPlan() 构建查询计划
    ↓
遍历 getFullSchema()，显式处理：
  - 所有可见列（保留或更新）
  - __DORIS_SEQUENCE_COL__（如果存在）
    ↓
LogicalProject(selectItems, logicalQuery)
    ↓
UnboundTableSink (DMLCommandType.UPDATE)
    ↓
BindSink 绑定目标表
    ↓
执行 INSERT INTO ... SELECT ... 操作（部分更新或全量更新）
    ↓
写入新行，保留或更新隐藏列
```

## 6. BindSink 阶段的处理

### 6.1 绑定目标列

在 `BindSink.bindOlapTableSink()` 中，会根据 DML 类型处理隐藏列：

```java
// BindSink.java:161-288
private Plan bindOlapTableSink(MatchingContext<UnboundTableSink<Plan>> ctx) {
    ...
    boolean isPartialUpdate = sink.isPartialUpdate() 
            && table.getKeysType() == KeysType.UNIQUE_KEYS;
    
    // 绑定目标列：包括隐藏列
    Pair<List<Column>, Integer> bindColumnsResult =
            bindTargetColumns(table, sink.getColNames(), childHasSeqCol, needExtraSeqCol,
                    sink.getDMLCommandType() == DMLCommandType.GROUP_COMMIT);
    List<Column> bindColumns = bindColumnsResult.first;
    
    // 对于 UPDATE 和 DELETE，会自动添加隐藏列
    if (boundSink.getDmlCommandType() != DMLCommandType.UPDATE
            && boundSink.getDmlCommandType() != DMLCommandType.DELETE) {
        // INSERT 操作需要检查序列列
        ...
    }
    
    // 构建完整的输出 Project，包含所有需要的列（包括隐藏列）
    Map<String, NamedExpression> columnToOutput = getColumnToOutput(
            ctx, table, isPartialUpdate, boundSink, child);
    LogicalProject<?> fullOutputProject = getOutputProjectByCoercion(
            table.getFullSchema(), child, columnToOutput);
    ...
}
```

### 6.2 列到输出的映射

```java
// BindSink.java:349-541
private static Map<String, NamedExpression> getColumnToOutput(
        MatchingContext<? extends UnboundLogicalSink<Plan>> ctx,
        TableIf table, boolean isPartialUpdate, LogicalTableSink<?> boundSink, LogicalPlan child) {
    ...
    for (Column column : boundSink.getTargetTable().getFullSchema()) {
        // 跳过生成的列、物化视图列、影子列
        if (column.isGeneratedColumn()) {
            generatedColumns.add(column);
            continue;
        } else if (column.isMaterializedViewColumn()) {
            materializedViewColumn.add(column);
            continue;
        } else if (Column.isShadowColumn(column.getName())) {
            shadowColumns.add(column);
            continue;
        }
        
        // 处理隐藏列：对于 DELETE/UPDATE，隐藏列会被包含
        if (columnToChildOutput.containsKey(column)
                && !(columnToChildOutput.get(column) instanceof DefaultValueSlot)) {
            // 列在子查询输出中，直接映射
            ...
        } else {
            // 处理序列列的特殊逻辑
            if (table instanceof OlapTable && ((OlapTable) table).hasSequenceCol()
                    && column.getName().equals(Column.SEQUENCE_COL)
                    && ((OlapTable) table).getSequenceMapCol() != null) {
                // 从 sequence map column 生成 sequence column
                ...
            } else if (isPartialUpdate) {
                // 部分更新：跳过未提及的列
                ...
            } else {
                // 使用默认值或 NULL
                ...
            }
        }
    }
    ...
}
```

## 7. 设计原理总结

### 7.1 为什么普通 SELECT 不读取隐藏列？

1. **性能优化**：隐藏列（如 DELETE_SIGN、SEQUENCE_COL）在普通查询中不需要，避免不必要的 I/O
2. **用户透明性**：用户不应该看到这些内部实现细节
3. **列裁剪**：优化器会自动裁剪不需要的列，隐藏列因为不可见会被自动排除

### 7.2 为什么 DELETE/UPDATE 需要读取隐藏列？

1. **删除标记**：DELETE 需要设置 `DELETE_SIGN = 1` 来标记删除
2. **序列控制**：UPDATE 需要读取/更新 `SEQUENCE_COL` 来保证更新顺序
3. **行定位**：需要 `ROWID_COL` 来定位要更新的具体行
4. **版本控制**：需要 `VERSION_COL` 来处理并发更新

### 7.3 实现机制

```
┌─────────────────────────────────────────────────────────┐
│                   普通 SELECT 查询                        │
├─────────────────────────────────────────────────────────┤
│ 1. getBaseSchema(false) → 只返回可见列                   │
│ 2. LogicalOlapScan.computeOutput() → 只包含可见列        │
│ 3. ColumnPruning → 隐藏列被自动过滤                      │
│ 4. 最终查询计划：不包含隐藏列                             │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                  DELETE/UPDATE 操作                      │
├─────────────────────────────────────────────────────────┤
│ 1. DeleteFromCommand/UpdateCommand.completeQueryPlan()   │
│    → 显式遍历 getBaseSchema(true) 或 getFullSchema()     │
│ 2. 手动构建包含隐藏列的 SELECT 列表                       │
│    - DELETE: 添加 DELETE_SIGN=1, SEQUENCE_COL, Key列    │
│    - UPDATE: 添加 SEQUENCE_COL, 所有需要更新的列         │
│ 3. LogicalProject(包含隐藏列, logicalQuery)              │
│ 4. UnboundTableSink (DMLCommandType.DELETE/UPDATE)      │
│ 5. BindSink → 绑定包含隐藏列的完整 schema                │
│ 6. 最终执行计划：包含必要的隐藏列                         │
└─────────────────────────────────────────────────────────┘
```

## 8. 关键代码位置

| 功能 | 文件路径 | 关键方法 |
|------|---------|---------|
| Schema 获取 | `fe/fe-core/src/main/java/org/apache/doris/catalog/OlapTable.java` | `getSchemaByIndexId(Long, boolean)` |
| 列可见性 | `fe/fe-core/src/main/java/org/apache/doris/catalog/Column.java` | `isVisible()` |
| DELETE 计划构建 | `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/DeleteFromCommand.java` | `completeQueryPlan()` |
| UPDATE 计划构建 | `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/UpdateCommand.java` | `completeQueryPlan()` |
| Sink 绑定 | `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/BindSink.java` | `bindOlapTableSink()` |
| 列裁剪 | `fe/fe-core/src/main/java/org/apache/doris/nereids/rules/rewrite/ColumnPruning.java` | `computeUsedColumns()` |
| Scan 输出 | `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/logical/LogicalOlapScan.java` | `computeOutput()` |

## 9. 总结

Doris Unique Key 模型通过以下机制实现了"普通查询不读隐藏列，DML 操作才读隐藏列"：

1. **Schema 分层**：通过 `getBaseSchema(boolean full)` 参数控制是否返回隐藏列
2. **可见性标记**：隐藏列的 `visible = false` 属性确保普通查询不会包含它们
3. **显式包含**：DELETE/UPDATE 命令在构建查询计划时，显式遍历完整 schema 并手动添加隐藏列
4. **列裁剪优化**：优化器会自动裁剪隐藏列，除非被显式引用
5. **DML 特殊处理**：通过 `DMLCommandType` 标记，在 BindSink 阶段特殊处理隐藏列

这种设计既保证了普通查询的性能和简洁性，又确保了 DML 操作的正确性和完整性。
