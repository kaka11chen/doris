# Parquet 普通列查询性能问题分析

## 问题描述
Commit `0375981938dc435e25c2142fa458f264c432afce` 提交后，普通列查询性能变慢。

## 根本原因分析

### 1. 重复调用 `parse_page_header()`

在 `vparquet_column_reader.cpp` 的 `read_column_data()` 函数中，对于普通列（`IN_COLLECTION == false` 且 `OFFSET_INDEX == false`），存在重复调用 `parse_page_header()` 的问题：

**代码位置：** `be/src/vec/exec/format/parquet/vparquet_column_reader.cpp:578-617`

```cpp
// 第580行：第一次调用 parse_page_header()
int64_t right_row = 0;
if constexpr (OFFSET_INDEX == false) {
    RETURN_IF_ERROR(_chunk_reader->parse_page_header());  // ← 第一次调用
    right_row = _chunk_reader->page_end_row();
} else {
    right_row = _chunk_reader->page_end_row();
}

do {
    RowRanges read_ranges;
    _generate_read_ranges(RowRange {_current_row_index, right_row}, &read_ranges);
    if (read_ranges.count() == 0) {
        _current_row_index = right_row;
    } else {
        // ...
        // 第616行：第二次调用 parse_page_header()
        RETURN_IF_ERROR(_chunk_reader->parse_page_header());  // ← 第二次调用（多余）
        RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
        // ...
    }
} while (false);
```

**问题：**
- 虽然 `parse_page_header()` 内部有状态检查（`_state == HEADER_PARSED || _state == DATA_LOADED`），但重复调用仍然带来：
  1. **函数调用开销**：每次调用都需要进入函数、检查状态
  2. **Page Cache 查找开销**：`parse_page_header()` 会触发 page cache 查找（见 `vparquet_page_reader.cpp:134-213`）
  3. **代码可读性差**：不必要的重复调用

### 2. Page Cache 查找次数增加

`parse_page_header()` 在 `vparquet_page_reader.cpp` 中会进行 page cache 查找：

```cpp
// vparquet_page_reader.cpp:134-213
if (_ctx.enable_parquet_file_page_cache && !config::disable_storage_page_cache &&
    StoragePageCache::instance() != nullptr) {
    PageCacheHandle handle;
    StoragePageCache::CacheKey key(...);
    if (StoragePageCache::instance()->lookup(key, &handle, segment_v2::DATA_PAGE)) {
        // Cache hit - parse from cached data
        // ...
    } else {
        _page_statistics.page_cache_missing_counter += 1;
        // Cache miss - read from file
    }
}
```

**影响：**
- 每次调用 `parse_page_header()` 都会触发 cache 查找
- 即使第二次调用有状态检查，但第一次调用已经触发了 cache 查找
- 如果 cache miss，会增加 I/O 操作

### 3. 即使跳过整个 Page 也需要解析 Header

当 `read_ranges.count() == 0`（整个 page 被跳过）时，代码仍然在第580行调用 `parse_page_header()` 来获取 `right_row`。这意味着：
- 即使不需要读取任何数据，也需要解析 page header
- 可能触发不必要的 I/O 或 cache 查找

## 性能影响

1. **Page Header 解析次数增加**：每个 page 至少解析一次 header（即使被跳过）
2. **Page Cache 查找次数增加**：重复调用导致额外的 cache 查找
3. **函数调用开销**：不必要的重复调用增加 CPU 开销

## 修复建议

### 方案1：移除重复的 `parse_page_header()` 调用（推荐）

移除第616行的 `parse_page_header()` 调用，因为：
- 第580行已经解析过 header
- `load_page_data_idempotent()` 会检查状态，如果 header 未解析会调用 `load_page_data()`，而 `load_page_data()` 内部会调用 `parse_page_header()`（见 `vparquet_column_chunk_reader.cpp:164`）

**修改代码：**

```cpp
// 修改前（第616行）
RETURN_IF_ERROR(_chunk_reader->parse_page_header());
RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());

// 修改后
RETURN_IF_ERROR(_chunk_reader->load_page_data_idempotent());
```

**验证：**
- `load_page_data()` 内部会检查 `_state != HEADER_PARSED`，如果未解析会先调用 `parse_page_header()`
- 由于第580行已经解析过，`load_page_data()` 会直接加载数据，不会重复解析

### 方案2：延迟解析 Header（更激进，需要更多测试）

只在真正需要读取数据时才解析 header：

```cpp
int64_t right_row = 0;
if constexpr (OFFSET_INDEX == false) {
    // 不在这里解析 header，延迟到真正需要时
    // 可以通过其他方式估算 right_row，或者延迟解析
    // 需要进一步分析可行性
}
```

**注意：** 这个方案需要确保 `right_row` 的计算不依赖于 header 解析。

## 测试建议

1. **性能测试**：对比修复前后的查询性能
2. **功能测试**：确保所有查询场景正常工作
3. **Page Cache 统计**：监控 `page_cache_hit_counter` 和 `page_cache_missing_counter` 的变化

## 相关文件

- `be/src/vec/exec/format/parquet/vparquet_column_reader.cpp:578-617`
- `be/src/vec/exec/format/parquet/vparquet_column_chunk_reader.cpp:123-143`
- `be/src/vec/exec/format/parquet/vparquet_page_reader.cpp:107-260`
