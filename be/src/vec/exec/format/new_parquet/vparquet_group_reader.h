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
#pragma once
#include <common/status.h>
#include <stddef.h>
#include <stdint.h>

#include <limits>
#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "io/fs/file_reader_writer_fwd.h"
#include "olap/id_manager.h"
#include "olap/utils.h"
#include "vec/columns/column.h"
#include "vec/exec/format/column_type_convert.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exec/format/table/table_format_reader.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vparquet_column_reader.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class SlotDescriptor;
class TupleDescriptor;

namespace io {
struct IOContext;
} // namespace io
namespace vectorized {
class Block;
class FieldDescriptor;
} // namespace vectorized
} // namespace doris
namespace tparquet {
class ColumnMetaData;
class OffsetIndex;
class RowGroup;
} // namespace tparquet

namespace doris::vectorized::new_parquet {
#include "common/compile_check_begin.h"
// TODO: we need to determine it by test.

class RowGroupReader : public ProfileCollector {
public:
    std::shared_ptr<TableSchemaChangeHelper::Node> _table_info_node_ptr;

    struct RowGroupIndex {
        int32_t row_group_id;
        int64_t first_row;
        int64_t last_row;
        RowGroupIndex(int32_t id, int64_t first, int64_t last)
                : row_group_id(id), first_row(first), last_row(last) {}
    };

    RowGroupReader(io::FileReaderSPtr file_reader,
                   const std::vector<std::string>& read_columns_names, const int32_t row_group_id,
                   const tparquet::RowGroup& row_group, cctz::time_zone* ctz, io::IOContext* io_ctx,
                   RuntimeState* state,
                   converter::ColumnTypeConverterFactory* converter_factory = nullptr);

    ~RowGroupReader();
    Status init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                std::unordered_map<int, tparquet::OffsetIndex>& col_offsets);
    Status next_batch(Block* block, size_t batch_size, size_t* read_rows, bool* batch_eof);

    ParquetColumnReader::Statistics statistics();
    void set_remaining_rows(int64_t rows) { _remaining_rows = rows; }
    int64_t get_remaining_rows() { return _remaining_rows; }

    // void set_row_id_column_iterator(
    //         const std::pair<std::shared_ptr<RowIdColumnIteratorV2>, int>& iterator_pair) {
    //     _row_id_column_iterator_pair = iterator_pair;
    // }

    void set_current_row_group_idx(RowGroupIndex row_group_idx) {
        _current_row_group_idx = row_group_idx;
    }

protected:
    void _collect_profile_before_close() override {
        if (_file_reader != nullptr) {
            _file_reader->collect_profile_before_close();
        }
    }

private:
    void _merge_read_ranges(std::vector<RowRange>& row_ranges);
    Status _read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof);
    Status _read_column_data(Block* block, const std::vector<std::string>& table_column_names,
                             size_t batch_size, size_t* read_rows, bool* batch_eof,
                             FilterMap& filter_map);
    Status _rebuild_filter_map(FilterMap& filter_map, std::unique_ptr<uint8_t[]>& filter_map_data,
                               size_t pre_read_rows) const;

    Status _get_current_batch_row_id(size_t read_rows);

    io::FileReaderSPtr _file_reader;
    std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>>
            _column_readers; // table_column_name
    std::vector<std::string> _table_column_names;

    const int32_t _row_group_id;
    const tparquet::RowGroup& _row_group_meta;
    int64_t _remaining_rows;
    cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    // merge the row ranges generated from page index and position delete.
    std::vector<RowRange> _read_ranges;

    // If continuous batches are skipped, we can cache them to skip a whole page
    int64_t _total_read_rows = 0;
    // std::pair<col_name, slot_id>
    RuntimeState* _state = nullptr;
    std::shared_ptr<ObjectPool> _obj_pool;
    bool _is_row_group_filtered = false;
    converter::ColumnTypeConverterFactory* _converter_factory = nullptr;

    RowGroupIndex _current_row_group_idx {0, 0, 0};
    // std::pair<std::shared_ptr<RowIdColumnIteratorV2>, int> _row_id_column_iterator_pair = {nullptr,
    //                                                                                        -1};
    std::vector<rowid_t> _current_batch_row_ids;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized::new_parquet
