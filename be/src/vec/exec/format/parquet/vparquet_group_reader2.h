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
#include "vec/columns/column.h"
#include "vec/exec/format/parquet/parquet_common.h"
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

namespace doris::vectorized {
#include "common/compile_check_begin.h"
// TODO: we need to determine it by test.
//static constexpr uint32_t MAX_DICT_CODE_PREDICATE_TO_REWRITE = std::numeric_limits<uint32_t>::max();

class RowGroupReader2 : public ProfileCollector {
public:
    static const std::vector<int64_t> NO_DELETE;

    struct RowGroupIndex {
        int32_t row_group_id;
        int64_t first_row;
        int64_t last_row;
        RowGroupIndex(int32_t id, int64_t first, int64_t last)
                : row_group_id(id), first_row(first), last_row(last) {}
    };

    RowGroupReader2(io::FileReaderSPtr file_reader, const std::vector<std::string>& read_columns,
                   const int32_t row_group_id, const tparquet::RowGroup& row_group,
                   cctz::time_zone* ctz, io::IOContext* io_ctx, RuntimeState* state);

    ~RowGroupReader2();
    Status init(const FieldDescriptor& schema, std::vector<RowRange>& row_ranges,
                std::unordered_map<int, tparquet::OffsetIndex>& col_offsets);
    Status next_batch(Block* block, size_t batch_size, size_t* read_rows, bool* batch_eof);

    Status _read_column_data(Block* block, const std::string& column_name,
                             size_t batch_size, size_t* read_rows, bool* eof,
                             FilterMap& filter_map);

    ParquetColumnReader::Statistics statistics();
    void set_remaining_rows(int64_t rows) { _remaining_rows = rows; }
    int64_t get_remaining_rows() { return _remaining_rows; }


    Status read_column_data(ColumnWithTypeAndName* column_with_type_and_name,
                            size_t batch_size, size_t* read_rows, bool* eof,
                            FilterMap& filter_map);

protected:
    void _collect_profile_before_close() override {
        if (_file_reader != nullptr) {
            _file_reader->collect_profile_before_close();
        }
    }

private:
    void _merge_read_ranges(std::vector<RowRange>& row_ranges);
    bool _can_filter_by_dict(const tparquet::ColumnMetaData& column_metadata);
    bool _is_dictionary_encoded(const tparquet::ColumnMetaData& column_metadata);
    Status _read_empty_batch(size_t batch_size, size_t* read_rows, bool* batch_eof);
    Status _read_columns_data(Block* block, const std::vector<std::string>& columns,
                             size_t batch_size, size_t* read_rows, bool* batch_eof,
                             FilterMap& filter_map);


    io::FileReaderSPtr _file_reader;
    std::unordered_map<std::string, std::unique_ptr<ParquetColumnReader>> _column_readers;
    const std::vector<std::string>& _read_columns;
    const int32_t _row_group_id;
    const tparquet::RowGroup& _row_group_meta;
    int64_t _remaining_rows;
    cctz::time_zone* _ctz = nullptr;
    io::IOContext* _io_ctx = nullptr;
    // merge the row ranges generated from page index and position delete.
    std::vector<RowRange> _read_ranges;

    int64_t _total_read_rows = 0;
    const TupleDescriptor* _tuple_descriptor = nullptr;
    const RowDescriptor* _row_descriptor = nullptr;
//    const std::unordered_map<std::string, int>* _col_name_to_slot_id = nullptr;
//    VExprContextSPtrs _not_single_slot_filter_conjuncts;
//    const std::unordered_map<int, VExprContextSPtrs>* _slot_id_to_filter_conjuncts = nullptr;
//    VExprContextSPtrs _dict_filter_conjuncts;
//    VExprContextSPtrs _filter_conjuncts;
    // std::pair<col_name, slot_id>
//    std::vector<std::pair<std::string, int>> _dict_filter_cols;
    std::vector<std::string> _dict_col_names;
    RuntimeState* _state = nullptr;
    std::shared_ptr<ObjectPool> _obj_pool;
    bool _is_row_group_filtered = false;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized
