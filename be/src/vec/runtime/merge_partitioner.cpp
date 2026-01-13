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

#include "vec/runtime/merge_partitioner.h"

#include <algorithm>

#include "common/cast_set.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "util/string_util.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/table/iceberg/partition_spec.h"
#include "vec/sink/writer/iceberg/partition_transformers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

namespace {
constexpr int8_t kInsertOperation = 1;
constexpr int8_t kDeleteOperation = 2;
constexpr int8_t kUpdateOperation = 3;
constexpr int8_t kUpdateInsertOperation = 4;
constexpr int8_t kUpdateDeleteOperation = 5;

int64_t scale_threshold_by_task(int64_t value, int task_num) {
    if (task_num <= 0) {
        return value;
    }
    int64_t scaled = value / task_num;
    return scaled == 0 ? value : scaled;
}
} // namespace

MergePartitioner::MergePartitioner(size_t partition_count, const TMergePartitionInfo& merge_info,
                                   bool use_new_shuffle_hash_method)
        : PartitionerBase(static_cast<HashValType>(partition_count)),
          _merge_info(merge_info),
          _use_new_shuffle_hash_method(use_new_shuffle_hash_method),
          _insert_random(merge_info.insert_random) {}

Status MergePartitioner::init(const std::vector<TExpr>& /*texprs*/) {
    VExprContextSPtr op_ctx;
    RETURN_IF_ERROR(VExpr::create_expr_tree(_merge_info.operation_expr, op_ctx));
    _operation_expr_ctxs.emplace_back(std::move(op_ctx));

    if (_merge_info.__isset.insert_partition_exprs &&
        !_merge_info.insert_partition_exprs.empty()) {
        RETURN_IF_ERROR(
                VExpr::create_expr_trees(_merge_info.insert_partition_exprs,
                                         _insert_partition_expr_ctxs));
    }

    if (_merge_info.__isset.insert_partition_fields &&
        !_merge_info.insert_partition_fields.empty()) {
        _insert_partition_fields.reserve(_merge_info.insert_partition_fields.size());
        for (const auto& field : _merge_info.insert_partition_fields) {
            VExprContextSPtr ctx;
            RETURN_IF_ERROR(VExpr::create_expr_tree(field.source_expr, ctx));
            InsertPartitionField insert_field;
            insert_field.transform = field.transform;
            insert_field.expr_ctx = std::move(ctx);
            insert_field.source_id = field.__isset.source_id ? field.source_id : 0;
            insert_field.name = field.__isset.name ? field.name : "";
            _insert_partition_fields.emplace_back(std::move(insert_field));
        }
    }

    if (_merge_info.__isset.delete_partition_exprs &&
        !_merge_info.delete_partition_exprs.empty()) {
        RETURN_IF_ERROR(
                VExpr::create_expr_trees(_merge_info.delete_partition_exprs,
                                         _delete_partition_expr_ctxs));
    }
    return Status::OK();
}

Status MergePartitioner::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(VExpr::prepare(_operation_expr_ctxs, state, row_desc));
    RETURN_IF_ERROR(VExpr::prepare(_insert_partition_expr_ctxs, state, row_desc));
    if (!_insert_partition_fields.empty()) {
        VExprContextSPtrs field_ctxs;
        field_ctxs.reserve(_insert_partition_fields.size());
        for (const auto& field : _insert_partition_fields) {
            field_ctxs.emplace_back(field.expr_ctx);
        }
        RETURN_IF_ERROR(VExpr::prepare(field_ctxs, state, row_desc));
    }
    RETURN_IF_ERROR(VExpr::prepare(_delete_partition_expr_ctxs, state, row_desc));
    return Status::OK();
}

Status MergePartitioner::open(RuntimeState* state) {
    RETURN_IF_ERROR(VExpr::open(_operation_expr_ctxs, state));
    RETURN_IF_ERROR(VExpr::open(_insert_partition_expr_ctxs, state));
    if (!_insert_partition_fields.empty()) {
        VExprContextSPtrs field_ctxs;
        field_ctxs.reserve(_insert_partition_fields.size());
        for (const auto& field : _insert_partition_fields) {
            field_ctxs.emplace_back(field.expr_ctx);
        }
        RETURN_IF_ERROR(VExpr::open(field_ctxs, state));
        for (auto& field : _insert_partition_fields) {
            try {
                doris::iceberg::PartitionField partition_field(
                        field.source_id, 0, field.name, field.transform);
                field.transformer = PartitionColumnTransforms::create(
                        partition_field, field.expr_ctx->root()->data_type());
            } catch (const doris::Exception& e) {
                LOG(WARNING) << "Merge partitioning fallback to RR: " << e.what();
                _insert_random = true;
                _insert_partition_fields.clear();
                break;
            }
        }
    }
    RETURN_IF_ERROR(VExpr::open(_delete_partition_expr_ctxs, state));
    _init_insert_scaling(state);
    return Status::OK();
}

Status MergePartitioner::close(RuntimeState* /*state*/) {
    return Status::OK();
}

Status MergePartitioner::do_partitioning(RuntimeState* /*state*/, Block* block) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        _channel_ids.clear();
        return Status::OK();
    }

    const size_t column_to_keep = block->columns();
    if (_operation_expr_ctxs.empty()) {
        return Status::InternalError("Merge partitioning missing operation expression");
    }

    int op_idx = -1;
    RETURN_IF_ERROR(_operation_expr_ctxs[0]->execute(block, &op_idx));
    if (op_idx < 0 || op_idx >= block->columns()) {
        return Status::InternalError("Merge partitioning missing operation column");
    }
    if (op_idx >= cast_set<int>(column_to_keep)) {
        return Status::InternalError("Merge partitioning requires operation column in input block");
    }

    const auto& op_column = block->get_by_position(op_idx).column;
    const auto* op_data = remove_nullable(op_column).get();
    std::vector<int8_t> ops(rows);
    bool has_insert = false;
    bool has_delete = false;
    bool has_update = false;
    for (size_t i = 0; i < rows; ++i) {
        int8_t op = static_cast<int8_t>(op_data->get_int(i));
        ops[i] = op;
        if (_is_insert_op(op)) {
            has_insert = true;
        }
        if (_is_delete_op(op)) {
            has_delete = true;
        }
        if (op == kUpdateOperation) {
            has_update = true;
        }
    }

    if (has_insert && !_insert_random && _insert_partition_expr_ctxs.empty()) {
        if (_insert_partition_fields.empty()) {
            return Status::InternalError("Merge partitioning insert exprs are empty");
        }
    }
    if (has_delete && _delete_partition_expr_ctxs.empty()) {
        return Status::InternalError("Merge partitioning delete exprs are empty");
    }

    std::vector<uint32_t> insert_hashes;
    std::vector<uint32_t> delete_hashes;
    if (has_insert && !_insert_random) {
        RETURN_IF_ERROR(_compute_insert_hashes(block, insert_hashes));
    }
    if (has_delete) {
        RETURN_IF_ERROR(
                _compute_hashes(block, _delete_partition_expr_ctxs, delete_hashes, true));
        _apply_partition_ids(delete_hashes, _partition_count);
    }
    if (has_insert) {
        if (_insert_random) {
            if (_non_partition_scaling_threshold > 0) {
                _insert_data_processed += static_cast<int64_t>(block->bytes());
                if (_insert_writer_count < static_cast<int>(_partition_count)
                    && _insert_data_processed >=
                            _insert_writer_count * _non_partition_scaling_threshold) {
                    _insert_writer_count++;
                }
            } else {
                _insert_writer_count = static_cast<int>(_partition_count);
            }
        } else {
            if (_enable_insert_rebalance) {
                _apply_partition_ids(insert_hashes, _insert_partition_count);
                _apply_insert_rebalance(ops, insert_hashes, block->bytes());
            } else {
                _apply_partition_ids(insert_hashes, _partition_count);
            }
        }
    }

    Block::erase_useless_column(block, column_to_keep);

    _channel_ids.resize(rows);
    for (size_t i = 0; i < rows; ++i) {
        const int8_t op = ops[i];
        if (op == kUpdateOperation) {
            _channel_ids[i] = delete_hashes[i];
            continue;
        }
        if (_is_insert_op(op)) {
            _channel_ids[i] = _insert_random ? _next_rr_channel() : insert_hashes[i];
        } else if (_is_delete_op(op)) {
            _channel_ids[i] = delete_hashes[i];
        } else {
            return Status::InternalError("Unknown Iceberg merge operation {}", op);
        }
    }

    if (has_update) {
        for (size_t col_idx = 0; col_idx < block->columns(); ++col_idx) {
            block->replace_by_position_if_const(col_idx);
        }

        MutableColumns mutable_columns = block->mutate_columns();
        MutableColumnPtr& op_mut = mutable_columns[op_idx];
        ColumnInt8* op_values_col = nullptr;
        if (auto* nullable_col = check_and_get_column<ColumnNullable>(op_mut.get())) {
            op_values_col =
                    check_and_get_column<ColumnInt8>(nullable_col->get_nested_column_ptr().get());
        } else {
            op_values_col = check_and_get_column<ColumnInt8>(op_mut.get());
        }
        if (op_values_col == nullptr) {
            block->set_columns(std::move(mutable_columns));
            return Status::InternalError("Merge operation column must be tinyint");
        }
        auto& op_values = op_values_col->get_data();
        const size_t original_rows = rows;
        for (size_t row = 0; row < original_rows; ++row) {
            if (ops[row] != kUpdateOperation) {
                continue;
            }
            op_values[row] = kUpdateDeleteOperation;
            for (size_t col_idx = 0; col_idx < mutable_columns.size(); ++col_idx) {
                mutable_columns[col_idx]->insert_from(*mutable_columns[col_idx], row);
            }
            const size_t new_row_idx = op_values.size() - 1;
            op_values[new_row_idx] = kUpdateInsertOperation;
            const uint32_t insert_channel =
                    _insert_random ? _next_rr_channel() : insert_hashes[row];
            _channel_ids.push_back(insert_channel);
        }
        block->set_columns(std::move(mutable_columns));
    }

    return Status::OK();
}

Status MergePartitioner::clone(RuntimeState* state,
                               std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner =
            new MergePartitioner(_partition_count, _merge_info, _use_new_shuffle_hash_method);
    partitioner.reset(new_partitioner);
    RETURN_IF_ERROR(
            _clone_expr_ctxs(state, _operation_expr_ctxs, new_partitioner->_operation_expr_ctxs));
    RETURN_IF_ERROR(_clone_expr_ctxs(state, _insert_partition_expr_ctxs,
                                    new_partitioner->_insert_partition_expr_ctxs));
    if (!_insert_partition_fields.empty()) {
        VExprContextSPtrs src_field_ctxs;
        src_field_ctxs.reserve(_insert_partition_fields.size());
        for (const auto& field : _insert_partition_fields) {
            src_field_ctxs.emplace_back(field.expr_ctx);
        }
        VExprContextSPtrs dst_field_ctxs;
        RETURN_IF_ERROR(_clone_expr_ctxs(state, src_field_ctxs, dst_field_ctxs));
        new_partitioner->_insert_partition_fields.reserve(dst_field_ctxs.size());
        for (size_t i = 0; i < dst_field_ctxs.size(); ++i) {
            InsertPartitionField field;
            field.transform = _insert_partition_fields[i].transform;
            field.expr_ctx = dst_field_ctxs[i];
            field.source_id = _insert_partition_fields[i].source_id;
            field.name = _insert_partition_fields[i].name;
            new_partitioner->_insert_partition_fields.emplace_back(std::move(field));
        }
    }
    RETURN_IF_ERROR(_clone_expr_ctxs(state, _delete_partition_expr_ctxs,
                                    new_partitioner->_delete_partition_expr_ctxs));
    new_partitioner->_insert_random = _insert_random;
    new_partitioner->_rr_offset = _rr_offset;
    return Status::OK();
}

Status MergePartitioner::_compute_insert_hashes(Block* block, std::vector<uint32_t>& hashes) const {
    if (!_insert_partition_fields.empty()) {
        return _compute_hashes_with_transform(block, _insert_partition_fields, hashes);
    }
    return _compute_hashes(block, _insert_partition_expr_ctxs, hashes, false);
}

Status MergePartitioner::_compute_hashes_with_transform(
        Block* block, const std::vector<InsertPartitionField>& fields,
        std::vector<uint32_t>& hashes) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        hashes.clear();
        return Status::OK();
    }
    if (fields.empty()) {
        return Status::InternalError("Merge partitioning insert fields are empty");
    }

    std::vector<int> results(fields.size());
    for (size_t i = 0; i < fields.size(); ++i) {
        RETURN_IF_ERROR(fields[i].expr_ctx->execute(block, &results[i]));
    }

    _initialize_hash_vals(hashes, rows);
    auto* __restrict hash_values = hashes.data();
    for (size_t i = 0; i < fields.size(); ++i) {
        if (fields[i].transformer == nullptr) {
            return Status::InternalError("Merge partitioning transform is not initialized");
        }
        ColumnWithTypeAndName transformed =
                fields[i].transformer->apply(*block, results[i]);
        const auto& [column, is_const] = unpack_if_const(transformed.column);
        if (is_const) {
            continue;
        }
        _hash_column(column, transformed.type, hash_values);
    }
    return Status::OK();
}

Status MergePartitioner::_compute_hashes(Block* block, const VExprContextSPtrs& expr_ctxs,
                                         std::vector<uint32_t>& hashes,
                                         bool delete_branch) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        hashes.clear();
        return Status::OK();
    }

    std::vector<int> results(expr_ctxs.size());
    for (size_t i = 0; i < expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(expr_ctxs[i]->execute(block, &results[i]));
    }

    _initialize_hash_vals(hashes, rows);
    auto* __restrict hash_values = hashes.data();
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& col_info = block->get_by_position(results[i]);
        const auto& [column, is_const] = unpack_if_const(col_info.column);
        if (is_const) {
            continue;
        }
        ColumnPtr hash_col = column;
        DataTypePtr hash_type = col_info.type;
        if (delete_branch) {
            RETURN_IF_ERROR(_get_delete_hash_column(col_info, &hash_col, &hash_type));
        }
        _hash_column(hash_col, hash_type, hash_values);
    }
    return Status::OK();
}

void MergePartitioner::_initialize_hash_vals(std::vector<uint32_t>& hashes, size_t rows) const {
    hashes.resize(rows);
    if (_use_new_shuffle_hash_method) {
        constexpr uint32_t kShuffleSeed = 0x9E3779B9U;
        std::fill(hashes.begin(), hashes.end(), kShuffleSeed);
    } else {
        std::fill(hashes.begin(), hashes.end(), 0);
    }
}

void MergePartitioner::_hash_column(const ColumnPtr& column, const DataTypePtr& type,
                                    uint32_t* hashes) const {
    if (_use_new_shuffle_hash_method) {
        column->update_crc32c_batch(hashes, nullptr);
    } else {
        column->update_crcs_with_value(hashes, type->get_primitive_type(),
                                       cast_set<uint32_t>(column->size()));
    }
}

Status MergePartitioner::_get_delete_hash_column(const ColumnWithTypeAndName& column,
                                                 ColumnPtr* out_column,
                                                 DataTypePtr* out_type) const {
    ColumnPtr hash_col = column.column;
    DataTypePtr hash_type = column.type;
    if (auto* nullable_col = check_and_get_column<ColumnNullable>(hash_col.get())) {
        hash_col = nullable_col->get_nested_column_ptr();
        hash_type = remove_nullable(hash_type);
    }
    const auto* struct_col = check_and_get_column<ColumnStruct>(hash_col.get());
    const auto* struct_type = check_and_get_data_type<DataTypeStruct>(hash_type.get());
    if (!struct_col || !struct_type) {
        *out_column = column.column;
        *out_type = column.type;
        return Status::OK();
    }

    int file_path_idx = _find_file_path_index(*struct_type);
    if (file_path_idx < 0 || file_path_idx >= struct_col->tuple_size()) {
        return Status::InternalError("Row id struct missing file_path field");
    }
    *out_column = struct_col->get_column_ptr(file_path_idx);
    *out_type = struct_type->get_element(file_path_idx);
    return Status::OK();
}

int MergePartitioner::_find_file_path_index(const DataTypeStruct& struct_type) const {
    auto normalize = [](const std::string& name) { return doris::to_lower(name); };
    auto match_any = [](const std::string& name,
                        std::initializer_list<const char*> candidates) {
        for (const char* candidate : candidates) {
            if (name == candidate) {
                return true;
            }
        }
        return false;
    };

    int file_path_idx = -1;
    const auto& field_names = struct_type.get_element_names();
    for (size_t i = 0; i < field_names.size(); ++i) {
        std::string name = normalize(field_names[i]);
        if (file_path_idx < 0 && match_any(name, {"file_path", "data_file_path", "path"})) {
            file_path_idx = static_cast<int>(i);
            break;
        }
    }

    if (file_path_idx < 0 && !struct_type.get_elements().empty()) {
        file_path_idx = 0;
    }
    return file_path_idx;
}

void MergePartitioner::_apply_partition_ids(std::vector<uint32_t>& hashes,
                                            size_t partition_count) const {
    if (partition_count == 0) {
        return;
    }
    if (_use_new_shuffle_hash_method) {
        for (auto& hash : hashes) {
            hash = ShiftChannelIds()(hash, partition_count);
        }
    } else {
        for (auto& hash : hashes) {
            hash = ShuffleChannelIds()(hash, partition_count);
        }
    }
}

void MergePartitioner::_apply_insert_rebalance(const std::vector<int8_t>& ops,
                                               std::vector<uint32_t>& insert_hashes,
                                               size_t block_bytes) const {
    if (!_enable_insert_rebalance || _partition_rebalancer == nullptr) {
        return;
    }
    if (insert_hashes.empty() || _insert_partition_count == 0) {
        return;
    }
    if (_partition_row_counts.size() != _insert_partition_count
        || _partition_writer_ids.size() != _insert_partition_count
        || _partition_writer_indexes.size() != _insert_partition_count) {
        return;
    }

    _partition_rebalancer->rebalance();
    std::fill(_partition_row_counts.begin(), _partition_row_counts.end(), 0);
    std::fill(_partition_writer_ids.begin(), _partition_writer_ids.end(), -1);

    for (size_t i = 0; i < ops.size(); ++i) {
        if (!_is_insert_op(ops[i])) {
            continue;
        }
        const uint32_t partition_id = insert_hashes[i];
        if (partition_id >= _insert_partition_count) {
            continue;
        }
        _partition_row_counts[partition_id] += 1;
        int writer_id = _partition_writer_ids[partition_id];
        if (writer_id == -1) {
            writer_id = _get_next_writer_id(static_cast<int>(partition_id));
            _partition_writer_ids[partition_id] = writer_id;
        }
        insert_hashes[i] = static_cast<uint32_t>(writer_id);
    }

    for (size_t i = 0; i < _partition_row_counts.size(); ++i) {
        if (_partition_row_counts[i] > 0) {
            _partition_rebalancer->add_partition_row_count(
                    static_cast<int>(i), _partition_row_counts[i]);
        }
    }
    _partition_rebalancer->add_data_processed(static_cast<long>(block_bytes));
}

void MergePartitioner::_init_insert_scaling(RuntimeState* state) {
    _enable_insert_rebalance = false;
    _insert_partition_count = 0;
    _insert_data_processed = 0;
    _insert_writer_count = 1;
    _non_partition_scaling_threshold =
            config::table_sink_non_partition_write_scaling_data_processed_threshold;

    if (_partition_count == 0) {
        return;
    }
    if (_insert_random) {
        return;
    }
    if (_insert_partition_expr_ctxs.empty() && _insert_partition_fields.empty()) {
        return;
    }

    int max_partitions_per_writer = config::table_sink_partition_write_max_partition_nums_per_writer;
    if (max_partitions_per_writer <= 0) {
        return;
    }
    _insert_partition_count = _partition_count * max_partitions_per_writer;
    if (_insert_partition_count == 0) {
        return;
    }

    int task_num = state == nullptr ? 0 : state->task_num();
    int64_t min_partition_threshold = scale_threshold_by_task(
            config::table_sink_partition_write_min_partition_data_processed_rebalance_threshold,
            task_num);
    int64_t min_data_threshold = scale_threshold_by_task(
            config::table_sink_partition_write_min_data_processed_rebalance_threshold,
            task_num);

    _partition_rebalancer = std::make_unique<SkewedPartitionRebalancer>(
            static_cast<int>(_insert_partition_count), static_cast<int>(_partition_count), 1,
            min_partition_threshold, min_data_threshold);
    _partition_row_counts.assign(_insert_partition_count, 0);
    _partition_writer_ids.assign(_insert_partition_count, -1);
    _partition_writer_indexes.assign(_insert_partition_count, 0);
    _enable_insert_rebalance = true;
}

int MergePartitioner::_get_next_writer_id(int partition_id) const {
    return _partition_rebalancer->get_task_id(partition_id,
                                              _partition_writer_indexes[partition_id]++);
}

bool MergePartitioner::_is_insert_op(int8_t op) const {
    return op == kInsertOperation || op == kUpdateInsertOperation || op == kUpdateOperation;
}

bool MergePartitioner::_is_delete_op(int8_t op) const {
    return op == kDeleteOperation || op == kUpdateDeleteOperation || op == kUpdateOperation;
}

uint32_t MergePartitioner::_next_rr_channel() const {
    uint32_t writer_count = static_cast<uint32_t>(_partition_count);
    if (_insert_random && _insert_writer_count > 0) {
        writer_count = std::min<uint32_t>(static_cast<uint32_t>(_partition_count),
                                          static_cast<uint32_t>(_insert_writer_count));
    }
    if (writer_count == 0) {
        return 0;
    }
    const uint32_t channel = _rr_offset % writer_count;
    _rr_offset = (_rr_offset + 1) % writer_count;
    return channel;
}

Status MergePartitioner::_clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                                          VExprContextSPtrs& dst) const {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        RETURN_IF_ERROR(src[i]->clone(state, dst[i]));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
