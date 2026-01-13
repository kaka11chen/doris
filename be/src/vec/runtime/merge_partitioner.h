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

#include <gen_cpp/Partitions_types.h>

#include <string>

#include "vec/runtime/partitioner.h"
#include "vec/exec/skewed_partition_rebalancer.h"
#include "vec/sink/writer/iceberg/partition_transformers.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class MergePartitioner final : public PartitionerBase {
public:
    MergePartitioner(size_t partition_count, const TMergePartitionInfo& merge_info,
                     bool use_new_shuffle_hash_method);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block) const override;
    const std::vector<HashValType>& get_channel_ids() const override { return _channel_ids; }
    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    struct InsertPartitionField {
        std::string transform;
        VExprContextSPtr expr_ctx;
        std::unique_ptr<PartitionColumnTransform> transformer;
        int32_t source_id = 0;
        std::string name;
    };

    Status _compute_insert_hashes(Block* block, std::vector<uint32_t>& hashes) const;
    Status _compute_hashes(Block* block, const VExprContextSPtrs& expr_ctxs,
                           std::vector<uint32_t>& hashes, bool delete_branch) const;
    Status _compute_hashes_with_transform(Block* block,
                                          const std::vector<InsertPartitionField>& fields,
                                          std::vector<uint32_t>& hashes) const;
    void _initialize_hash_vals(std::vector<uint32_t>& hashes, size_t rows) const;
    void _hash_column(const ColumnPtr& column, const DataTypePtr& type, uint32_t* hashes) const;
    Status _get_delete_hash_column(const ColumnWithTypeAndName& column, ColumnPtr* out_column,
                                   DataTypePtr* out_type) const;
    int _find_file_path_index(const DataTypeStruct& struct_type) const;
    void _apply_partition_ids(std::vector<uint32_t>& hashes, size_t partition_count) const;
    void _apply_insert_rebalance(const std::vector<int8_t>& ops, std::vector<uint32_t>& insert_hashes,
                                 size_t block_bytes) const;
    void _init_insert_scaling(RuntimeState* state);
    int _get_next_writer_id(int partition_id) const;
    bool _is_insert_op(int8_t op) const;
    bool _is_delete_op(int8_t op) const;
    uint32_t _next_rr_channel() const;
    Status _clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                            VExprContextSPtrs& dst) const;

    TMergePartitionInfo _merge_info;
    bool _use_new_shuffle_hash_method = false;
    bool _insert_random = false;
    bool _enable_insert_rebalance = false;
    size_t _insert_partition_count = 0;
    mutable int64_t _insert_data_processed = 0;
    mutable int _insert_writer_count = 1;
    int64_t _non_partition_scaling_threshold = 0;
    VExprContextSPtrs _operation_expr_ctxs;
    VExprContextSPtrs _insert_partition_expr_ctxs;
    std::vector<InsertPartitionField> _insert_partition_fields;
    VExprContextSPtrs _delete_partition_expr_ctxs;
    mutable std::unique_ptr<SkewedPartitionRebalancer> _partition_rebalancer;
    mutable std::vector<int> _partition_row_counts;
    mutable std::vector<int> _partition_writer_ids;
    mutable std::vector<int> _partition_writer_indexes;
    mutable std::vector<uint32_t> _channel_ids;
    mutable uint32_t _rr_offset = 0;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
