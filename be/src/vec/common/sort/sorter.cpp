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

#include "vec/common/sort/sorter.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <ostream>
#include <string>
#include <utility>

#include "common/object_pool.h"
#include "runtime/exec_env.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/utils/util.hpp"

namespace doris {
class RowDescriptor;
} // namespace doris

namespace doris::vectorized {

// When doing spillable sorting, each sorted block is spilled into a single file.
//
// In order to decrease memory pressure when merging
// multiple spilled blocks into one bigger sorted block, only part
// of each spilled blocks are read back into memory at a time.
//
// Currently the spilled blocks are splitted into small sub blocks,
// each sub block is serialized in PBlock format and appended
// to the spill file.
//

void MergeSorterState::reset() {
    auto empty_queue = std::priority_queue<MergeSortCursor>();
    priority_queue_.swap(empty_queue);
    std::vector<MergeSortCursorImpl> empty_cursors(0);
    cursors_.swap(empty_cursors);
    std::vector<Block> empty_blocks(0);
    sorted_blocks_.swap(empty_blocks);
    unsorted_block_ = Block::create_unique(unsorted_block_->clone_empty());
    in_mem_sorted_bocks_size_ = 0;
}
Status MergeSorterState::add_sorted_block(Block& block) {
    auto rows = block.rows();
    if (0 == rows) {
        return Status::OK();
    }
    in_mem_sorted_bocks_size_ += block.bytes();
    sorted_blocks_.emplace_back(std::move(block));
    num_rows_ += rows;
    return Status::OK();
}

Status MergeSorterState::build_merge_tree(const SortDescription& sort_description) {
    fprintf(stderr, "build_merge_tree:: sorted_blocks_.size(): %ld\n", sorted_blocks_.size());
    for (auto& block : sorted_blocks_) {
        fprintf(stderr, "build_merge_tree:: block.rows(): %ld, block.columns: %ld\n", block.rows(), block.columns());
        cursors_.emplace_back(block, sort_description);
    }

    if (sorted_blocks_.size() > 1) {
        for (auto& cursor : cursors_) {
            priority_queue_.emplace(&cursor);
        }
    }

    return Status::OK();
}

Status MergeSorterState::merge_sort_read(doris::vectorized::Block* block, int batch_size,
                                         bool* eos) {
    if (sorted_blocks_.empty()) {
        *eos = true;
    } else if (sorted_blocks_.size() == 1) {
        if (offset_ != 0) {
            sorted_blocks_[0].skip_num_rows(offset_);
        }
        block->swap(sorted_blocks_[0]);
        *eos = true;
    } else {
        RETURN_IF_ERROR(_merge_sort_read_impl(batch_size, block, eos));
    }
    return Status::OK();
}

Status MergeSorterState::_merge_sort_read_impl(int batch_size, doris::vectorized::Block* block,
                                               bool* eos) {
    size_t num_columns = sorted_blocks_[0].columns();

    MutableBlock m_block = VectorizedUtils::build_mutable_mem_reuse_block(block, sorted_blocks_[0]);
    MutableColumns& merged_columns = m_block.mutable_columns();

    /// Take rows from queue in right order and push to 'merged'.
    size_t merged_rows = 0;
    while (!priority_queue_.empty()) {
        auto current = priority_queue_.top();
        priority_queue_.pop();

        if (offset_ == 0) {
            for (size_t i = 0; i < num_columns; ++i)
                merged_columns[i]->insert_from(*current->all_columns[i], current->pos);
            ++merged_rows;
        } else {
            offset_--;
        }

        if (!current->is_last()) {
            current->next();
            priority_queue_.push(current);
        }

        if (merged_rows == batch_size) break;
    }
    block->set_columns(std::move(merged_columns));

    if (merged_rows == 0) {
        *eos = true;
        return Status::OK();
    }

    return Status::OK();
}

Status Sorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                         int batch_size, bool* eos) {
    return get_next(state, block, eos);
}

Status Sorter::partial_sort(Block& src_block, Block& dest_block) {
    size_t num_cols = src_block.columns();
    if (_materialize_sort_exprs) {
        fprintf(stderr, "_materialize_sort_exprs\n");
        fprintf(stderr, "partial_sort. src_block->columns(): %ld, desc_block.columns(): %ld\n", src_block.columns(), dest_block.columns());
        auto output_tuple_expr_ctxs = _vsort_exec_exprs.sort_tuple_slot_expr_ctxs();
        std::vector<int> valid_column_ids(output_tuple_expr_ctxs.size());
        for (int i = 0; i < output_tuple_expr_ctxs.size(); ++i) {
            RETURN_IF_ERROR(output_tuple_expr_ctxs[i]->execute(&src_block, &valid_column_ids[i]));
        }

        Block new_block;
        int i = 0;
        const auto& convert_nullable_flags = _vsort_exec_exprs.get_convert_nullable_flags();
        for (auto column_id : valid_column_ids) {
            if (column_id < 0) {
                continue;
            }
            if (i < convert_nullable_flags.size() && convert_nullable_flags[i]) {
                auto column_ptr = make_nullable(src_block.get_by_position(column_id).column);
                new_block.insert(
                        {column_ptr, make_nullable(src_block.get_by_position(column_id).type), ""});
            } else {
                new_block.insert(src_block.get_by_position(column_id));
            }
            i++;
        }
        dest_block.swap(new_block);
    }

    fprintf(stderr, "_vsort_exec_exprs.lhs_ordering_expr_ctxs().size(): %ld\n", _vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    _sort_description.resize(_vsort_exec_exprs.lhs_ordering_expr_ctxs().size());
    Block* result_block = _materialize_sort_exprs ? &dest_block : &src_block;
    fprintf(stderr, "result_block->columns(): %ld\n", result_block->columns());
    for (int i = 0; i < _sort_description.size(); i++) {
        const auto& ordering_expr = _vsort_exec_exprs.lhs_ordering_expr_ctxs()[i];
        fprintf(stderr, "before ordering_expr->execute(result_block, &_sort_description[i].column_number)\n");
        RETURN_IF_ERROR(ordering_expr->execute(result_block, &_sort_description[i].column_number));
        fprintf(stderr, "after ordering_expr->execute(result_block, &_sort_description[i].column_number)\n");

        _sort_description[i].direction = _is_asc_order[i] ? 1 : -1;
        _sort_description[i].nulls_direction =
                _nulls_first[i] ? -_sort_description[i].direction : _sort_description[i].direction;
    }

    {
        SCOPED_TIMER(_partial_sort_timer);
        if (_materialize_sort_exprs) {
            sort_block(dest_block, dest_block, _sort_description, _offset + _limit);
        } else {
            sort_block(src_block, dest_block, _sort_description, _offset + _limit);
        }
        src_block.clear_column_data(num_cols);
    }

    return Status::OK();
}

FullSorter::FullSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset,
                       ObjectPool* pool, std::vector<bool>& is_asc_order,
                       std::vector<bool>& nulls_first, const RowDescriptor& row_desc,
                       RuntimeState* state, RuntimeProfile* profile)
        : Sorter(vsort_exec_exprs, limit, offset, pool, is_asc_order, nulls_first),
          _state(MergeSorterState::create_unique(row_desc, offset, limit, state, profile)) {}

Status FullSorter::append_block(Block* block) {
    fprintf(stderr, "append_block print_use_count: %s\n", block->print_use_count().c_str());
    DCHECK(block->rows() > 0);
    {
        SCOPED_TIMER(_merge_block_timer);
        fprintf(stderr, "block.rows(): %ld, columns(): %ld\n", block->rows(), block->columns());
        auto& data = _state->unsorted_block_->get_columns_with_type_and_name();
        const auto& arrival_data = block->get_columns_with_type_and_name();
        auto sz = block->rows();
        fprintf(stderr, "data.size(): %ld\n", data.size());
        for (int i = 0; i < data.size(); ++i) {
            DCHECK(data[i].type->equals(*(arrival_data[i].type)))
                    << " type1: " << data[i].type->get_name()
                    << " type2: " << arrival_data[i].type->get_name() << " i: " << i;
            //TODO: to eliminate unnecessary expansion, we need a `insert_range_from_const` for every column type.
            data[i].column->assume_mutable()->insert_range_from(
                    *arrival_data[i].column->convert_to_full_column_if_const(), 0, sz);
        }
        block->clear_column_data();
    }
//    if (_reach_limit()) {
        RETURN_IF_ERROR(_do_sort());
//    }
    return Status::OK();
}

Status FullSorter::prepare_for_read() {
    if (_state->unsorted_block_->rows() > 0) {
        RETURN_IF_ERROR(_do_sort());
    }
    return _state->build_merge_tree(_sort_description);
}

Status FullSorter::get_next(RuntimeState* state, Block* block, bool* eos) {
    return _state->merge_sort_read(block, state->batch_size(), eos);
}

Status FullSorter::merge_sort_read_for_spill(RuntimeState* state, doris::vectorized::Block* block,
                                             int batch_size, bool* eos) {
    return _state->merge_sort_read(block, batch_size, eos);
}

Status FullSorter::_do_sort() {
    fprintf(stderr, "_do_sort\n");
    Block* src_block = _state->unsorted_block_.get();
    Block desc_block = src_block->clone_without_columns();
    fprintf(stderr, "_do_sort. src_block->columns(): %ld, desc_block.columns(): %ld\n", src_block->columns(), desc_block.columns());
    RETURN_IF_ERROR(partial_sort(*src_block, desc_block));

    // dispose TOP-N logic
    if (_limit != -1 && !_enable_spill) {
        fprintf(stderr, "_limit != -1 && !_enable_spill");
        // Here is a little opt to reduce the mem usage, we build a max heap
        // to order the block in _block_priority_queue.
        // if one block totally greater the heap top of _block_priority_queue
        // we can throw the block data directly.
        if (_state->num_rows() < _offset + _limit) {
            static_cast<void>(_state->add_sorted_block(desc_block));
            _block_priority_queue.emplace(_pool->add(
                    new MergeSortCursorImpl(_state->last_sorted_block(), _sort_description)));
        } else {
            auto tmp_cursor_impl =
                    std::make_unique<MergeSortCursorImpl>(desc_block, _sort_description);
            MergeSortBlockCursor block_cursor(tmp_cursor_impl.get());
            if (!block_cursor.totally_greater(_block_priority_queue.top())) {
                static_cast<void>(_state->add_sorted_block(desc_block));
                _block_priority_queue.emplace(_pool->add(
                        new MergeSortCursorImpl(_state->last_sorted_block(), _sort_description)));
            }
        }
    } else {
        // dispose normal sort logic
        fprintf(stderr, "_state->add_sorted_block\n");
        static_cast<void>(_state->add_sorted_block(desc_block));
    }
    return Status::OK();
}
size_t FullSorter::data_size() const {
    return _state->data_size();
}

void FullSorter::reset() {
    _state->reset();
}

} // namespace doris::vectorized
