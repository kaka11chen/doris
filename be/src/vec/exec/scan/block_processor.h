#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/columns/column.h"
#include "vec/columns/lazy_column.h"
#include "vec/exprs/vexpr_fwd.h"
#include <vector>
#include <memory>
#include <optional>
#include <functional>
#include "vec/exec/format/parquet/parquet_common.h"
#include "common/status.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/data_types/data_type_lazy.h"
#include "vec/columns/dictionary_column.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {



class BlockFilter {
public:
    virtual Result<std::shared_ptr<IColumn::Filter>> filter(Block* block) = 0;
    virtual ~BlockFilter() = default;
};

class BlockProjection {
public:
    virtual Status project(Block* block, const std::vector<uint32_t>& columns_to_filter, const IColumn::Filter& filter, size_t column_to_keep) = 0;
    virtual ~BlockProjection() = default;
};

class DefaultBlockFilter : public BlockFilter {
public:
    DefaultBlockFilter(VExprContextSPtrs conjuncts) : _conjuncts(std::move(conjuncts)) {
        // 递归收集所有 column id
        std::function<void(const VExpr*)> collect_filter_column_ids = [&](const VExpr* expr) {
            if (auto* slot_ref = dynamic_cast<const VSlotRef*>(expr)) {
                _filter_column_ids.push_back(slot_ref->column_id());
            }
            // 递归处理子表达式
            for (const auto& child : expr->children()) {
                collect_filter_column_ids(child.get());
            }
        };

        // 遍历所有 conjuncts
        for (const auto& conjunct : _conjuncts) {
            collect_filter_column_ids(conjunct->root().get());
        }
    }
    Result<std::shared_ptr<IColumn::Filter>> filter(Block* block) override {
//        FilterMap filter_map;
        bool can_filter_all = false;

        // 对 block 中的 LazyColumn 进行脱壳
        bool conjuncts_contains_first_column = false;
        size_t column_size;
        for (int filter_column_id : _filter_column_ids) {
            if (filter_column_id == 0) {
                conjuncts_contains_first_column = true;
            }
            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(filter_column_id);
            auto& column = column_with_type_and_name.column;
            auto& type = column_with_type_and_name.type;
            if (is_column<LazyColumn>(*column)) {
                const LazyColumn& lazy_column = assert_cast<const LazyColumn&>(*column);
                block->replace_by_position(filter_column_id, lazy_column.get_column());
                column_with_type_and_name.column = lazy_column.get_column();
                column_with_type_and_name.type = assert_cast<const DataTypeLazy*>(type.get())->get_nested_type();
                column_size = column->size();
            }
        }

        if (!conjuncts_contains_first_column) {
            auto first_column = block->get_by_position(0).column->assume_mutable();
            if (is_column<LazyColumn>(*first_column)) {
                LazyColumn& lazy_column = assert_cast<LazyColumn&>(*first_column);
                lazy_column.set_size(column_size);
            }
        }
        auto result_filter = std::make_shared<IColumn::Filter>(block->rows(), 1);
        RETURN_IF_ERROR_RESULT(VExprContext::execute_conjuncts(_conjuncts, nullptr, block,
                                                               result_filter.get(), &can_filter_all));
        for (size_t i = 0; i < result_filter->size(); ++i) {
            fprintf(stdout, "filter[%zu] = %d\n", i, result_filter->operator[](i));
        }
        return result_filter;
    }
private:
    VExprContextSPtrs _conjuncts;
    std::vector<int> _filter_column_ids;
};

class DefaultBlockProjection : public BlockProjection {
public:
    Status project(Block* block, const std::vector<uint32_t>& columns_to_filter, const IColumn::Filter& filter, size_t column_to_keep) override {
        for (int i = 0; i < column_to_keep; ++i) {
            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(i);
            auto& column = column_with_type_and_name.column;
            auto& type = column_with_type_and_name.type;
            if (is_column<LazyColumn>(*column)) {
                const LazyColumn& lazy_column = assert_cast<const LazyColumn&>(*column);
//                block->replace_by_position(i, lazy_column.get_column());
                column_with_type_and_name.column = lazy_column.get_column();
                column_with_type_and_name.type = assert_cast<const DataTypeLazy*>(type.get())->get_nested_type();
            }
        }
        for (size_t i = 0; i < filter.size(); ++i) {
            fprintf(stdout, "filter[%zu] = %d\n", i, filter[i]);
        }
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(
                block, columns_to_filter, filter));
        Block::erase_useless_column(block, column_to_keep);
        return Status::OK();
    }
};

class DictionaryAwareBlockFilter : public BlockFilter {
public:
    DictionaryAwareBlockFilter(VExprContextSPtrs conjuncts) 
        : _conjuncts(std::move(conjuncts)) {
        // 递归收集所有 column id
        std::function<void(const VExpr*)> collect_filter_column_ids = [&](const VExpr* expr) {
            if (auto* slot_ref = dynamic_cast<const VSlotRef*>(expr)) {
                _filter_column_ids.push_back(slot_ref->column_id());
            }
            // 递归处理子表达式
            for (const auto& child : expr->children()) {
                collect_filter_column_ids(child.get());
            }
        };

        // 遍历所有 conjuncts
        for (const auto& conjunct : _conjuncts) {
            collect_filter_column_ids(conjunct->root().get());
        }
    }

    Result<std::shared_ptr<IColumn::Filter>> filter(Block* block) override {

        if (_filter_dict_column_ids.empty()) {
            // 遍历所有列，查找字典列
//            for (size_t col_idx = 0; col_idx < block->columns(); ++col_idx) {
            for (int filter_column_id : _filter_column_ids) {
                ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(filter_column_id);
                auto& column = column_with_type_and_name.column;
                auto& type = column_with_type_and_name.type;
                if (is_column<LazyColumn>(*column)) {
                    const LazyColumn& lazy_column = assert_cast<const LazyColumn&>(*column);
//                    block->replace_by_position(filter_column_id, lazy_column.get_column());
                    column_with_type_and_name.column = lazy_column.get_column();
                    column_with_type_and_name.type = assert_cast<const DataTypeLazy*>(type.get())->get_nested_type();
                }
            }

            for (int filter_column_id : _filter_column_ids) {
                ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(filter_column_id);
                auto& column = column_with_type_and_name.column;
                const IColumn* actual_column = column.get();
                if (is_column<ColumnNullable>(*actual_column)) {
                    const ColumnNullable& nullable_column = assert_cast<const ColumnNullable&>(*actual_column);
                    actual_column = nullable_column.get_nested_column_ptr().get();
                }
                if (is_column<DictionaryColumn<int32_t>>(*actual_column)) {
                    _filter_dict_column_ids.push_back(filter_column_id);
                }
            }
        }

        // 如果有字典列，则处理所有字典列
        if (!_filter_dict_column_ids.empty()) {
            auto result_filter = std::make_shared<IColumn::Filter>(block->rows(), 1);
            bool has_filter = false;

            // 遍历所有字典列
            for (size_t filter_dict_column_id : _filter_dict_column_ids) {
                ColumnWithTypeAndName& dict_column_with_type_and_name = block->get_by_position(filter_dict_column_id);
                auto& column = dict_column_with_type_and_name.column;
                const IColumn* actual_column = column.get();
                if (is_column<ColumnNullable>(*actual_column)) {
                    const ColumnNullable& nullable_column = assert_cast<const ColumnNullable&>(*actual_column);
                    actual_column = nullable_column.get_nested_column_ptr().get();
                }
                const DictionaryColumn<int32_t>& dict_column = assert_cast<const DictionaryColumn<int32_t>&>(*actual_column);
                auto dictionary = dict_column.get_dictionary();
                const auto& ids = dict_column.get_ids();

                // 处理字典
                auto dictionary_filter = _process_dictionary(dictionary, filter_dict_column_id, block);
                if (dictionary_filter) {
                    // 应用字典过滤结果
                    for (size_t i = 0; i < ids.size(); ++i) {
                        (*result_filter)[i] &= (*dictionary_filter)[ids[i]];
                    }
                    has_filter = true;
                }
            }

            if (has_filter) {
                return result_filter;
            }
        }

        // 如果没有找到字典列或处理失败，回退到普通过滤
        return DefaultBlockFilter(_conjuncts).filter(block);
    }

private:
    std::shared_ptr<IColumn::Filter> _process_dictionary(ColumnPtr dictionary, size_t filter_dict_column_id, Block* block) {
        // 查找当前字典列是否已经缓存
        auto it = std::find(_filter_dict_column_indices.begin(), _filter_dict_column_indices.end(), filter_dict_column_id);
        if (it != _filter_dict_column_indices.end()) {
            size_t index = std::distance(_filter_dict_column_indices.begin(), it);
            if (_last_dictionaries[index] == dictionary) {
                return _last_dictionary_filters[index];
            }
        }

        // 1. 构建临时 block
        Block temp_block;
        for (size_t col_id = 0; col_id < block->columns(); ++col_id) {
            if (col_id == filter_dict_column_id) {
                // 使用字典列替换当前列
                temp_block.insert(ColumnWithTypeAndName(dictionary, std::make_shared<DataTypeString>(), ""));
            } else {
                // 插入空列
                auto type = std::make_shared<DataTypeString>();
                temp_block.insert(ColumnWithTypeAndName(type->create_column(),
                                                        type,
                                                        ""));
                if (col_id == 0) {
                    temp_block.get_by_position(0).column->assume_mutable()->resize(
                            dictionary->size());
                }
            }
        }

        // 2. 执行过滤
        IColumn::Filter result_filter(temp_block.rows(), 1);
        bool can_filter_all = false;
        {
//            SCOPED_RAW_TIMER(&_predicate_filter_time);
            static_cast<void>(VExprContext::execute_conjuncts(_conjuncts, nullptr, &temp_block,
                                                                   &result_filter, &can_filter_all));
            for (size_t i = 0; i < result_filter.size(); ++i) {
                fprintf(stdout, "filter[%zu] = %d\n", i, result_filter.operator[](i));
            }
        }

        // 3. 保存过滤结果
        if (it == _filter_dict_column_indices.end()) {
            // 如果是新字典列，添加到缓存中
            _filter_dict_column_indices.push_back(filter_dict_column_id);
            _last_dictionaries.push_back(dictionary);
            _last_dictionary_types.push_back(block->get_by_position(filter_dict_column_id).type);
            _last_dictionary_filters.push_back(std::make_shared<IColumn::Filter>(std::move(result_filter)));  // 使用 std::move
        } else {
            // 更新已有字典列的缓存
            size_t index = std::distance(_filter_dict_column_indices.begin(), it);
            _last_dictionaries[index] = dictionary;
            _last_dictionary_types[index] = block->get_by_position(filter_dict_column_id).type;
            _last_dictionary_filters[index] = std::make_shared<IColumn::Filter>(std::move(result_filter));  // 使用 std::move
        }

        return _last_dictionary_filters.back();
    }

    VExprContextSPtrs _conjuncts;
    std::vector<int> _filter_column_ids;
    std::vector<int> _filter_dict_column_ids;
    // 记录所有字典列的索引
    std::vector<size_t> _filter_dict_column_indices;
    // 使用 vector 支持多个字典列的缓存
    std::vector<ColumnPtr> _last_dictionaries;
    std::vector<DataTypePtr> _last_dictionary_types;
    std::vector<std::shared_ptr<IColumn::Filter>> _last_dictionary_filters;
};

class DictionaryAwareBlockProjection : public BlockProjection {
public:
    Status project(Block* block, const std::vector<uint32_t>& columns_to_filter, const IColumn::Filter& filter, size_t column_to_keep) override {
        for (int i = 0; i < column_to_keep; ++i) {
            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(i);
            auto& column = column_with_type_and_name.column;
            auto& type = column_with_type_and_name.type;
            if (is_column<LazyColumn>(*column)) {
                const LazyColumn& lazy_column = assert_cast<const LazyColumn&>(*column);
                column_with_type_and_name.column = lazy_column.get_column();
                column_with_type_and_name.type = assert_cast<const DataTypeLazy*>(type.get())->get_nested_type();
            }
        }

        for (int i = 0; i < column_to_keep; ++i) {
            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(i);
            auto& column = column_with_type_and_name.column;
            const IColumn* actual_column = column.get();
            if (is_column<ColumnNullable>(*actual_column)) {
                const ColumnNullable& nullable_column = assert_cast<const ColumnNullable&>(*actual_column);
                actual_column = nullable_column.get_nested_column_ptr().get();
            }
            if (is_column<DictionaryColumn<int32_t>>(*actual_column)) {
                const DictionaryColumn<int32_t>& dict_column = assert_cast<const DictionaryColumn<int32_t>&>(*actual_column);
                auto string_column = ColumnString::create();
                for (int i = 0; i < dict_column.size(); ++i) {
                    Field field;
                    dict_column.get(i, field);
                    string_column->insert(Field(field.get<String>()));
                }

                column_with_type_and_name.column = std::move(string_column);
                column_with_type_and_name.type = std::make_shared<DataTypeString>();
            }
        }
        for (size_t i = 0; i < filter.size(); ++i) {
            fprintf(stdout, "filter[%zu] = %d\n", i, filter[i]);
        }
        RETURN_IF_CATCH_EXCEPTION(Block::filter_block_internal(
                block, columns_to_filter, filter));
        Block::erase_useless_column(block, column_to_keep);
        return Status::OK();
    }
};

//class DictionaryAwareBlockProjection : public BlockProjection {
//public:
//    Status project(Block* block, const std::vector<uint32_t>& columns_to_filter,
//                   const IColumn::Filter& filter, size_t column_to_keep) override {
//        // 遍历所有列，查找字典列
//        for (size_t col_idx = 0; col_idx < block->columns(); ++col_idx) {
//            ColumnWithTypeAndName& column_with_type_and_name = block->get_by_position(col_idx);
//            auto& column = column_with_type_and_name.column;
//            auto& type = column_with_type_and_name.type;
//
//            if (is_column<DictionaryColumn>(*column)) {
//                const DictionaryColumn& dict_column = assert_cast<const DictionaryColumn&>(*column);
//                auto dictionary = dict_column.get_dictionary();
//                const auto& ids = dict_column.get_ids();
//
//                // 创建新的字典列
//                auto new_dictionary = dictionary->clone_empty();
//                std::vector<int32_t> new_ids;
//
//                // 应用过滤
//                for (size_t i = 0; i < filter.size(); ++i) {
//                    if (filter[i]) {
//                        new_ids.push_back(ids[i]);
//                    }
//                }
//
//                // 替换原列
//                block->replace_by_position(col_idx, std::make_shared<DictionaryColumn<int32_t>>(
//                    0, new_ids.size(), new_dictionary, new_ids,
//                    dict_column.is_compacted(), dict_column.is_sequential_ids(),
//                    dict_column.get_dictionary_source_id()));
//            }
//        }
//
//        // 回退到普通投影
//        return DefaultBlockProjection().project(block, columns_to_filter, filter, column_to_keep);
//    }
//};

class BlockProcessor {
public:
    BlockProcessor(std::shared_ptr<BlockFilter> block_filter, std::shared_ptr<BlockProjection> projection)
            : _block_filter(std::move(block_filter)), _projection(std::move(projection)) {}

    Result<Block> process(Block&& block) {
        if (block.rows() == 0) {
            return block;
        }

        size_t orig_column_size = block.columns();
        Result<std::shared_ptr<IColumn::Filter>> filter_result = _block_filter->filter(&block);
        if (!filter_result.has_value()) {
            return unexpected(filter_result.error());
        }

        std::vector<uint32_t> columns_to_filter;
        int column_to_keep = block.columns();
        columns_to_filter.resize(column_to_keep);
        for (uint32_t i = 0; i < column_to_keep; ++i) {
            columns_to_filter[i] = i;
        }
        RETURN_IF_ERROR_RESULT(_projection->project(&block, columns_to_filter, *filter_result.value(), orig_column_size));
        return block;
    }

private:
    std::shared_ptr<BlockFilter> _block_filter;
    std::shared_ptr<BlockProjection> _projection;
};

} // namespace doris::vectorized