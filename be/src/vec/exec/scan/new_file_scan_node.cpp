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

#include "vec/exec/scan/new_file_scan_node.h"

#include "vec/exec/scan/vfile_scanner.h"
#include "vec/exprs/vectorized_fn_call.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris::vectorized {

NewFileScanNode::NewFileScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs) {
    _output_tuple_id = tnode.file_scan_node.tuple_id;
}

Status NewFileScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));
    return Status::OK();
}

Status NewFileScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::prepare(state));
    return Status::OK();
}

void NewFileScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    int max_scanners = config::doris_scanner_thread_pool_thread_num;
    if (scan_ranges.size() <= max_scanners) {
        _scan_ranges = scan_ranges;
    } else {
        // There is no need for the number of scanners to exceed the number of threads in thread pool.
        _scan_ranges.clear();
        auto range_iter = scan_ranges.begin();
        for (int i = 0; i < max_scanners && range_iter != scan_ranges.end(); ++i, ++range_iter) {
            _scan_ranges.push_back(*range_iter);
        }
        for (int i = 0; range_iter != scan_ranges.end(); ++i, ++range_iter) {
            if (i == max_scanners) {
                i = 0;
            }
            auto& ranges = _scan_ranges[i].scan_range.ext_scan_range.file_scan_range.ranges;
            auto& merged_ranges = range_iter->scan_range.ext_scan_range.file_scan_range.ranges;
            ranges.insert(ranges.end(), merged_ranges.begin(), merged_ranges.end());
        }
        _scan_ranges.shrink_to_fit();
        LOG(INFO) << "Merge " << scan_ranges.size() << " scan ranges to " << _scan_ranges.size();
    }
    if (scan_ranges.size() > 0) {
        _input_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.src_tuple_id;
        _output_tuple_id =
                scan_ranges[0].scan_range.ext_scan_range.file_scan_range.params.dest_tuple_id;
    }
}

Status NewFileScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

//Status NewFileScanNode::_process_conjuncts() {
//    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
//    if (_eos) {
//        return Status::OK();
//    }
//    // TODO: Push conjuncts down to reader.
//    return Status::OK();
//}

Status NewFileScanNode::_process_conjuncts() {
//    RETURN_IF_ERROR(_split_conjuncts());
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
//    _build_filter_conjuncts();
    return Status::OK();
}

Status NewFileScanNode::_split_conjuncts() {
    if (_vconjunct_ctx_ptr) {
        if ((*_vconjunct_ctx_ptr)->root()) {
            RETURN_IF_ERROR(_split_conjuncts_internal((*_vconjunct_ctx_ptr)->root()));
        }
    }

    return Status::OK();
}

Status NewFileScanNode::_split_conjuncts_internal(VExpr* conjunct_expr_root) {
    static constexpr auto is_leaf = [](VExpr* expr) { return !expr->is_and_expr(); };
//    auto in_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
//                                   VExpr** child_contains_slot) {
//        if (children.empty() ||
//            VExpr::expr_without_cast(children[0])->node_type() != TExprNodeType::SLOT_REF) {
//            // not a slot ref(column)
//            return false;
//        }
//        *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(children[0]));
//        *child_contains_slot = children[0];
//        return true;
//    };
//    auto eq_predicate_checker = [](const std::vector<VExpr*>& children, const VSlotRef** slot,
//                                   VExpr** child_contains_slot) {
//        for (const VExpr* child : children) {
//            if (VExpr::expr_without_cast(child)->node_type() != TExprNodeType::SLOT_REF) {
//                // not a slot ref(column)
//                continue;
//            }
//            *slot = reinterpret_cast<const VSlotRef*>(VExpr::expr_without_cast(child));
//            *child_contains_slot = const_cast<VExpr*>(child);
//            return true;
//        }
//        return false;
//    };

    if (conjunct_expr_root != nullptr) {
        if (is_leaf(conjunct_expr_root)) {
            auto impl = conjunct_expr_root->get_impl();
            // If impl is not null, which means this a conjuncts from runtime filter.
            VExpr* cur_expr = impl ? const_cast<VExpr*>(impl) : conjunct_expr_root;
//            bool is_runtimer_filter_predicate =
//                    _rf_vexpr_set.find(conjunct_expr_root) != _rf_vexpr_set.end();
            std::vector<int> slot_ids;
            _get_slot_ids(cur_expr, &slot_ids);
            bool single_slot = true;
            for (int i = 1; i < slot_ids.size(); i++) {
                if (slot_ids[i] != slot_ids[0]) {
                    single_slot = false;
                    break;
                }
            }
            VExprContext* new_ctx = _state->obj_pool()->add(new VExprContext(cur_expr->clone(_state->obj_pool())));
            (*_vconjunct_ctx_ptr)->clone_fn_contexts(new_ctx);
            RETURN_IF_ERROR(new_ctx->prepare(_state, _row_descriptor));
            RETURN_IF_ERROR(new_ctx->open(_state));
            if (single_slot) {
                SlotId slot_id = slot_ids[0];
                if (_slot_id_to_filter_conjuncts.find(slot_id) == _slot_id_to_filter_conjuncts.end()) {
                    _slot_id_to_filter_conjuncts.insert({slot_id, std::vector<VExprContext*>()});
                }
                _slot_id_to_filter_conjuncts[slot_id].emplace_back(new_ctx);
            } else {
                _other_filter_conjuncts.emplace_back(new_ctx);
            }
        } else {
            RETURN_IF_ERROR(_split_conjuncts_internal(conjunct_expr_root->children()[0]));
            RETURN_IF_ERROR(_split_conjuncts_internal(conjunct_expr_root->children()[1]));
        }
    }
    return Status::OK();
}

void NewFileScanNode::_get_slot_ids(VExpr* expr, std::vector<int> *slot_ids) {
    for (VExpr* child_expr : expr->children()) {
        if (child_expr->is_slot_ref()) {
            VSlotRef* slot_ref = reinterpret_cast<VSlotRef*>(child_expr);
            slot_ids->emplace_back(slot_ref->slot_id());
        }
        _get_slot_ids(child_expr, slot_ids);
    }
}

Status NewFileScanNode::_build_filter_conjuncts() {
//    if (!_olap_scan_node.__isset.push_down_agg_type_opt ||
//        _olap_scan_node.push_down_agg_type_opt == TPushAggOp::NONE) {
//        const std::vector<std::string>& column_names = _olap_scan_node.key_column_name;
//        const std::vector<TPrimitiveType::type>& column_types = _olap_scan_node.key_column_type;
//        DCHECK(column_types.size() == column_names.size());

        for (auto& iter : _slot_id_to_value_range) {
            std::vector<vectorized::VExprContext* > filter_conjuncts;
            std::visit([&](auto&& range) {
                to_filter_conjuncts(range, filter_conjuncts, _output_tuple_desc->slots()[iter.first], _state);
                _colname_to_slot_id[range.column_name()] = iter.first;
            }, iter.second.second);
            _slot_id_to_filter_conjuncts.insert({iter.first, filter_conjuncts});
        }

//        for (auto& iter : _compound_value_ranges) {
//            std::vector<TCondition> filters;
//            std::visit(
//                    [&](auto&& range) {
//                        if (range.is_in_compound_value_range()) {
//                            range.to_condition_in_compound(filters);
//                        } else if (range.is_match_value_range()) {
//                            range.to_match_condition(filters);
//                        }
//                    },
//                    iter);
//            for (const auto& filter : filters) {
//                _compound_filters.push_back(filter);
//            }
//        }

        // Append value ranges in "_not_in_value_ranges"
//        for (auto& range : _not_in_value_ranges) {
//            std::visit([&](auto&& the_range) { the_range.to_in_condition(_olap_filters, false); },
//                       range);
//        }
//    } else {
//        _runtime_profile->add_info_string(
//                "PushDownAggregate",
//                push_down_agg_to_string(_olap_scan_node.push_down_agg_type_opt));
//    }

//    if (_state->enable_profile()) {
//        _runtime_profile->add_info_string("PushDownPredicates",
//                                          olap_filters_to_string(_olap_filters));
//    }

    return Status::OK();
}

template<PrimitiveType primitive_type>
void NewFileScanNode::to_filter_conjuncts(ColumnValueRange<primitive_type>& column_value_range,
                         std::vector<vectorized::VExprContext* >& filter_conjuncts,
                         const SlotDescriptor *slot_desc,
                         const RuntimeState* state) {
    if (column_value_range.is_fixed_value_range() || column_value_range.is_match_value_range()) {
            // 1. convert to in filter condition
            if (column_value_range.get_fixed_value_set().size() == 0) {
                to_eq_filter_conjunct<primitive_type>(column_value_range, filter_conjuncts, slot_desc, state, true);
            } else if (column_value_range.get_fixed_value_set().size() > 1) {
//                to_in_filter_conjunct(filter_conjuncts, slot_desc, state, true);
            }
            //            if (is_match_value_range()) {
            //                to_match_filter_conjunct(filter_conjuncts);
            //            }
    } else if (column_value_range.get_range_min_value() < column_value_range.get_range_max_value()) {
            // 2. convert to min max filter condition
            //            TCondition null_pred;
            //            if (TYPE_MAX == _high_value && _high_op == FILTER_LESS_OR_EQUAL &&
            //                TYPE_MIN == _low_value && _low_op == FILTER_LARGER_OR_EQUAL && !contain_null()) {
            //                null_pred.__set_column_name(_column_name);
            //                null_pred.__set_condition_op("is");
            //                null_pred.condition_values.emplace_back("not null");
            //            }
            //
            //            if (null_pred.condition_values.size() != 0) {
            //                filter_conjuncts.push_back(null_pred);
            //                return;
            //            }
            //
            //            TCondition low;
            //            if (TYPE_MIN != _low_value || FILTER_LARGER_OR_EQUAL != _low_op) {
            //                low.__set_column_name(_column_name);
            //                low.__set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? ">=" : ">>"));
            //                low.__set_marked_by_runtime_filter(_marked_runtime_filter_predicate);
            //                low.condition_values.push_back(
            //                        cast_to_string<primitive_type, CppType>(_low_value, _scale));
            //            }
            //
            //            if (low.condition_values.size() != 0) {
            //                filters.push_back(low);
            //            }
            //
            //            TCondition high;
            //            if (TYPE_MAX != _high_value || FILTER_LESS_OR_EQUAL != _high_op) {
            //                high.__set_column_name(_column_name);
            //                high.__set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? "<=" : "<<"));
            //                high.__set_marked_by_runtime_filter(_marked_runtime_filter_predicate);
            //                high.condition_values.push_back(
            //                        cast_to_string<primitive_type, CppType>(_high_value, _scale));
            //            }
            //
            //            if (high.condition_values.size() != 0) {
            //                filters.push_back(high);
            //            }
    } else {
            // 3. convert to is null and is not null filter condition
            //            TCondition null_pred;
            //            if (TYPE_MAX == _low_value && TYPE_MIN == _high_value && contain_null()) {
            //                null_pred.__set_column_name(_column_name);
            //                null_pred.__set_condition_op("is");
            //                null_pred.condition_values.emplace_back("null");
            //            }
            //
            //            if (null_pred.condition_values.size() != 0) {
            //                filters.push_back(null_pred);
            //            }
    }
}

template<PrimitiveType primitive_type>
void NewFileScanNode::to_eq_filter_conjunct(ColumnValueRange<primitive_type>& column_value_range,
        std::vector<vectorized::VExprContext *>& filter_conjuncts,
                           const SlotDescriptor *slot_desc,
                           const RuntimeState* state,
                           bool is_in) {
    vectorized::VExpr* root;
    {
            TFunction fn;
            TFunctionName fn_name;
            fn_name.__set_db_name("");
            fn_name.__set_function_name("eq");
            fn.__set_name(fn_name);
            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
            std::vector<TTypeDesc> arg_types;
            arg_types.push_back(create_type_desc(primitive_type));
            arg_types.push_back(create_type_desc(primitive_type));
            fn.__set_arg_types(arg_types);
            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            fn.__set_has_var_args(false);

            TExprNode texpr_node;
            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            texpr_node.__set_opcode(TExprOpcode::EQ);
            texpr_node.__set_vector_opcode(TExprOpcode::EQ);
            texpr_node.__set_fn(fn);
            texpr_node.__set_is_nullable(true);
            texpr_node.__set_child_type(TPrimitiveType::INT);
            texpr_node.__set_num_children(2);
            root = state->obj_pool()->add(new VectorizedFnCall(texpr_node));
    }
    {
            //            TExprNode texpr_node;
            //            texpr_node.__set_node_type(TExprNodeType::SLOT_REF);
            //            texpr_node.__set_type(create_type_desc(TYPE_INT));
            //            texpr_node.__set_num_children(0);
            //            texpr_node.__isset.slot_ref = true;
            //            TSlotRef slot_ref;
            //            slot_ref.__set_slot_id(0);
            //            slot_ref.__set_tuple_id(0);
            //            texpr_node.__set_slot_ref(slot_ref);
            //            texpr_node.__isset.output_column = true;
            //            texpr_node.__set_output_column(0);
            //            VExpr* slot_ref_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
            //                VExpr* slot_ref_expr =  _lazy_read_ctx.vconjunct_ctx->root()[1].clone(_state->obj_pool());
            vectorized::VExpr* slot_ref_expr = new VSlotRef(slot_desc);
            root->add_child(slot_ref_expr);
    }

    {
            auto* literal_expr = create_literal<primitive_type, typename ColumnValueRange<primitive_type>::CppType>(*column_value_range.get_fixed_value_set().begin(),
                                                                column_value_range.precision(), column_value_range.scale());
            root->add_child(literal_expr);
    }

    filter_conjuncts.emplace_back();
}

//void NewFileScanNode::to_in_filter_conjunct(std::vector<vectorized::VExprContext *>& filter_conjuncts,
//                           const RuntimeState* state,
//                           bool is_in) {
    //        vectorized::VExpr* root;
    //        {
    //            TFunction fn;
    //            TFunctionName fn_name;
    //            fn_name.__set_db_name("");
    //            fn_name.__set_function_name("eq");
    //            fn.__set_name(fn_name);
    //            fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    //            std::vector<TTypeDesc> arg_types;
    //            arg_types.push_back(create_type_desc(primitive_type));
    //            arg_types.push_back(create_type_desc(primitive_type));
    //            fn.__set_arg_types(arg_types);
    //            fn.__set_ret_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    //            fn.__set_has_var_args(false);
    //
    //            TExprNode texpr_node;
    //            texpr_node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
    //            texpr_node.__set_node_type(TExprNodeType::BINARY_PRED);
    //            texpr_node.__set_opcode(TExprOpcode::EQ);
    //            texpr_node.__set_vector_opcode(TExprOpcode::EQ);
    //            texpr_node.__set_fn(fn);
    //            texpr_node.__set_is_nullable(true);
    //            texpr_node.__set_child_type(TPrimitiveType::INT);
    //            texpr_node.__set_num_children(2);
    //            root = _state->obj_pool()->add(new VectorizedFnCall(texpr_node));
    //        }
    //        {
    //            //            TExprNode texpr_node;
    //            //            texpr_node.__set_node_type(TExprNodeType::SLOT_REF);
    //            //            texpr_node.__set_type(create_type_desc(TYPE_INT));
    //            //            texpr_node.__set_num_children(0);
    //            //            texpr_node.__isset.slot_ref = true;
    //            //            TSlotRef slot_ref;
    //            //            slot_ref.__set_slot_id(0);
    //            //            slot_ref.__set_tuple_id(0);
    //            //            texpr_node.__set_slot_ref(slot_ref);
    //            //            texpr_node.__isset.output_column = true;
    //            //            texpr_node.__set_output_column(0);
    //            //            VExpr* slot_ref_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
    //            //                VExpr* slot_ref_expr =  _lazy_read_ctx.vconjunct_ctx->root()[1].clone(_state->obj_pool());
    //            int pos = 0;
    //            for (SlotDescriptor* slot : _tuple_descriptor->slots()) {
    //                if (slot->col_name() == "s_region") {
    //                    break;
    //                }
    //                ++pos;
    //            }
    //            vectorized::VExpr* slot_ref_expr = new vectorized::VSlotRef(_tuple_descriptor->slots()[pos]);
    //            root->add_child(slot_ref_expr);
    //        }
    //
    //        {
    //            TExprNode texpr_node;
    //            texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
    //            texpr_node.__set_type(create_type_desc(TYPE_INT));
    //            TIntLiteral int_literal;
    //            int_literal.__set_value(dict_codes[0]);
    //            texpr_node.__set_int_literal(int_literal);
    //            vectorized::VExpr* literal_expr = _state->obj_pool()->add(new vectorized::VLiteral(texpr_node));
    //            root->add_child(literal_expr);
    //        }
//}

template <PrimitiveType primitive_type, class T>
vectorized::VExpr* NewFileScanNode::create_literal(T value, int precision, int scale) {
    if constexpr (primitive_type == TYPE_DECIMAL32) {
//            TExprNode texpr_node;
//            texpr_node.__set_node_type(TExprNodeType::DECIMAL_LITERAL);
//            texpr_node.__set_type(create_type_desc(primitive_type, precision, scale));
//            TDecimalLiteral literal;
//            literal.__set_value(value);
//            texpr_node.__set_decimal_literal(literal);
//            vectorized::VExpr* literal_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
//            return literal_expr;
    } else if constexpr (primitive_type == TYPE_DECIMAL64) {
    } else if constexpr (primitive_type == TYPE_DECIMAL128I) {
    } else if constexpr (primitive_type == TYPE_TINYINT) {
//            TExprNode texpr_node;
//            texpr_node.__set_node_type(TExprNodeType::INT_LITERAL);
//            texpr_node.__set_type(create_type_desc(primitive_type));
//            TIntLiteral literal;
//            literal.__set_value(value);
//            texpr_node.__set_int_literal(literal);
//            vectorized::VExpr* literal_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
//            return literal_expr;
    } else if constexpr (primitive_type == TYPE_LARGEINT) {
//            TExprNode texpr_node;
//            texpr_node.__set_node_type(TExprNodeType::LARGE_INT_LITERAL);
//            texpr_node.__set_type(create_type_desc(primitive_type));
//            TLargeIntLiteral literal;
//            literal.__set_value(value);
//            texpr_node.__set_large_int_literal(literal);
//            vectorized::VExpr* literal_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
//            return literal_expr;
    } else if constexpr (primitive_type == TYPE_VARCHAR) {
            TExprNode texpr_node;
            texpr_node.__set_node_type(TExprNodeType::STRING_LITERAL);
            texpr_node.__set_type(create_type_desc(primitive_type));
            TStringLiteral literal;
            literal.__set_value(value.to_string());
            texpr_node.__set_string_literal(literal);
            vectorized::VExpr* literal_expr = _state->obj_pool()->add(new VLiteral(texpr_node));
            return literal_expr;
    } else {
    }
    return nullptr;
}

Status NewFileScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        return Status::OK();
    }

    for (auto& scan_range : _scan_ranges) {
        VScanner* scanner =
                (VScanner*)_create_scanner(scan_range.scan_range.ext_scan_range.file_scan_range);
        scanners->push_back(scanner);
    }

    return Status::OK();
}

VScanner* NewFileScanNode::_create_scanner(const TFileScanRange& scan_range) {
    VScanner* scanner = new VFileScanner(_state, this, _limit_per_scanner, scan_range,
                                         runtime_profile(), _kv_cache);
    ((VFileScanner*)scanner)->prepare(_vconjunct_ctx_ptr.get(),
                                      &_colname_to_value_range, &_colname_to_slot_id);
    _scanner_pool.add(scanner);
    return scanner;
}

}; // namespace doris::vectorized
