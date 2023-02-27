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
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }
    // TODO: Push conjuncts down to reader.
    _build_filter_conjuncts();
    return Status::OK();
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
                range.to_filter_conjuncts(filter_conjuncts, _output_tuple_desc->slots()[iter.first], _state);
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
    ((VFileScanner*)scanner)->prepare(_vconjunct_ctx_ptr.get(), &_colname_to_value_range);
    _scanner_pool.add(scanner);
    return scanner;
}

}; // namespace doris::vectorized
