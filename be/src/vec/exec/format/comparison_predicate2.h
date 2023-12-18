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

#include <cstdint>
#include <type_traits>

#include "olap/column_predicate.h"
#include "olap/wrapper_field.h"

namespace doris {

template <PrimitiveType Type, PredicateType PT>
class ComparisonPredicateBase2 : public ColumnPredicate {
public:
    using T = typename PrimitiveTypeTraits<Type>::CppType;
    ComparisonPredicateBase2(uint32_t column_id, const T& value, bool opposite = false)
            : ColumnPredicate(column_id, opposite),
              _cached_code(_InvalidateCodeValue),
              _value(value) {}

    void clone(ColumnPredicate** to) const override {
        auto* cloned = new ComparisonPredicateBase2(_column_id, _value, _opposite);
        cloned->predicate_params()->value = _predicate_params->value;
        cloned->_cache_code_enabled = true;
        cloned->predicate_params()->marked_by_runtime_filter =
                _predicate_params->marked_by_runtime_filter;
        *to = cloned;
    }

    bool can_do_apply_safely(PrimitiveType input_type, bool is_null) const override {
        return input_type == Type || (is_string_type(input_type) && is_string_type(Type));
    }

    bool need_to_clone() const override { return true; }

    PredicateType type() const override { return PT; }

    virtual Status evaluate(BitmapIndexIterator* iterator, uint32_t num_rows,
                            roaring::Roaring* roaring) const override {
        return Status::OK();
    }

            template <bool is_and>
    __attribute__((flatten)) void _evaluate_vec_internal(const vectorized::IColumn& column,
                                                         uint16_t size, bool* flags) const {
        if (column.is_nullable()) {
            auto* nullable_column_ptr =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(column);
            auto& nested_column = nullable_column_ptr->get_nested_column();
            auto& null_map = reinterpret_cast<const vectorized::ColumnUInt8&>(
                                     nullable_column_ptr->get_null_map_column())
                                     .get_data();
                auto* data_array =
                        vectorized::check_and_get_column<
                                const vectorized::ColumnVector<int>>(
                                nested_column)
                                ->get_data()
                                .data();

                _base_loop_vec<true, is_and>(size, flags, null_map.data(), data_array, _value);
        } else {
                auto* data_array =
                        vectorized::check_and_get_column<
                                vectorized::ColumnVector<int>>(
                                column)
                                ->get_data()
                                .data();

                _base_loop_vec<false, is_and>(size, flags, nullptr, data_array, _value);
        }

        if (_opposite) {
            for (uint16_t i = 0; i < size; i++) {
                flags[i] = !flags[i];
            }
        }
    }

    void evaluate_vec(const vectorized::IColumn& column, uint16_t size,
                      bool* flags) const override {
        _evaluate_vec_internal<false>(column, size, flags);
    }

    void evaluate_and_vec(const vectorized::IColumn& column, uint16_t size,
                          bool* flags) const override {
        _evaluate_vec_internal<true>(column, size, flags);
    }

private:
    template <typename LeftT, typename RightT>
    bool _operator(const LeftT& lhs, const RightT& rhs) const {
        if constexpr (PT == PredicateType::EQ) {
            return lhs == rhs;
        } else if constexpr (PT == PredicateType::NE) {
            return lhs != rhs;
        } else if constexpr (PT == PredicateType::LT) {
            return lhs < rhs;
        } else if constexpr (PT == PredicateType::LE) {
            return lhs <= rhs;
        } else if constexpr (PT == PredicateType::GT) {
            return lhs > rhs;
        } else if constexpr (PT == PredicateType::GE) {
            return lhs >= rhs;
        }
    }

    constexpr bool _is_range() const { return PredicateTypeTraits::is_range(PT); }

    constexpr bool _is_greater() const { return _operator(1, 0); }

    constexpr bool _is_eq() const { return _operator(1, 1); }


    template <bool is_nullable, bool is_and, typename TArray, typename TValue>
    __attribute__((flatten)) void _base_loop_vec(uint16_t size, bool* __restrict bflags,
                                                 const uint8_t* __restrict null_map,
                                                 const TArray* __restrict data_array,
                                                 const TValue& value) const {
        //uint8_t helps compiler to generate vectorized code
        uint8_t* flags = reinterpret_cast<uint8_t*>(bflags);
        if constexpr (is_and) {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] &= (uint8_t)(!null_map[i] && _operator(data_array[i], value));
                } else {
                    flags[i] &= (uint8_t)_operator(data_array[i], value);
                }
            }
        } else {
            for (uint16_t i = 0; i < size; i++) {
                if constexpr (is_nullable) {
                    flags[i] = !null_map[i] && _operator(data_array[i], value);
                } else {
                    flags[i] = _operator(data_array[i], value);
                }
            }
        }
    }

    std::string _debug_string() const override {
        std::string info =
                "ComparisonPredicateBase2(" + type_to_string(Type) + ", " + type_to_string(PT) + ")";
        return info;
    }

    static constexpr int32_t _InvalidateCodeValue = std::numeric_limits<int32_t>::max();
    mutable int32_t _cached_code;
    bool _cache_code_enabled = false;
    T _value;
};

} //namespace doris
