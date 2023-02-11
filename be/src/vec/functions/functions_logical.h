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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsLogical.h
// and modified by Doris

#pragma once

#include <type_traits>

#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function.h"

/** Logical functions AND, OR, XOR and NOT support three-valued (or ternary) logic
  * https://en.wikibooks.org/wiki/Structured_Query_Language/NULLs_and_the_Three_Valued_Logic
  *
  * Functions XOR and NOT rely on "default implementation for NULLs":
  *   - if any of the arguments is of Nullable type, the return value type is Nullable
  *   - if any of the arguments is NULL, the return value is NULL
  *
  * Functions AND and OR provide their own special implementations for ternary logic
  */

namespace doris::vectorized {
namespace FunctionsLogicalDetail {
namespace Ternary {
using ResultType = UInt8;

/** These values are carefully picked so that they could be efficiently evaluated with bitwise operations, which
      * are feasible for auto-vectorization by the compiler. The expression for the ternary value evaluation writes:
      *
      * ternary_value = ((value << 1) | is_null) & (1 << !is_null)
      *
      * The truth table of the above formula lists:
      *  +---------------+--------------+-------------+
      *  | is_null\value |      0       |      1      |
      *  +---------------+--------------+-------------+
      *  |             0 | 0b00 (False) | 0b10 (True) |
      *  |             1 | 0b01 (Null)  | 0b01 (Null) |
      *  +---------------+--------------+-------------+
      *
      * As the numerical values of False, Null and True are assigned in ascending order, the "and" and "or" of
      * ternary logic could be implemented with minimum and maximum respectively, which are also vectorizable.
      * https://en.wikipedia.org/wiki/Three-valued_logic
      *
      * This logic does not apply for "not" and "xor" - they work with default implementation for NULLs:
      *  anything with NULL returns NULL, otherwise use conventional two-valued logic.
      */
static constexpr UInt8 False = 0;   /// 0b00
static constexpr UInt8 Null = 1;    /// 0b01
static constexpr UInt8 True = 2;   /// 0b10

template <typename T>
inline ResultType make_value(T value) {
    return value != 0 ? Ternary::True : Ternary::False;
}

template <typename T>
inline ResultType make_value(T value, bool is_null) {
    if (is_null) return Ternary::Null;
    return make_value<T>(value);
}
} // namespace Ternary

struct AndImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return true; }
    /// Final value in two-valued logic (no further operations with True, False will change this value)
    static inline constexpr bool is_saturated_value(bool a) { return !a; }
    /// Final value in three-valued logic (no further operations with True, False, Null will change this value)
    static inline constexpr bool is_saturated_value_ternary(UInt8 a) { return a == Ternary::False; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a & b; }
    static inline constexpr ResultType ternary_apply(UInt8 a, UInt8 b) { return std::min(a, b); }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

struct OrImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return true; }
    static inline constexpr bool is_saturated_value(bool a) { return a; }
    static inline constexpr bool is_saturated_value_ternary(UInt8 a) { return a == Ternary::True; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a | b; }
    static inline constexpr ResultType ternary_apply(UInt8 a, UInt8 b) { return std::max(a, b); }
    static inline constexpr bool special_implementation_for_nulls() { return true; }
};

struct XorImpl {
    using ResultType = UInt8;

    static inline constexpr bool is_saturable() { return false; }
    static inline constexpr bool is_saturated_value(bool) { return false; }
    static inline constexpr bool is_saturated_value_ternary(UInt8) { return false; }
    static inline constexpr ResultType apply(UInt8 a, UInt8 b) { return a != b; }
    static inline constexpr ResultType ternary_apply(UInt8 a, UInt8 b) { return a != b; }
    static inline constexpr bool special_implementation_for_nulls() { return false; }
};

template <typename A>
struct NotImpl {
    using ResultType = UInt8;

    static inline ResultType apply(A a) { return !a; }
};

template <typename Impl, typename Name>
class FunctionAnyArityLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    //    static FunctionPtr create(const Context &) { return std::make_shared<FunctionAnyArityLogical>(); }
    static FunctionPtr create() { return std::make_shared<FunctionAnyArityLogical>(); }

public:
    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }
    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_nulls() const override {
        return !Impl::special_implementation_for_nulls();
    }

    /// Get result types by argument types. If the function does not apply to these arguments, throw an exception.
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override;
};

template <template <typename> class Impl, typename Name>
class FunctionUnaryLogical : public IFunction {
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create() { return std::make_shared<FunctionUnaryLogical>(); }

public:
    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override;

    bool use_default_implementation_for_constants() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) override;
};

} // namespace FunctionsLogicalDetail

struct NameAnd {
    static constexpr auto name = "and";
};
struct NameOr {
    static constexpr auto name = "or";
};
struct NameXor {
    static constexpr auto name = "xor";
};
struct NameNot {
    static constexpr auto name = "not";
};

using FunctionAnd =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::AndImpl, NameAnd>;
using FunctionOr =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::OrImpl, NameOr>;
using FunctionXor =
        FunctionsLogicalDetail::FunctionAnyArityLogical<FunctionsLogicalDetail::XorImpl, NameXor>;
using FunctionNot =
        FunctionsLogicalDetail::FunctionUnaryLogical<FunctionsLogicalDetail::NotImpl, NameNot>;
} // namespace doris::vectorized
