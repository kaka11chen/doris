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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionsLogical.cpp
// and modified by Doris

#include "vec/functions/functions_logical.h"

#include <algorithm>

#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/field_visitors.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

namespace {
using namespace FunctionsLogicalDetail;

using UInt8Container = ColumnUInt8::Container;
using UInt8ColumnPtrs = std::vector<const ColumnUInt8*>;

//MutableColumnPtr convert_from_ternary_data(const UInt8Container& ternary_data,
//                                           const bool make_nullable) {
//    const size_t rows_count = ternary_data.size();
//
//    auto new_column = ColumnUInt8::create(rows_count);
//    std::transform(ternary_data.cbegin(), ternary_data.cend(), new_column->get_data().begin(),
//                   [](const auto x) { return x == Ternary::True; });
//
//    if (!make_nullable) return new_column;
//
//    auto null_column = ColumnUInt8::create(rows_count);
//    std::transform(ternary_data.cbegin(), ternary_data.cend(), null_column->get_data().begin(),
//                   [](const auto x) { return x == Ternary::Null; });
//
//    return ColumnNullable::create(std::move(new_column), std::move(null_column));
//}

//MutableColumnPtr build_column_from_ternary_data(const UInt8Container & ternary_data, const bool make_nullable)
//{
//    const size_t rows_count = ternary_data.size();
//
//    auto new_column = ColumnUInt8::create(rows_count);
//    for (size_t i = 0; i < rows_count; ++i)
//        new_column->get_data()[i] = (ternary_data[i] == Ternary::True);
//
//    if (!make_nullable)
//        return new_column;
//
//    auto null_column = ColumnUInt8::create(rows_count);
//    for (size_t i = 0; i < rows_count; ++i)
//        null_column->get_data()[i] = (ternary_data[i] == Ternary::Null);
//
//    return ColumnNullable::create(std::move(new_column), std::move(null_column));
//}

MutableColumnPtr build_column_from_ternary_data(const UInt8Container & ternary_data, const bool make_nullable)
{
    const size_t rows_count = ternary_data.size();

    auto new_column = ColumnUInt8::create(rows_count);
    for (size_t i = 0; i < rows_count; ++i)
        new_column->get_data()[i] = (ternary_data[i] == 1);

    if (!make_nullable)
        return new_column;

    auto null_column = ColumnUInt8::create(rows_count);
    for (size_t i = 0; i < rows_count; ++i)
        null_column->get_data()[i] = false;

    return ColumnNullable::create(std::move(new_column), std::move(null_column));
}

MutableColumnPtr build_column_from_ternary_data2(const ColumnUInt8::MutablePtr &column, const bool make_nullable)
{
    const size_t rows_count = column->size();

//    auto new_column = ColumnUInt8::create(rows_count);
//    for (size_t i = 0; i < rows_count; ++i)
//        new_column->get_data()[i] = (ternary_data[i] == 1);
//
    if (!make_nullable)
        return column->get_ptr();

    auto null_column = ColumnUInt8::create(rows_count);
    auto* __restrict null_column_data = null_column->get_data().data();
    for (size_t i = 0; i < rows_count; ++i)
        null_column_data[i] = false;

//    for (size_t i = 0; i < rows_count; ++i)
//        null_column->get_data()[i] = false;

    return ColumnNullable::create(column->get_ptr(), std::move(null_column));
}


//template <typename T>
//bool try_convert_column_to_uint8(const IColumn* column, UInt8Container& res) {
//    const auto col = check_and_get_column<ColumnVector<T>>(column);
//    if (!col) return false;
//
//    std::transform(col->get_data().cbegin(), col->get_data().cend(), res.begin(),
//                   [](const auto x) { return x != 0; });
//
//    return true;
//}

template <typename T>
bool try_convert_column_to_bool(const IColumn * column, UInt8Container & res)
{
    const auto column_typed = check_and_get_column<ColumnVector<T>>(column);
    if (!column_typed)
        return false;

    auto & data = column_typed->get_data();
    size_t data_size = data.size();
    for (size_t i = 0; i < data_size; ++i)
        res[i] = static_cast<bool>(data[i]);

    return true;
}

//void convert_column_to_uint8(const IColumn* column, UInt8Container& res) {
//    if (!try_convert_column_to_uint8<Int8>(column, res) &&
//        !try_convert_column_to_uint8<Int16>(column, res) &&
//        !try_convert_column_to_uint8<Int32>(column, res) &&
//        !try_convert_column_to_uint8<Int64>(column, res) &&
//        !try_convert_column_to_uint8<UInt16>(column, res) &&
//        !try_convert_column_to_uint8<UInt32>(column, res) &&
//        !try_convert_column_to_uint8<UInt64>(column, res) &&
//        !try_convert_column_to_uint8<Float32>(column, res) &&
//        !try_convert_column_to_uint8<Float64>(column, res))
//        LOG(FATAL) << "Unexpected type of column: " << column->get_name();
//}

void convert_any_column_to_bool(const IColumn * column, UInt8Container & res)
{
    if (!try_convert_column_to_bool<Int8>(column, res) &&
        !try_convert_column_to_bool<Int16>(column, res) &&
        !try_convert_column_to_bool<Int32>(column, res) &&
        !try_convert_column_to_bool<Int64>(column, res) &&
        !try_convert_column_to_bool<UInt16>(column, res) &&
        !try_convert_column_to_bool<UInt32>(column, res) &&
        !try_convert_column_to_bool<UInt64>(column, res) &&
        !try_convert_column_to_bool<Float32>(column, res) &&
        !try_convert_column_to_bool<Float64>(column, res))
        LOG(FATAL) << "Unexpected type of column: " << column->get_name();
}

//template <class Op, typename Func>
//static bool extract_const_columns(ColumnRawPtrs& in, UInt8& res, Func&& func) {
//    bool has_res = false;
//
//    for (int i = static_cast<int>(in.size()) - 1; i >= 0; --i) {
//        if (!is_column_const(*in[i])) continue;
//
//        UInt8 x = func((*in[i])[0]);
//        if (has_res) {
//            res = Op::apply(res, x);
//        } else {
//            res = x;
//            has_res = true;
//        }
//
//        in.erase(in.begin() + i);
//    }
//
//    return has_res;
//}

template <class Op, bool IsTernary, typename Func>
bool extract_const_columns(ColumnRawPtrs & in, UInt8 & res, Func && func)
{
    bool has_res = false;

    for (Int64 i = static_cast<Int64>(in.size()) - 1; i >= 0; --i)
    {
        UInt8 x;

//        if (in[i]->onlyNull())
//            x = func(Null());
//        else if (is_column_const(*in[i]))
//            x = func((*in[i])[0]);
//        else
//            continue;
        if (is_column_const(*in[i]))
            x = func((*in[i])[0]);
        else
            continue;

        if (has_res)
        {
            if constexpr (IsTernary)
                res = Op::ternary_apply(res, x);
            else
                res = Op::apply(res, x);
        }
        else
        {
            res = x;
            has_res = true;
        }

        in.erase(in.begin() + i);
    }

    return has_res;
}

//template <class Op>
//inline bool extract_const_columns(ColumnRawPtrs& in, UInt8& res) {
//    return extract_const_columns<Op>(in, res, [](const Field& value) {
//        return !value.is_null() && apply_visitor(FieldVisitorConvertToNumber<bool>(), value);
//    });
//}
//
//template <class Op>
//inline bool extract_const_columns_ternary(ColumnRawPtrs& in, UInt8& res_3v) {
//    return extract_const_columns<Op>(in, res_3v, [](const Field& value) {
//        return value.is_null() ? Ternary::make_value(false, true)
//                               : Ternary::make_value(
//                                         apply_visitor(FieldVisitorConvertToNumber<bool>(), value));
//    });
//}

template <class Op>
inline bool extract_const_columns_as_bool(ColumnRawPtrs & in, UInt8 & res)
{
    return extract_const_columns<Op, false>(
            in, res,
            [](const Field & value)
            {
                return !value.is_null() && apply_visitor(FieldVisitorConvertToNumber<bool>(), value);
            }
    );
}

template <class Op>
inline bool extract_const_columns_as_ternary(ColumnRawPtrs & in, UInt8 & res_3v)
{
    return extract_const_columns<Op, true>(
            in, res_3v,
            [](const Field & value)
            {
                return value.is_null()
                               ? Ternary::make_value(false, true)
                               : Ternary::make_value(apply_visitor(FieldVisitorConvertToNumber<bool>(), value));
            }
    );
}

//template <typename Op, size_t N>
//class AssociativeApplierImpl {
//    using ResultValueType = typename Op::ResultType;
//
//public:
//    /// Remembers the last N columns from `in`.
//    AssociativeApplierImpl(const UInt8ColumnPtrs& in)
//            : vec(in[in.size() - N]->get_data()), next(in) {}
//
//    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
//    ResultValueType apply(const size_t i) const {
//        const auto& a = vec[i];
//        if constexpr (Op::is_saturable())
//            return Op::is_saturated_value(a) ? a : Op::apply(a, next.apply(i));
//        else
//            return Op::apply(a, next.apply(i));
//    }
//
//private:
//    const UInt8Container& vec;
//    const AssociativeApplierImpl<Op, N - 1> next;
//};

//template <typename Op>
//class AssociativeApplierImpl<Op, 1> {
//    using ResultValueType = typename Op::ResultType;
//
//public:
//    AssociativeApplierImpl(const UInt8ColumnPtrs& in) : vec(in[in.size() - 1]->get_data()) {}
//
//    ResultValueType apply(const size_t i) const { return vec[i]; }
//
//private:
//    const UInt8Container& vec;
//};

/// N.B. This class calculates result only for non-nullable types
template <typename Op, size_t N>
class AssociativeApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeApplierImpl(const UInt8ColumnPtrs & in)
            : vec(in[in.size() - N]->get_data()), next(in) {}

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const size_t i) const
    {
        const auto a = !!vec[i];
        return Op::apply(a, next.apply(i));
    }

private:
    const UInt8Container & vec;
    const AssociativeApplierImpl<Op, N - 1> next;
};

template <typename Op>
class AssociativeApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    explicit AssociativeApplierImpl(const UInt8ColumnPtrs & in)
            : vec(in[in.size() - 1]->get_data()) {}

    inline ResultValueType apply(const size_t i) const { return !!vec[i]; }

private:
    const UInt8Container & vec;
};

/// A helper class used by AssociativeGenericApplierImpl
/// Allows for on-the-fly conversion of any data type into intermediate ternary representation
//using ValueGetter = std::function<Ternary::ResultType(size_t)>;
//
//template <typename... Types>
//struct ValueGetterBuilderImpl;
//
//template <typename Type, typename... Types>
//struct ValueGetterBuilderImpl<Type, Types...> {
//    static ValueGetter build(const IColumn* x) {
//        if (const auto nullable_column = typeid_cast<const ColumnNullable*>(x)) {
//            if (const auto nested_column = typeid_cast<const ColumnVector<Type>*>(
//                        nullable_column->get_nested_column_ptr().get())) {
//                return [&null_data = nullable_column->get_null_map_data(),
//                        &column_data = nested_column->get_data()](size_t i) {
//                    return Ternary::make_value(column_data[i], null_data[i]);
//                };
//            } else
//                return ValueGetterBuilderImpl<Types...>::build(x);
//        } else if (const auto column = typeid_cast<const ColumnVector<Type>*>(x))
//            return [&column_data = column->get_data()](size_t i) {
//                return Ternary::make_value(column_data[i]);
//            };
//        else
//            return ValueGetterBuilderImpl<Types...>::build(x);
//    }
//};
//
//template <>
//struct ValueGetterBuilderImpl<> {
//    [[noreturn]] static ValueGetter build(const IColumn* x) {
//        LOG(FATAL) << "Unknown numeric column of type: " << demangle(typeid(x).name());
//    }
//};
//
//using ValueGetterBuilder = ValueGetterBuilderImpl<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32,
//                                                  Int64, Float32, Float64>;

template <typename ... Types>
struct TernaryValueBuilderImpl;

template <typename Type, typename ...Types>
struct TernaryValueBuilderImpl<Type, Types...>
{
//    static void build(const IColumn * x, UInt8* __restrict ternary_column_data)
//    {
////        struct timespec startT, endT;
////        clock_gettime(CLOCK_MONOTONIC, &startT);
//        size_t size = x->size();
////        if (x->onlyNull())
////        {
////            memset(ternary_column_data, Ternary::Null, size);
////        }
//        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(x))
//        {
//            if (const auto * nested_column = typeid_cast<const ColumnVector<Type> *>(nullable_column->get_nested_column_ptr().get()))
//            {
//                const auto& null_data = nullable_column->get_null_map_data();
//                const auto& column_data = nested_column->get_data();
//
//                if constexpr (sizeof(Type) == 1)
//                {
//                    for (size_t i = 0; i < size; ++i)
//                    {
//                        auto has_value = static_cast<UInt8>(column_data[i] != 0);
//                        auto is_null = !!null_data[i];
//
//                        ternary_column_data[i] = ((has_value << 1) | is_null) & (1 << !is_null);
//                    }
//                }
//                else
//                {
//                    for (size_t i = 0; i < size; ++i)
//                    {
//                        auto has_value = static_cast<UInt8>(column_data[i] != 0);
//                        ternary_column_data[i] = has_value;
//                    }
//
//                    for (size_t i = 0; i < size; ++i)
//                    {
//                        auto has_value = ternary_column_data[i];
//                        auto is_null = !!null_data[i];
//
//                        ternary_column_data[i] = ((has_value << 1) | is_null) & (1 << !is_null);
//                    }
//                }
//            }
//            else
//                TernaryValueBuilderImpl<Types...>::build(x, ternary_column_data);
//        }
//        else if (const auto column = typeid_cast<const ColumnVector<Type> *>(x))
//        {
//            auto &column_data = column->get_data();
//
//            for (size_t i = 0; i < size; ++i)
//            {
//                ternary_column_data[i] = (column_data[i] != 0) << 1;
//            }
//        }
//        else
//            TernaryValueBuilderImpl<Types...>::build(x, ternary_column_data);
////        clock_gettime(CLOCK_MONOTONIC, &endT);
////        fprintf(stderr, "==> build %lu ns\n", (endT.tv_sec - startT.tv_sec) * 1000000000 + (endT.tv_nsec - startT.tv_nsec));
//    }
    static UInt8* __restrict build(const IColumn * x)
    {
        //        struct timespec startT, endT;
        //        clock_gettime(CLOCK_MONOTONIC, &startT);
//        size_t size = x->size();
        //        if (x->onlyNull())
        //        {
        //            memset(ternary_column_data, Ternary::Null, size);
        //        }
        if (const auto * nullable_column = typeid_cast<const ColumnNullable *>(x))
        {
            if (const auto * nested_column = typeid_cast<const ColumnVector<Type> *>(nullable_column->get_nested_column_ptr().get()))
            {
                MutableColumnPtr mutable_holder = nested_column->assume_mutable();
                ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
                auto &column_data = concrete_column->get_data();
                auto* __restrict column_raw_data = column_data.data();
                return column_raw_data;
            }
            else
                return TernaryValueBuilderImpl<Types...>::build(x);
        }
        else if (const auto column = typeid_cast<const ColumnVector<Type> *>(x))
        {
            MutableColumnPtr mutable_holder = column->assume_mutable();
            ColumnUInt8* concrete_column = typeid_cast<ColumnUInt8*>(mutable_holder.get());
            auto &column_data = concrete_column->get_data();
            auto* __restrict column_raw_data = column_data.data();
            return column_raw_data;
        }
        else
            return TernaryValueBuilderImpl<Types...>::build(x);
    }
};

template <>
struct TernaryValueBuilderImpl<>
{
    [[noreturn]] static UInt8* __restrict build(const IColumn * x)
    {
        LOG(FATAL) << "Unknown numeric column of type: " << demangle(typeid(x).name());
    }
};

using TernaryValueBuilder =
        TernaryValueBuilderImpl<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;

/// This class together with helper class ValueGetterBuilder can be used with columns of arbitrary data type
/// Allows for on-the-fly conversion of any type of data into intermediate ternary representation
/// and eliminates the need to materialize data columns in intermediate representation
//template <typename Op, size_t N>
//class AssociativeGenericApplierImpl {
//    using ResultValueType = typename Op::ResultType;
//
//public:
//    /// Remembers the last N columns from `in`.
//    AssociativeGenericApplierImpl(const ColumnRawPtrs& in)
//            : val_getter {ValueGetterBuilder::build(in[in.size() - N])}, next {in} {}
//
//    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
//    ResultValueType apply(const size_t i) const {
//        const auto a = val_getter(i);
//        if constexpr (Op::is_saturable())
//            return Op::is_saturated_value(a) ? a : Op::apply(a, next.apply(i));
//        else
//            return Op::apply(a, next.apply(i));
//    }
//
//private:
//    const ValueGetter val_getter;
//    const AssociativeGenericApplierImpl<Op, N - 1> next;
//};
//
//template <typename Op>
//class AssociativeGenericApplierImpl<Op, 1> {
//    using ResultValueType = typename Op::ResultType;
//
//public:
//    /// Remembers the last N columns from `in`.
//    AssociativeGenericApplierImpl(const ColumnRawPtrs& in)
//            : val_getter {ValueGetterBuilder::build(in[in.size() - 1])} {}
//
//    inline ResultValueType apply(const size_t i) const { return val_getter(i); }
//
//private:
//    const ValueGetter val_getter;
//};

template <typename Op, size_t N>
class AssociativeGenericApplierImpl
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
            : next{in}
    {
        vec = TernaryValueBuilder::build(in[in.size() - N]);
    }

    /// Returns a combination of values in the i-th row of all columns stored in the constructor.
    inline ResultValueType apply(const size_t i) const
    {
//        return Op::ternary_apply(vec[i], next.apply(i));
        return Op::apply(vec[i], next.apply(i));
    }

private:
    UInt8 *vec;
    const AssociativeGenericApplierImpl<Op, N - 1> next;
};


template <typename Op>
class AssociativeGenericApplierImpl<Op, 1>
{
    using ResultValueType = typename Op::ResultType;

public:
    /// Remembers the last N columns from `in`.
    explicit AssociativeGenericApplierImpl(const ColumnRawPtrs & in)
    {
//        struct timespec startT, endT;
//        clock_gettime(CLOCK_MONOTONIC, &startT);
        vec = TernaryValueBuilder::build(in[in.size() - 1]);
//        clock_gettime(CLOCK_MONOTONIC, &endT);
//        fprintf(stderr, "==> build %lu ns\n", (endT.tv_sec - startT.tv_sec) * 1000000000 + (endT.tv_nsec - startT.tv_nsec));
    }

    inline ResultValueType apply(const size_t i) const { return vec[i]; }

private:
    UInt8 *vec;
};

///// Apply target function by feeding it "batches" of N columns
///// Combining 10 columns per pass is the fastest for large block sizes.
///// For small block sizes - more columns is faster.
//template <typename Op, template <typename, size_t> typename OperationApplierImpl, size_t N = 10>
//struct OperationApplier {
//    template <typename Columns, typename ResultColumn>
//    static void apply(Columns& in, ResultColumn& result) {
//        while (in.size() > 1) {
//            do_batched_apply(in, result->get_data());
//            in.push_back(result.get());
//        }
//    }
//
//    template <typename Columns, typename ResultData>
//    static void NO_INLINE do_batched_apply(Columns& in, ResultData& result_data) {
//        if (N > in.size()) {
//            OperationApplier<Op, OperationApplierImpl, N - 1>::do_batched_apply(in, result_data);
//            return;
//        }
//
//        const OperationApplierImpl<Op, N> operationApplierImpl(in);
//        size_t i = 0;
//        for (auto& res : result_data) res = operationApplierImpl.apply(i++);
//
//        in.erase(in.end() - N, in.end());
//    }
//};
//
//template <typename Op, template <typename, size_t> typename OperationApplierImpl>
//struct OperationApplier<Op, OperationApplierImpl, 1> {
//    template <typename Columns, typename Result>
//    static void NO_INLINE do_batched_apply(Columns&, Result&) {
//        LOG(FATAL) << "OperationApplier<...>::apply(...): not enough arguments to run this method";
//    }
//};

/// Apply target function by feeding it "batches" of N columns
/// Combining 8 columns per pass is the fastest method, because it's the maximum when clang vectorizes a loop.
template <
        typename Op, template <typename, size_t> typename OperationApplierImpl, size_t N = 8>
struct OperationApplier
{
    template <typename Columns, typename ResultData>
    static void apply(Columns & in, ResultData & result_data, bool use_result_data_as_input = false)
    {
        if (!use_result_data_as_input)
            doBatchedApply<false>(in, result_data.data(), result_data.size());
        while (!in.empty())
            doBatchedApply<true>(in, result_data.data(), result_data.size());
    }

    template <bool CarryResult, typename Columns, typename Result>
    static void NO_INLINE doBatchedApply(Columns & in, Result * __restrict result_data, size_t size)
    {
        if (N > in.size())
        {
            OperationApplier<Op, OperationApplierImpl, N - 1>
                    ::template doBatchedApply<CarryResult>(in, result_data, size);
            return;
        }
//        struct timespec startT, endT;
//        clock_gettime(CLOCK_MONOTONIC, &startT);

        const OperationApplierImpl<Op, N> operation_applier_impl(in);
//        clock_gettime(CLOCK_MONOTONIC, &endT);
//        fprintf(stderr, "==> test %lu ns\n", (endT.tv_sec - startT.tv_sec) * 1000000000 + (endT.tv_nsec - startT.tv_nsec));

        for (size_t i = 0; i < size; ++i)
        {
            if constexpr (CarryResult)
            {
                if constexpr (std::is_same_v<OperationApplierImpl<Op, N>, AssociativeApplierImpl<Op, N>>) {
                    result_data[i] = Op::apply(result_data[i], operation_applier_impl.apply(i));
                }
                else {
                    result_data[i] =
                            Op::ternary_apply(result_data[i], operation_applier_impl.apply(i));
                }
            }
            else
                result_data[i] = operation_applier_impl.apply(i);
        }

        in.erase(in.end() - N, in.end());
    }
};

template <
        typename Op, template <typename, size_t> typename OperationApplierImpl>
struct OperationApplier<Op, OperationApplierImpl, 0>
{
    template <bool, typename Columns, typename Result>
    static void NO_INLINE doBatchedApply(Columns &, Result &, size_t)
    {
        LOG(FATAL) << "OperationApplier<...>::apply(...): not enough arguments to run this method";
    }
};

//template <class Op>
//static void execute_for_ternary_logic_impl(ColumnRawPtrs arguments,
//                                           ColumnWithTypeAndName& result_info,
//                                           size_t input_rows_count) {
//    /// Combine all constant columns into a single constant value.
//    UInt8 const_3v_value = 0;
//    const bool has_consts = extract_const_columns_as_ternary<Op>(arguments, const_3v_value);
//
//    /// If the constant value uniquely determines the result, return it.
//    if (has_consts &&
//        (arguments.empty() || (Op::is_saturable() && Op::is_saturated_value(const_3v_value)))) {
//        result_info.column =
//                ColumnConst::create(build_column_from_ternary_data(UInt8Container({const_3v_value}),
//                                                                   result_info.type->is_nullable()),
//                                    input_rows_count);
//        return;
//    }
//
//    const auto result_column = ColumnUInt8::create(input_rows_count);
//    MutableColumnPtr const_column_holder;
//    if (has_consts) {
//        const_column_holder = build_column_from_ternary_data(
//                UInt8Container(input_rows_count, const_3v_value), const_3v_value == Ternary::Null);
//        arguments.push_back(const_column_holder.get());
//    }
//
//    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, result_column);
//
//    result_info.column = build_column_from_ternary_data(result_column->get_data(),
//                                                        result_info.type->is_nullable());
//}

template <class Op>
static void execute_for_ternary_logic_impl(ColumnRawPtrs arguments,
                                           ColumnWithTypeAndName& result_info,
                                           size_t input_rows_count) {
    /// Combine all constant columns into a single constant value.
    UInt8 const_3v_value = 0;
    const bool has_consts = extract_const_columns_as_ternary<Op>(arguments, const_3v_value);

//    /// If the constant value uniquely determines the result, return it.
//    if (has_consts &&
//        (arguments.empty() || (Op::is_saturable() && Op::is_saturated_value(const_3v_value)))) {
//        result_info.column =
//                ColumnConst::create(build_column_from_ternary_data(UInt8Container({const_3v_value}),
//                                                                   result_info.type->is_nullable()),
//                                    input_rows_count);
//        return;
//    }

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || Op::is_saturated_value_ternary(const_3v_value)))
    {
        result_info.column = ColumnConst::create(
                build_column_from_ternary_data(UInt8Container({const_3v_value}), result_info.type->is_nullable()),
                input_rows_count
        );
    }

//    const auto result_column = ColumnUInt8::create(input_rows_count);
//    MutableColumnPtr const_column_holder;
//    if (has_consts) {
//        const_column_holder = build_column_from_ternary_data(
//                UInt8Container(input_rows_count, const_3v_value), const_3v_value == Ternary::Null);
//        arguments.push_back(const_column_holder.get());
//    }
//
//    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, result_column);
//
//    result_info.column = build_column_from_ternary_data(result_column->get_data(),
//                                                        result_info.type->is_nullable());

    const auto result_column = has_consts ?
                                          ColumnUInt8::create(input_rows_count, const_3v_value) : ColumnUInt8::create(input_rows_count);

    OperationApplier<Op, AssociativeGenericApplierImpl>::apply(arguments, result_column->get_data(), has_consts);
//    struct timespec startT, endT;
//    clock_gettime(CLOCK_MONOTONIC, &startT);
    result_info.column = build_column_from_ternary_data2(result_column, result_info.type->is_nullable());
//    clock_gettime(CLOCK_MONOTONIC, &endT);
//    fprintf(stderr, "==> execute_for_ternary_logic_impl %lu ns\n", (endT.tv_sec - startT.tv_sec) * 1000000000 + (endT.tv_nsec - startT.tv_nsec));
}

//template <typename Op, typename... Types>
//struct TypedExecutorInvoker;
//
//template <typename Op>
//using FastApplierImpl = TypedExecutorInvoker<Op, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32,
//                                             Int64, Float32, Float64>;
//
//template <typename Op, typename Type, typename... Types>
//struct TypedExecutorInvoker<Op, Type, Types...> {
//    template <typename T, typename Result>
//    static void apply(const ColumnVector<T>& x, const IColumn& y, Result& result) {
//        if (const auto column = typeid_cast<const ColumnVector<Type>*>(&y))
//            std::transform(x.get_data().cbegin(), x.get_data().cend(), column->get_data().cbegin(),
//                           result.begin(),
//                           [](const auto a, const auto b) { return Op::apply(!!a, !!b); });
//        else
//            TypedExecutorInvoker<Op, Types...>::template apply<T>(x, y, result);
//    }
//
//    template <typename Result>
//    static void apply(const IColumn& x, const IColumn& y, Result& result) {
//        if (const auto column = typeid_cast<const ColumnVector<Type>*>(&x))
//            FastApplierImpl<Op>::template apply<Type>(*column, y, result);
//        else
//            TypedExecutorInvoker<Op, Types...>::apply(x, y, result);
//    }
//};
//
//template <typename Op>
//struct TypedExecutorInvoker<Op> {
//    template <typename T, typename Result>
//    static void apply(const ColumnVector<T>&, const IColumn& y, Result&) {
//        LOG(FATAL) << "Unknown numeric column y of type: " << demangle(typeid(y).name());
//    }
//
//    template <typename Result>
//    static void apply(const IColumn& x, const IColumn&, Result&) {
//        LOG(FATAL) << "Unknown numeric column x of type: " << demangle(typeid(x).name());
//    }
//};

template <typename Op, typename ... Types>
struct TypedExecutorInvoker;

template <typename Op>
using FastApplierImpl =
        TypedExecutorInvoker<Op, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>;

template <typename Op, typename Type, typename ... Types>
struct TypedExecutorInvoker<Op, Type, Types ...>
{
    template <typename T, typename Result>
    static void apply(const ColumnVector<T> & x, const IColumn & y, Result & result)
    {
        if (const auto column = typeid_cast<const ColumnVector<Type> *>(&y))
            std::transform(
                    x.get_data().cbegin(), x.get_data().cend(),
                    column->get_data().cbegin(), result.begin(),
                    [](const auto a, const auto b) { return Op::apply(static_cast<bool>(a), static_cast<bool>(b)); });
        else
            TypedExecutorInvoker<Op, Types ...>::template apply<T>(x, y, result);
    }

    template <typename Result>
    static void apply(const IColumn & x, const IColumn & y, Result & result)
    {
        if (const auto column = typeid_cast<const ColumnVector<Type> *>(&x))
            FastApplierImpl<Op>::template apply<Type>(*column, y, result);
        else
            TypedExecutorInvoker<Op, Types ...>::apply(x, y, result);
    }
};

template <typename Op>
struct TypedExecutorInvoker<Op>
{
    template <typename T, typename Result>
    static void apply(const ColumnVector<T> &, const IColumn & y, Result &)
    {
        LOG(FATAL) << "Unknown numeric column y of type: " << demangle(typeid(y).name());
    }

    template <typename Result>
    static void apply(const IColumn & x, const IColumn &, Result &)
    {
        LOG(FATAL) << "Unknown numeric column x of type: " << demangle(typeid(x).name());
    }
};

//template <class Op>
//static void basic_execute_impl(ColumnRawPtrs arguments, ColumnWithTypeAndName& result_info,
//                               size_t input_rows_count) {
//    /// Combine all constant columns into a single constant value.
//    UInt8 const_val = 0;
//    bool has_consts = extract_const_columns_as_bool<Op>(arguments, const_val);
//
//    /// If the constant value uniquely determines the result, return it.
//    if (has_consts && (arguments.empty() || Op::apply(const_val, 0) == Op::apply(const_val, 1))) {
//        if (!arguments.empty()) const_val = Op::apply(const_val, 0);
//        result_info.column =
//                DataTypeUInt8().create_column_const(input_rows_count, to_field(const_val));
//        return;
//    }
//
//    /// If the constant value is a neutral element, let's forget about it.
//    if (has_consts && Op::apply(const_val, 0) == 0 && Op::apply(const_val, 1) == 1)
//        has_consts = false;
//
//    UInt8ColumnPtrs uint8_args;
//
//    auto col_res = ColumnUInt8::create();
//    UInt8Container& vec_res = col_res->get_data();
//    if (has_consts) {
//        vec_res.assign(input_rows_count, const_val);
//        uint8_args.push_back(col_res.get());
//    } else {
//        vec_res.resize(input_rows_count);
//    }
//
//    /// FastPath detection goes in here
//    if (arguments.size() == (has_consts ? 1 : 2)) {
//        if (has_consts)
//            FastApplierImpl<Op>::apply(*arguments[0], *col_res, col_res->get_data());
//        else
//            FastApplierImpl<Op>::apply(*arguments[0], *arguments[1], col_res->get_data());
//
//        result_info.column = std::move(col_res);
//        return;
//    }
//
//    /// Convert all columns to UInt8
//    Columns converted_columns;
//    for (const IColumn* column : arguments) {
//        if (auto uint8_column = check_and_get_column<ColumnUInt8>(column))
//            uint8_args.push_back(uint8_column);
//        else {
//            auto converted_column = ColumnUInt8::create(input_rows_count);
////            convert_column_to_uint8(column, converted_column->get_data());
//            convert_any_column_to_bool(column, converted_column->get_data());
//            uint8_args.push_back(converted_column.get());
//            converted_columns.emplace_back(std::move(converted_column));
//        }
//    }
//
//    OperationApplier<Op, AssociativeApplierImpl>::apply(uint8_args, col_res);
//
//    /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
//    if (uint8_args[0] != col_res.get()) vec_res.assign(uint8_args[0]->get_data());
//
//    result_info.column = std::move(col_res);
//}

template <class Op>
static void basic_execute_impl(ColumnRawPtrs arguments, ColumnWithTypeAndName& result_info,
                               size_t input_rows_count) {
    /// Combine all constant columns into a single constant value.
    UInt8 const_val = 0;
    bool has_consts = extract_const_columns_as_bool<Op>(arguments, const_val);

    /// If the constant value uniquely determines the result, return it.
    if (has_consts && (arguments.empty() || Op::apply(const_val, 0) == Op::apply(const_val, 1))) {
        if (!arguments.empty()) const_val = Op::apply(const_val, 0);
        result_info.column =
                DataTypeUInt8().create_column_const(input_rows_count, to_field(const_val));
        return;
    }

    /// If the constant value is a neutral element, let's forget about it.
    if (has_consts && Op::apply(const_val, 0) == 0 && Op::apply(const_val, 1) == 1)
        has_consts = false;

//    UInt8ColumnPtrs uint8_args;

//    auto col_res = ColumnUInt8::create();
//    UInt8Container& vec_res = col_res->get_data();
//    if (has_consts) {
//        vec_res.assign(input_rows_count, const_val);
//        uint8_args.push_back(col_res.get());
//    } else {
//        vec_res.resize(input_rows_count);
//    }

    auto col_res = has_consts ?
                              ColumnUInt8::create(input_rows_count, const_val) : ColumnUInt8::create(input_rows_count);

    /// FastPath detection goes in here
    if (arguments.size() == (has_consts ? 1 : 2)) {
        if (has_consts)
            FastApplierImpl<Op>::apply(*arguments[0], *col_res, col_res->get_data());
        else
            FastApplierImpl<Op>::apply(*arguments[0], *arguments[1], col_res->get_data());

        result_info.column = std::move(col_res);
        return;
    }

    /// Convert all columns to UInt8
    UInt8ColumnPtrs uint8_args;
    Columns converted_columns;
    for (const IColumn* column : arguments) {
        if (auto uint8_column = check_and_get_column<ColumnUInt8>(column))
            uint8_args.push_back(uint8_column);
        else {
            auto converted_column = ColumnUInt8::create(input_rows_count);
            //            convert_column_to_uint8(column, converted_column->get_data());
            convert_any_column_to_bool(column, converted_column->get_data());
            uint8_args.push_back(converted_column.get());
            converted_columns.emplace_back(std::move(converted_column));
        }
    }

    OperationApplier<Op, AssociativeApplierImpl>::apply(uint8_args, col_res->get_data(), has_consts);

//    /// This is possible if there is exactly one non-constant among the arguments, and it is of type UInt8.
//    if (uint8_args[0] != col_res.get()) vec_res.assign(uint8_args[0]->get_data());

    result_info.column = std::move(col_res);
}

} // namespace

template <typename Impl, typename Name>
DataTypePtr FunctionAnyArityLogical<Impl, Name>::get_return_type_impl(
        const DataTypes& arguments) const {
    if (arguments.size() < 2) {
        LOG(FATAL) << fmt::format(
                "Number of arguments for function \"{}\" should be at least 2: passed {}",
                get_name(), arguments.size());
    }

    bool has_nullable_arguments = false;
    for (size_t i = 0; i < arguments.size(); ++i) {
        const auto& arg_type = arguments[i];

        if (!has_nullable_arguments) {
            has_nullable_arguments = arg_type->is_nullable();
            if (has_nullable_arguments && !Impl::special_implementation_for_nulls()) {
                LOG(WARNING) << fmt::format(
                        "Logical error: Unexpected type of argument for function \"{}\" argument "
                        "{} is of type {}",
                        get_name(), i + 1, arg_type->get_name());
            }
        }

        if (!(is_native_number(arg_type) ||
              (Impl::special_implementation_for_nulls() &&
               (arg_type->only_null() || is_native_number(remove_nullable(arg_type)))))) {
            LOG(FATAL) << fmt::format("Illegal type ({}) of {} argument of function {}",
                                      arg_type->get_name(), i + 1, get_name());
        }
    }

    auto result_type = std::make_shared<DataTypeUInt8>();
    return has_nullable_arguments ? make_nullable(result_type) : result_type;
}

template <typename Impl, typename Name>
Status FunctionAnyArityLogical<Impl, Name>::execute_impl(FunctionContext* context, Block& block,
                                                         const ColumnNumbers& arguments,
                                                         size_t result_index,
                                                         size_t input_rows_count) {
//    struct timespec startT, endT;
//    clock_gettime(CLOCK_MONOTONIC, &startT);
    ColumnRawPtrs args_in;
    for (const auto arg_index : arguments)
        args_in.push_back(block.get_by_position(arg_index).column.get());

    auto& result_info = block.get_by_position(result_index);
    if (result_info.type->is_nullable())
        execute_for_ternary_logic_impl<Impl>(std::move(args_in), result_info, input_rows_count);
    else
        basic_execute_impl<Impl>(std::move(args_in), result_info, input_rows_count);
//    clock_gettime(CLOCK_MONOTONIC, &endT);
//    fprintf(stderr, "==> execute_impl %lu ns\n", (endT.tv_sec - startT.tv_sec) * 1000000000 + (endT.tv_nsec - startT.tv_nsec));
    return Status::OK();
}

template <typename A, typename Op>
struct UnaryOperationImpl {
    using ResultType = typename Op::ResultType;
    using ArrayA = typename ColumnVector<A>::Container;
    using ArrayC = typename ColumnVector<ResultType>::Container;

    static void NO_INLINE vector(const ArrayA& a, ArrayC& c) {
        std::transform(a.cbegin(), a.cend(), c.begin(), [](const auto x) { return Op::apply(x); });
    }
};

template <template <typename> class Impl, typename Name>
DataTypePtr FunctionUnaryLogical<Impl, Name>::get_return_type_impl(
        const DataTypes& arguments) const {
    if (!is_native_number(arguments[0])) {
        LOG(FATAL) << fmt::format("Illegal type ({}) of argument of function {}",
                                  arguments[0]->get_name(), get_name());
    }

    return std::make_shared<DataTypeUInt8>();
}

template <template <typename> class Impl, typename T>
bool functionUnaryExecuteType(Block& block, const ColumnNumbers& arguments, size_t result) {
    if (auto col = check_and_get_column<ColumnVector<T>>(
                block.get_by_position(arguments[0]).column.get())) {
        auto col_res = ColumnUInt8::create();

        typename ColumnUInt8::Container& vec_res = col_res->get_data();
        vec_res.resize(col->get_data().size());
        UnaryOperationImpl<T, Impl<T>>::vector(col->get_data(), vec_res);

        block.replace_by_position(result, std::move(col_res));
        return true;
    }

    return false;
}

template <template <typename> class Impl, typename Name>
Status FunctionUnaryLogical<Impl, Name>::execute_impl(FunctionContext* context, Block& block,
                                                      const ColumnNumbers& arguments, size_t result,
                                                      size_t /*input_rows_count*/) {
    if (!(functionUnaryExecuteType<Impl, UInt8>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, UInt16>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, UInt32>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, UInt64>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Int8>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Int16>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Int32>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Int64>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Float32>(block, arguments, result) ||
          functionUnaryExecuteType<Impl, Float64>(block, arguments, result))) {
        LOG(FATAL) << fmt::format("Illegal column {} of argument of function {}",
                                  block.get_by_position(arguments[0]).column->get_name(),
                                  get_name());
    }

    return Status::OK();
}

void register_function_logical(SimpleFunctionFactory& instance) {
    instance.register_function<FunctionAnd>();
    instance.register_function<FunctionOr>();
    instance.register_function<FunctionXor>();
    instance.register_function<FunctionNot>();
}

} // namespace doris::vectorized
