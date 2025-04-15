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

#include "vec/data_types/data_type_dictionary.h"

#include <typeinfo>

#include "vec/columns/dictionary_column.h"

namespace doris::vectorized {

MutableColumnPtr DataTypeDictionary::create_column() const {
    return DictionaryColumn<int32_t>::create();
}

char* DataTypeDictionary::serialize(const IColumn& column, char* buf, int be_exec_version) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "serialize not support");
    __builtin_unreachable();
}

const char* DataTypeDictionary::deserialize(const char* buf, MutableColumnPtr* column,
                                         int be_exec_version) const {
    throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "deserialize not support");
    __builtin_unreachable();
}

bool DataTypeDictionary::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

} // namespace doris::vectorized
