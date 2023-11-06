// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "common/status.h"

#include <gen_cpp/Status_types.h>
#include <gen_cpp/types.pb.h> // for PStatus
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <new>
#include <vector>

#include "service/backend_options.h"

namespace doris {

Status& Status::prepend(std::string_view msg) {
    if (!ok()) {
        if (_err_msg == nullptr) {
            _err_msg = std::make_unique<ErrMsg>();
        }
        _err_msg->_msg = std::string(msg) + _err_msg->_msg;
    }
    return *this;
}

Status& Status::append(std::string_view msg) {
    if (!ok()) {
        if (_err_msg == nullptr) {
            _err_msg = std::make_unique<ErrMsg>();
        }
        _err_msg->_msg.append(msg);
    }
    return *this;
}

std::string Status::to_json() const {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);
    writer.StartObject();
    // status
    writer.Key("status");
    writer.String(code_as_string().c_str());
    // msg
    writer.Key("msg");
    ok() ? writer.String("OK") : writer.String(_err_msg ? _err_msg->_msg.c_str() : "");
    writer.EndObject();
    return s.GetString();
}

} // namespace doris
