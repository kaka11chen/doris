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

package org.apache.doris.analysis;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

// For syntax select * from tbl INTO OUTFILE xxxx
public class BlackHoleClause {
    private static final Logger LOG = LogManager.getLogger(BlackHoleClause.class);

    public static final List<String> RESULT_COL_NAMES = Lists.newArrayList();
    public static final List<Type> RESULT_COL_TYPES = Lists.newArrayList();
    public static final String ROWS_PROCESSED = "RowsProcessed";
    public static final String BYTES_PROCESSED = "BytesProcessed";

    public static final String SCAN_ROWS = "SCAN_ROWS";
    public static final String SCAN_BYTES = "SCAN_BYTES";
    public static final String SCAN_BYTES_FROM_LOCAL_STORAGE = "ScanBytesFromLocalStorage";
    public static final String SCAN_BYTES_FROM_REMOTE_STORAGE = "ScanBytesFromRemoteStorage";

    static {
        RESULT_COL_NAMES.add(ROWS_PROCESSED);
        RESULT_COL_NAMES.add(BYTES_PROCESSED);
        RESULT_COL_NAMES.add(SCAN_ROWS);
        RESULT_COL_NAMES.add(SCAN_BYTES);
        RESULT_COL_NAMES.add(SCAN_BYTES_FROM_LOCAL_STORAGE);
        RESULT_COL_NAMES.add(SCAN_BYTES_FROM_REMOTE_STORAGE);

        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
        RESULT_COL_TYPES.add(ScalarType.createType(PrimitiveType.BIGINT));
    }

    public BlackHoleClause() {

    }
}
