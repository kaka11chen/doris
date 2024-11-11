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

package org.apache.doris.fs;

import org.apache.doris.backup.Status;
import org.apache.doris.backup.Status.ErrCode;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.util.LocationPath;
import org.apache.doris.fs.remote.RemoteFile;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SimpleDirectoryLister implements DirectoryLister
{
    private static final Logger LOG = LogManager.getLogger(SimpleDirectoryLister.class);

    public RemoteIterator<RemoteFile> listFilesRecursively(FileSystem fs, TableIf table, String location)
            throws IOException {
        LOG.info("SimpleDirectoryLister directory listing: {}", location);

        List<RemoteFile> result = new ArrayList<>();
        Status status = fs.listFiles(location, false, result);
        if (!status.ok()) {
            throw new IOException(status.getErrMsg());
        }
        return new RemoteFileRemoteIterator(result);
    }
}
