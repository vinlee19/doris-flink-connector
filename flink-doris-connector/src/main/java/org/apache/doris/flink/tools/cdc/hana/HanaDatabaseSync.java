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

package org.apache.doris.flink.tools.cdc.hana;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class HanaDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(HanaDatabaseSync.class);

    protected HanaDatabaseSync() throws SQLException {}

    @Override
    public void registerDriver() throws SQLException {}

    @Override
    public Connection getConnection() throws SQLException {
        return null;
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        return Collections.emptyList();
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public String getTableListPrefix() {
        return "";
    }
}
