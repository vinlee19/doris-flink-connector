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
package org.apache.doris.flink.tools.cdc.tidb;


import com.ververica.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.TiConfiguration;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;

import java.sql.*;
import java.util.*;


public class TidbDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(TidbDatabaseSync.class);
    private static  String JDBC_URL = "jdbc:mysql://%s:%d?useInformationSchema=true";
    private static final String PROPERTIES_PREFIX = "jdbc.properties.";

    public TidbDatabaseSync() {
        // TODO document why this constructor is empty
    }


    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        StringBuilder jdbcUrlSb = new StringBuilder(JDBC_URL);
        jdbcProperties.forEach((key, value) -> jdbcUrlSb.append("&").append(key).append("=").append(value));
        String jdbcUrl = String.format(jdbcUrlSb.toString(), config.get(MySqlSourceOptions.HOSTNAME), config.get(MySqlSourceOptions.PORT));

        return DriverManager.getConnection(jdbcUrl,config.get(MySqlSourceOptions.USERNAME),config.get(MySqlSourceOptions.PASSWORD));
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {

        String databaseName = config.get(TDBSourceOptions.DATABASE_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        LOG.info("database-name {}", databaseName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                         metaData.getTables(databaseName, null, "%", new String[]{"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString("TABLE_NAME");
                    String tableComment = tables.getString("REMARKS");
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new TidbSchema(metaData, databaseName, tableName, tableComment);
                    sourceSchema.setModel(!sourceSchema.primaryKeys.isEmpty() ? DataModel.UNIQUE : DataModel.DUPLICATE);
                    schemaList.add(sourceSchema);
                }
            }
        }
        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        TiDBSource.Builder<String> tidbSourceBuilder = TiDBSource.<String>builder();
        String databaseName = config.get(TDBSourceOptions.DATABASE_NAME);
        String pdAddress = config.getString(TDBSourceOptions.PD_ADDRESSES);
        Preconditions.checkNotNull(databaseName,"database-name in tidb is required");
        Preconditions.checkNotNull(pdAddress,"pd-address in tidb is required");
        String tableName = config.get(TDBSourceOptions.TABLE_NAME);

        StartupOptions startupOptions;
        String startupMode = config.get(TDBSourceOptions.SCAN_STARTUP_MODE);
        if ("initial".equalsIgnoreCase(startupMode)){
            startupOptions = StartupOptions.initial();
        }else {
            startupOptions = StartupOptions.latest();
        }

        Map<String,String> map = new HashMap<>();
        map.put("tikv.grpc.timeout_in_ms","2000");
        TiConfiguration tiConfiguration = TDBSourceOptions.getTiConfiguration(pdAddress, map);


        RichParallelSourceFunction<String> tidbSource = tidbSourceBuilder
                .database(database)
                .startupOptions(startupOptions)
                .tableName(tableName)
                .changeEventDeserializer(new TiKVChangeEventDeserializationSchema<String>() {
                    @Override
                    public void deserialize(Cdcpb.Event.Row row, Collector<String> collector) throws Exception {
                        System.err.println(row.toString());
                        collector.collect(row.toString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                       return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .snapshotEventDeserializer(new TiKVSnapshotEventDeserializationSchema<String>() {
                    @Override
                    public void deserialize(Kvrpcpb.KvPair kvPair, Collector<String> collector) throws Exception {
                        System.out.println(kvPair.toString());
                        collector.collect(kvPair.toString());
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                })
                .tiConf(tiConfiguration).build();


        return env.addSource(tidbSource,"TiDB Source");
    }

    private Properties getJdbcProperties(){
        Properties jdbcProps = new Properties();
        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProps.put(key.substring(PROPERTIES_PREFIX.length()), value);
            }
        }
        return jdbcProps;
    }
}
