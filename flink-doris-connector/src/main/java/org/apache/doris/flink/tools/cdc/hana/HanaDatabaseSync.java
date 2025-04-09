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

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class HanaDatabaseSync extends DatabaseSync {
    private static final Logger LOG = LoggerFactory.getLogger(HanaDatabaseSync.class);
    public static final ConfigOption<Integer> SAP_HANA_PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(30015)
                    .withDescription("Integer port number of the SAP HANA database server.");
    private static final String JDBC_URL = "jdbc:sap://%s:%d/?databaseName=%s";

    public HanaDatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("com.sap.db.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            throw new SQLException(
                    "No suitable driver found, can not found class com.sap.db.jdbc.Driver");
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        String jdbcUrlTemplate = getJdbcUrlTemplate(JDBC_URL, jdbcProperties);
        String jdbcUrl =
                String.format(
                        jdbcUrlTemplate,
                        config.get(JdbcSourceOptions.HOSTNAME),
                        config.get(SAP_HANA_PORT),
                        config.get(JdbcSourceOptions.DATABASE_NAME));
        Properties pro = new Properties();
        pro.setProperty(DatabaseSyncConfig.USER, config.get(JdbcSourceOptions.USERNAME));
        pro.setProperty(DatabaseSyncConfig.PASSWORD, config.get(JdbcSourceOptions.PASSWORD));
        return DriverManager.getConnection(jdbcUrl, pro);
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        String schemaName = config.get(JdbcSourceOptions.SCHEMA_NAME);
        List<SourceSchema> schemaList = new ArrayList<>();
        LOG.info("database-name {}", databaseName);
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet tables =
                    metaData.getTables(databaseName, schemaName, "%", new String[] {"TABLE"})) {
                while (tables.next()) {
                    String tableName = tables.getString(DatabaseSyncConfig.TABLE_NAME);
                    String tableComment = tables.getString(DatabaseSyncConfig.REMARKS);
                    if (!isSyncNeeded(tableName)) {
                        continue;
                    }
                    SourceSchema sourceSchema =
                            new HanaSchema(
                                    metaData, databaseName, schemaName, tableName, tableComment);
                    sourceSchema.setModel(
                            !sourceSchema.primaryKeys.isEmpty()
                                    ? DataModel.UNIQUE
                                    : DataModel.DUPLICATE);
                    schemaList.add(sourceSchema);
                }
            }
        }
        return schemaList;
    }

    /** mock null DataStreamSource */
    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        return null;
    }

    @Override
    public String getTableListPrefix() {
        return config.get(JdbcSourceOptions.SCHEMA_NAME);
    }
}
