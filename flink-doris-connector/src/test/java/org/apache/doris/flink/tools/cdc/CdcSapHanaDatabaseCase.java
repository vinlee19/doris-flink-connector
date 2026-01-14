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

package org.apache.doris.flink.tools.cdc;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.table.DorisConfigOptions;
import org.apache.doris.flink.tools.cdc.hana.HanaDatabaseSync;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcSapHanaDatabaseCase {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.enableCheckpointing(10000);

        String database = "saphana_cdc";
        String tablePrefix = "";
        String tableSuffix = "";
        Map<String, String> sourceConfig = new HashMap<>();
        sourceConfig.put(JdbcSourceOptions.DATABASE_NAME.key(), "SYSTEMDB");
        sourceConfig.put(JdbcSourceOptions.SCHEMA_NAME.key(), "epo_test");
        sourceConfig.put(JdbcSourceOptions.HOSTNAME.key(), "172.21.16.12");
        sourceConfig.put(DatabaseSyncConfig.PORT, "39017");
        sourceConfig.put(JdbcSourceOptions.USERNAME.key(), "SYSTEM");
        sourceConfig.put(JdbcSourceOptions.PASSWORD.key(), "Doris123456");
        // add jdbc properties configuration
        // sourceConfig.put("jdbc.properties.encrypt", "false");
        // sourceConfig.put("jdbc.properties.integratedSecurity", "false");
        // sourceConfig.put("debezium.database.tablename.case.insensitive","false");
        // sourceConfig.put("scan.incremental.snapshot.enabled","true");
        // sourceConfig.put("debezium.include.schema.changes","false");

        Configuration config = Configuration.fromMap(sourceConfig);

        Map<String, String> sinkConfig = new HashMap<>();
        sinkConfig.put(DorisConfigOptions.FENODES.key(), "172.21.16.12:28030");
        sinkConfig.put(DorisConfigOptions.USERNAME.key(), "root");
        sinkConfig.put(DorisConfigOptions.PASSWORD.key(), "123456");
        sinkConfig.put(DorisConfigOptions.JDBC_URL.key(), "jdbc:mysql://172.21.16.12:29030");
        sinkConfig.put(DorisConfigOptions.SINK_LABEL_PREFIX.key(), UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String, String> tableConfig = new HashMap<>();
        tableConfig.put(DorisTableConfig.REPLICATION_NUM, "1");
        tableConfig.put(DorisTableConfig.TABLE_BUCKETS, "tbl1:10,tbl2:20,a.*:30,b.*:40,.*:50");
        String includingTables = ".*";
        String excludingTables = "";
        String multiToOneOrigin = "";
        String multiToOneTarget = "";
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = true;
        boolean ignoreIncompatible = false;
        DatabaseSync databaseSync = new HanaDatabaseSync();
        databaseSync
                .setEnv(env)
                .setDatabase(database)
                .setConfig(config)
                .setTablePrefix(tablePrefix)
                .setTableSuffix(tableSuffix)
                .setIncludingTables(includingTables)
                .setExcludingTables(excludingTables)
                .setMultiToOneOrigin(multiToOneOrigin)
                .setMultiToOneTarget(multiToOneTarget)
                .setIgnoreDefaultValue(ignoreDefaultValue)
                .setSinkConfig(sinkConf)
                .setTableConfig(tableConfig)
                .setCreateTableOnly(true)
                .setNewSchemaChange(useNewSchemaChange)
                .setIgnoreIncompatible(ignoreIncompatible)
                .create();
        databaseSync.build();
        env.execute(String.format("SqlServer-Doris Database Sync: %s", database));
    }
}
