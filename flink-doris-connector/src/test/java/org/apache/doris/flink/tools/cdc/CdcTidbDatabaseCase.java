package org.apache.doris.flink.tools.cdc;

import org.apache.doris.flink.tools.cdc.tidb.TidbDatabaseSync;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class CdcTidbDatabaseCase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        Map<String,String> flinkMap = new HashMap<>();
        flinkMap.put("execution.checkpointing.interval","10s");
        flinkMap.put("pipeline.operator-chaining","false");
        flinkMap.put("parallelism.default","1");


        Configuration configuration = Configuration.fromMap(flinkMap);
        env.configure(configuration);

        String database = "sf_test";
        String tablePrefix = "";
        String tableSuffix = "";
        // tidb通过mysql 驱动去连接
        Map<String,String> tidbConfig = new HashMap<>();
        tidbConfig.put("hostname","10.16.10.6");
        tidbConfig.put("port","14000");
        tidbConfig.put("username","root");
        tidbConfig.put("password","");
        tidbConfig.put("database-name","sf_test");
        tidbConfig.put("pd-addresses","10.16.10.6:2379");
//        tidbConfig.put("table-name","nation");
        Configuration config = Configuration.fromMap(tidbConfig);

        Map<String,String> sinkConfig = new HashMap<>();
        sinkConfig.put("fenodes","172.21.16.12:28030");
        // sinkConfig.put("benodes","10.20.30.1:8040, 10.20.30.2:8040, 10.20.30.3:8040");
        sinkConfig.put("username","root");
        sinkConfig.put("password","123456");
        sinkConfig.put("jdbc-url","jdbc:mysql://172.21.16.12:29030");
        sinkConfig.put("sink.label-prefix", UUID.randomUUID().toString());
        Configuration sinkConf = Configuration.fromMap(sinkConfig);

        Map<String,String> tableConfig = new HashMap<>();
        tableConfig.put("replication_num", "1");

        String includingTables = "";
        String excludingTables = "";
        boolean ignoreDefaultValue = false;
        boolean useNewSchemaChange = false;
        DatabaseSync databaseSync = new TidbDatabaseSync();
        databaseSync.create(env,database,config,tablePrefix,tableSuffix,null,excludingTables,ignoreDefaultValue,sinkConf,tableConfig, false, useNewSchemaChange);
        databaseSync.build();
        env.execute(String.format("TiDB-Doris Database Sync: %s", database));
    }

}
