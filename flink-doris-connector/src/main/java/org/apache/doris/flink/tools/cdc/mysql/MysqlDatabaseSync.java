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

package org.apache.doris.flink.tools.cdc.mysql;

import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.Column;
import io.debezium.relational.Table;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.mysql.debezium.task.MySqlSnapshotSplitReadTask;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSourceBuilder;
import org.apache.flink.cdc.connectors.mysql.source.assigners.MySqlSnapshotSplitAssigner;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffsetBuilder;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSnapshotSplit;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;
import org.apache.flink.cdc.connectors.mysql.source.utils.StatementUtils;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.connectors.shaded.org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.debezium.relational.TableId;
import org.apache.doris.flink.catalog.doris.DataModel;
import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.DatabaseSyncConfig;
import org.apache.doris.flink.tools.cdc.SourceSchema;
import org.apache.doris.flink.tools.cdc.deserialize.DorisJsonDebeziumDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceOptions.DATABASE_NAME;
import static org.apache.flink.cdc.debezium.utils.JdbcUrlUtils.PROPERTIES_PREFIX;

public class MysqlDatabaseSync extends DatabaseSync {
    private static ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(MysqlDatabaseSync.class);
    private static final String JDBC_URL = "jdbc:mysql://%s:%d?useInformationSchema=true";
    private static final String SPLIT_ID = "splitId";
    private static final String FINISH_SPLITS = "finishSplits";
    private static final String ASSIGNED_SPLITS = "assignedSplits";
    private static final String SNAPSHOT_TABLE = "snapshotTable";
    private static final String PURE_BINLOG_PHASE = "pureBinlogPhase";

    public MysqlDatabaseSync() throws SQLException {
        super();
    }

    @Override
    public void registerDriver() throws SQLException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException ex) {
            LOG.warn(
                    "can not found class com.mysql.cj.jdbc.Driver, use class com.mysql.jdbc.Driver");
            try {
                Class.forName("com.mysql.jdbc.Driver");
            } catch (Exception e) {
                throw new SQLException(
                        "No suitable driver found, can not found class com.mysql.cj.jdbc.Driver and com.mysql.jdbc.Driver");
            }
        }
    }

    @Override
    public Connection getConnection() throws SQLException {
        Properties jdbcProperties = getJdbcProperties();
        String jdbcUrlTemplate = getJdbcUrlTemplate(JDBC_URL, jdbcProperties);
        String jdbcUrl =
                String.format(
                        jdbcUrlTemplate,
                        config.get(MySqlSourceOptions.HOSTNAME),
                        config.get(MySqlSourceOptions.PORT));

        return DriverManager.getConnection(
                jdbcUrl,
                config.get(MySqlSourceOptions.USERNAME),
                config.get(MySqlSourceOptions.PASSWORD));
    }

    @Override
    public List<SourceSchema> getSchemaList() throws Exception {
        String databaseName = config.get(DATABASE_NAME);

        List<SourceSchema> schemaList = new ArrayList<>();
        try (Connection conn = getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();
            try (ResultSet catalogs = metaData.getCatalogs()) {
                while (catalogs.next()) {
                    String tableCatalog = catalogs.getString("TABLE_CAT");
                    if (tableCatalog.matches(databaseName)) {
                        try (ResultSet tables =
                                metaData.getTables(
                                        tableCatalog, null, "%", new String[] {"TABLE"})) {
                            while (tables.next()) {
                                String tableName = tables.getString(DatabaseSyncConfig.TABLE_NAME);
                                String tableComment = tables.getString(DatabaseSyncConfig.REMARKS);
                                if (!isSyncNeeded(tableName)) {
                                    continue;
                                }
                                SourceSchema sourceSchema =
                                        new MysqlSchema(
                                                metaData, tableCatalog, tableName, tableComment);
                                sourceSchema.setModel(
                                        !sourceSchema.primaryKeys.isEmpty()
                                                ? DataModel.UNIQUE
                                                : DataModel.DUPLICATE);
                                schemaList.add(sourceSchema);
                            }
                        }
                    }
                }
            }
        }
        return schemaList;
    }

    @Override
    public DataStreamSource<String> buildCdcSource(StreamExecutionEnvironment env) {
        MySqlSourceBuilder<String> sourceBuilder = MySqlSource.builder();

        String databaseName = config.get(DATABASE_NAME);
        Preconditions.checkNotNull(databaseName, "database-name in mysql is required");
        String tableName = config.get(MySqlSourceOptions.TABLE_NAME);
        sourceBuilder
                .hostname(config.get(MySqlSourceOptions.HOSTNAME))
                .port(config.get(MySqlSourceOptions.PORT))
                .username(config.get(MySqlSourceOptions.USERNAME))
                .password(config.get(MySqlSourceOptions.PASSWORD))
                .databaseList(databaseName)
                .tableList(tableName);
        // server_id
        config.getOptional(MySqlSourceOptions.SERVER_ID).ifPresent(sourceBuilder::serverId);
        config.getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(sourceBuilder::serverTimeZone);
        config.getOptional(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE)
                .ifPresent(sourceBuilder::fetchSize);
        config.getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(sourceBuilder::connectTimeout);
        config.getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(sourceBuilder::connectMaxRetries);
        config.getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(sourceBuilder::connectionPoolSize);
        config.getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(sourceBuilder::heartbeatInterval);
        config.getOptional(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED)
                .ifPresent(sourceBuilder::scanNewlyAddedTableEnabled);
        config.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(sourceBuilder::splitSize);
        config.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)
                .ifPresent(sourceBuilder::closeIdleReaders);

        setChunkColumns(sourceBuilder);
        String startupMode = config.get(MySqlSourceOptions.SCAN_STARTUP_MODE);
        if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_INITIAL.equalsIgnoreCase(startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.initial());
        } else if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_EARLIEST_OFFSET.equalsIgnoreCase(
                startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.earliest());
        } else if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_LATEST_OFFSET.equalsIgnoreCase(
                startupMode)) {
            sourceBuilder.startupOptions(StartupOptions.latest());
        } else if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_SPECIFIC_OFFSET.equalsIgnoreCase(
                startupMode)) {
            BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();
            String file = config.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
            Long pos = config.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);
            if (file != null && pos != null) {
                offsetBuilder.setBinlogFilePosition(file, pos);
            }
            config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_GTID_SET)
                    .ifPresent(offsetBuilder::setGtidSet);
            config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                    .ifPresent(offsetBuilder::setSkipEvents);
            config.getOptional(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                    .ifPresent(offsetBuilder::setSkipRows);
            sourceBuilder.startupOptions(StartupOptions.specificOffset(offsetBuilder.build()));
        } else if (DatabaseSyncConfig.SCAN_STARTUP_MODE_VALUE_TIMESTAMP.equalsIgnoreCase(
                startupMode)) {
            sourceBuilder.startupOptions(
                    StartupOptions.timestamp(
                            config.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)));
        }

        Properties jdbcProperties = new Properties();
        Properties debeziumProperties = new Properties();
        // date to string
        debeziumProperties.putAll(DateToStringConverter.DEFAULT_PROPS);

        for (Map.Entry<String, String> entry : config.toMap().entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (key.startsWith(PROPERTIES_PREFIX)) {
                jdbcProperties.put(key.substring(PROPERTIES_PREFIX.length()), value);
            } else if (key.startsWith(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX)) {
                debeziumProperties.put(
                        key.substring(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX.length()), value);
            }
        }
        sourceBuilder.jdbcProperties(jdbcProperties);
        sourceBuilder.debeziumProperties(debeziumProperties);
        DebeziumDeserializationSchema<String> schema;
        if (ignoreDefaultValue) {
            schema = new DorisJsonDebeziumDeserializationSchema();
        } else {
            Map<String, Object> customConverterConfigs = new HashMap<>();
            customConverterConfigs.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, "numeric");
            schema = new JsonDebeziumDeserializationSchema(false, customConverterConfigs);
        }
        MySqlSource<String> mySqlSource =
                sourceBuilder.deserializer(schema).includeSchemaChanges(true).build();
        MySqlSourceConfig mysqlSourceConfig = getMysqlSourceConfig(config);
        List<MySqlSnapshotSplit> mySqlSnapshotSplits = startSplitChunks(mysqlSourceConfig, "store_sales", config);
        System.out.println(mySqlSnapshotSplits.size());

        return env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
    }

    @Override
    public String getTableListPrefix() {
        return config.get(DATABASE_NAME);
    }

    /**
     * set chunkkeyColumn,eg: db.table1:column1,db.table2:column2.
     *
     * @param sourceBuilder
     */
    private void setChunkColumns(MySqlSourceBuilder<String> sourceBuilder) {
        Map<ObjectPath, String> chunkColumnMap = getChunkColumnMap();
        for (Map.Entry<ObjectPath, String> entry : chunkColumnMap.entrySet()) {
            sourceBuilder.chunkKeyColumn(entry.getKey(), entry.getValue());
        }
    }

    private Map<ObjectPath, String> getChunkColumnMap() {
        Map<ObjectPath, String> chunkMap = new HashMap<>();
        String chunkColumn =
                config.getString(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        if (!StringUtils.isNullOrWhitespaceOnly(chunkColumn)) {
            final Pattern chunkPattern = Pattern.compile("(\\S+)\\.(\\S+):(\\S+)");
            String[] tblColumns = chunkColumn.split(",");
            for (String tblCol : tblColumns) {
                Matcher matcher = chunkPattern.matcher(tblCol);
                if (matcher.find()) {
                    String db = matcher.group(1);
                    String table = matcher.group(2);
                    String col = matcher.group(3);
                    chunkMap.put(new ObjectPath(db, table), col);
                }
            }
        }
        return chunkMap;
    }

    /**
     *  startSplitChunk, obtain chunk info
     */
    private List<MySqlSnapshotSplit> startSplitChunks(
            MySqlSourceConfig sourceConfig, String snapshotTable, Configuration config) {
        List<TableId> remainingTables = new ArrayList<>();
        if (snapshotTable != null) {
            // need add database name
            String database = config.get(DATABASE_NAME);
            remainingTables.add(TableId.parse(database + "." + snapshotTable));
        }
        List<MySqlSnapshotSplit> remainingSplits = new ArrayList<>();
        MySqlSnapshotSplitAssigner splitAssigner =
                new MySqlSnapshotSplitAssigner(sourceConfig, 1, remainingTables, false);
        splitAssigner.open();
        while (true) {
            Optional<MySqlSplit> mySqlSplit = splitAssigner.getNext();
            if (mySqlSplit.isPresent()) {
                MySqlSnapshotSplit snapshotSplit = mySqlSplit.get().asSnapshotSplit();
                remainingSplits.add(snapshotSplit);
            } else {
                break;
            }
        }
        splitAssigner.close();
        return remainingSplits;
    }


    private MySqlSourceConfig getMysqlSourceConfig(Configuration configuration) {
        // Create factory and validate required fields
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        String databaseName = validateRequiredField(
                configuration.get(MySqlSourceOptions.DATABASE_NAME)
        );
        String tableName = configuration.get(MySqlSourceOptions.TABLE_NAME);

        // Apply optional configurations using Java 8 Optional
        applyOptionalConfigs(configuration, configFactory);

        // Configure startup mode
        configureStartupMode(configuration, configFactory);

        // Configure basic connection settings
        return configFactory
                .includeSchemaChanges(true)
                .hostname(configuration.get(MySqlSourceOptions.HOSTNAME))
                .port(configuration.get(MySqlSourceOptions.PORT))
                .databaseList(databaseName)
                .tableList(tableName)
                .username(configuration.get(MySqlSourceOptions.USERNAME))
                .password(configuration.get(MySqlSourceOptions.PASSWORD))
                .startupOptions(StartupOptions.latest())
                .createConfig(0);
    }

    private String validateRequiredField(String value) {
        return Optional.ofNullable(value)
                .orElseThrow(() -> new IllegalArgumentException("database-name in mysql is required"));
    }

    private void applyOptionalConfigs(Configuration configuration, MySqlSourceConfigFactory configFactory) {
        // Apply server related configurations
        configuration.getOptional(MySqlSourceOptions.SERVER_ID)
                .ifPresent(configFactory::serverId);
        configuration.getOptional(MySqlSourceOptions.SERVER_TIME_ZONE)
                .ifPresent(configFactory::serverTimeZone);

        // Apply scan related configurations
        configuration.getOptional(MySqlSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE)
                .ifPresent(configFactory::fetchSize);
        configuration.getOptional(MySqlSourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED)
                .ifPresent(configFactory::scanNewlyAddedTableEnabled);
        configuration.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE)
                .ifPresent(configFactory::splitSize);
        configuration.getOptional(MySqlSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED)
                .ifPresent(configFactory::closeIdleReaders);

        // Apply connection related configurations
        configuration.getOptional(MySqlSourceOptions.CONNECT_TIMEOUT)
                .ifPresent(configFactory::connectTimeout);
        configuration.getOptional(MySqlSourceOptions.CONNECT_MAX_RETRIES)
                .ifPresent(configFactory::connectMaxRetries);
        configuration.getOptional(MySqlSourceOptions.CONNECTION_POOL_SIZE)
                .ifPresent(configFactory::connectionPoolSize);
        configuration.getOptional(MySqlSourceOptions.HEARTBEAT_INTERVAL)
                .ifPresent(configFactory::heartbeatInterval);
    }

    private void configureStartupMode(Configuration configuration, MySqlSourceConfigFactory configFactory) {
        String startupMode = configuration.get(MySqlSourceOptions.SCAN_STARTUP_MODE);

        StartupOptions startupOptions;
        if (startupMode == null) {
            startupOptions = StartupOptions.latest();
        } else {
            switch (startupMode.toLowerCase()) {
                case "initial":
                    startupOptions = StartupOptions.initial();
                    break;
                case "earliest-offset":
                    startupOptions = StartupOptions.earliest();
                    break;
                case "latest-offset":
                    startupOptions = StartupOptions.latest();
                    break;
                case "specific-offset":
                    startupOptions = createSpecificOffset(configuration);
                    break;
                case "timestamp":
                    startupOptions = createTimestampOffset(configuration);
                    break;
                default:
                    startupOptions = StartupOptions.latest();
            }
        }

        configFactory.startupOptions(startupOptions);
    }

    private StartupOptions createSpecificOffset(Configuration configuration) {
        BinlogOffsetBuilder offsetBuilder = BinlogOffset.builder();

        // Handle file position
        String file = configuration.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_FILE);
        Long pos = configuration.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_POS);

        offsetBuilder.setBinlogFilePosition(
                Optional.ofNullable(file).orElse(""),
                Optional.ofNullable(pos).orElse(0L)
        );

        // Handle skip events
        if (configuration.containsKey(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS.key())) {
            offsetBuilder.setSkipEvents(
                    configuration.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
            );
        }

        // Handle skip rows
        if (configuration.containsKey(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS.key())) {
            offsetBuilder.setSkipRows(
                    configuration.get(MySqlSourceOptions.SCAN_STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
            );
        }

        return StartupOptions.specificOffset(offsetBuilder.build());
    }

    private StartupOptions createTimestampOffset(Configuration configuration) {
        return StartupOptions.timestamp(
                configuration.get(MySqlSourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS)
        );
    }

    private void createDataEventsForTable(
            MySqlSnapshotSplit snapshotSplit,
            EventDispatcher.SnapshotReceiver<MySqlPartition> snapshotReceiver,
            Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOG.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                StatementUtils.buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null);
        LOG.info(
                "For split '{}' of table {} using select statement: '{}'",
                snapshotSplit.splitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                     StatementUtils.readTableSplitDataStatement(
                             jdbcConnection,
                             selectSql,
                             snapshotSplit.getSplitStart() == null,
                             snapshotSplit.getSplitEnd() == null,
                             snapshotSplit.getSplitStart(),
                             snapshotSplit.getSplitEnd(),
                             snapshotSplit.getSplitKeyType().getFieldCount(),
                             sourceConfig.getFetchSize());
             ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = table.columns().get(i);
                    row[columnArray.getColumns()[i].position() - 1] =
                            readField(rs, i + 1, actualColumn, table);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOG.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(stop - exportStart));
                    snapshotChangeEventSourceMetrics.rowsScanned(
                            snapshotContext.partition, table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                        (MySqlPartition) snapshotContext.partition,
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            LOG.info(
                    "Finished exporting {} records for split '{}', total duration '{}'",
                    rows,
                    snapshotSplit.splitId(),
                    Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }

}
