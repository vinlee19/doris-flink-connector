package org.apache.doris.flink.tools.cdc.hana;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.doris.flink.tools.cdc.DatabaseSync;
import org.apache.doris.flink.tools.cdc.SourceSchema;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;

public class HanaDatabaseSync extends DatabaseSync {
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
