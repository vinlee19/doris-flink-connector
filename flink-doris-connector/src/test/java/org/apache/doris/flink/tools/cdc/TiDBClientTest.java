package org.apache.doris.flink.tools.cdc;

import com.ververica.cdc.connectors.tidb.TDBSourceOptions;
import com.ververica.cdc.connectors.tidb.TiDBSource;
import com.ververica.cdc.connectors.tidb.TiKVChangeEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.connectors.tidb.table.StartupOptions;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

public class TiDBClientTest {
    public static void main(String[] args) throws Exception {

        Map<String,String> map = new HashMap<>();
        map.put("tikv.grpc.timeout_in_ms","20000");
        SourceFunction<String> tidbSource =
                TiDBSource.<String>builder()
                        .database("sf_test") // set captured database
                        .tableName("nation")
                        .startupOptions(StartupOptions.initial())// set captured table
                        .tiConf(
                                TDBSourceOptions.getTiConfiguration(
                                        "10.16.10.6:2379", map))
                        .snapshotEventDeserializer(
                                new TiKVSnapshotEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Kvrpcpb.KvPair record, Collector<String> out)
                                            throws Exception {
                                        ByteString key = record.getKey();
                                        ByteString value = record.getValue();
                                        String s = key.toStringUtf8();
                                        String v = value.toStringUtf8();
                                        System.out.println(s + " : " + v);
                                        out.collect(record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .changeEventDeserializer(
                                new TiKVChangeEventDeserializationSchema<String>() {
                                    @Override
                                    public void deserialize(
                                            Cdcpb.Event.Row record, Collector<String> out)
                                            throws Exception {
                                        out.collect(record.toString());
                                    }

                                    @Override
                                    public TypeInformation<String> getProducedType() {
                                        return BasicTypeInfo.STRING_TYPE_INFO;
                                    }
                                })
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // enable checkpoint
        env.enableCheckpointing(10000);
        env.addSource(tidbSource).print().setParallelism(1);

        env.execute("Print TiDB Snapshot + Binlog");
    }
}
