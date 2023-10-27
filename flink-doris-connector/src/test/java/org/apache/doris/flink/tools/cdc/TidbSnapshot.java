package org.apache.doris.flink.tools.cdc;

import com.ververica.cdc.connectors.shaded.org.apache.kafka.connect.source.SourceRecord;
import com.ververica.cdc.connectors.tidb.TiKVSnapshotEventDeserializationSchema;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.tikv.kvproto.Kvrpcpb;

public class TidbSnapshot implements TiKVSnapshotEventDeserializationSchema<String>, DebeziumDeserializationSchema<String> {

    @Override
    public void deserialize(Kvrpcpb.KvPair kvPair, Collector<String> collector) throws Exception {
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

    }
}
