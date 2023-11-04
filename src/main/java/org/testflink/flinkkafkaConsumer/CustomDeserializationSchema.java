package org.testflink.flinkkafkaConsumer;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.testflink.debezium.DebeziumSchema;

import java.io.IOException;

public class CustomDeserializationSchema implements KafkaDeserializationSchema<Object> {

    private ObjectMapper mapper;


    @Override
    public boolean isEndOfStream(Object nextElement) {
        return false;
    }

    @Override
    public DebeziumSchema deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        DebeziumSchema cdcDebeziumEvent = null;
        try {
            if (record.value() != null) {
                cdcDebeziumEvent = mapper.readValue(record.value(), DebeziumSchema.class);
            }
        } catch (Exception e) {
            return null;
        }
        return cdcDebeziumEvent;
    }


    @Override
    public TypeInformation<Object> getProducedType() {
        return null;
    }
}
