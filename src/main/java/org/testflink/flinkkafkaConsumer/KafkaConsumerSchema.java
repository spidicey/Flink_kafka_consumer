package org.testflink.flinkkafkaConsumer;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.testflink.debezium.DebeziumSchema;

import java.util.Properties;

public class KafkaConsumerSchema {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final String kafkaSever = "10.1.9.77:19092";
    private static final String kafkaTopic = "postgres-connect.public.raw_product";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaSever);
        DataStream<String> sourceDataMapStream = null;
        sourceDataMapStream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, new org.apache.flink.api.common.serialization.SimpleStringSchema(), properties))
                .name("kafka source");
        sourceDataMapStream.print();
        env.execute("kafka consumer message unbounded Stream");
    }
}
