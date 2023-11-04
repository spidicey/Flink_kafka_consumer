package org.testflink.flinkkafkaConsumer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.testflink.debezium.DebeziumSchema;

import java.util.Properties;

public class KafkaConsumerSchema {
    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final String kafkaSever = "10.1.9.77:19092";
    private static final String kafkaTopic = "postgres-connect.public.raw_product";

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaSever);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//        DataStream<Object> sourceDataMapStream = null;
//        sourceDataMapStream = env
//                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, new CustomDeserializationSchema(), properties))
//                .name("kafka source").setParallelism(4);
        TypeInformation<Object> typeInfo = TypeInformation.of(Object.class);
        DataStream<Object> sourceDataMapStream = env
                .addSource(new FlinkKafkaConsumer<>(kafkaTopic, new CustomDeserializationSchema(), properties), typeInfo)
                .name("kafka source");
        sourceDataMapStream.print();

        env.execute("kafka consumer message unbounded Stream");
    }
}
