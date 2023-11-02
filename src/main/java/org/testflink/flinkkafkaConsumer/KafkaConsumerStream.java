package org.testflink.flinkkafkaConsumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaConsumerStream {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final String kafkaSever = "10.1.9.77:19092";
    private static final String kafkaTopic = "postgres-connect.public.raw_product";
    //private static JSONDeserialiser jsonMapper = new JSONDeserialiser();

    public static void main(String[] args) throws Exception {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaSever)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        stream.filter(Objects::nonNull).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {

                JSONObject json = new JSONObject(value);
                if (!json.isNull("after")) {
                    System.out.println(value);
                    JSONObject technology = json.getJSONObject("after");
                    System.out.println(technology);
                } else {
                    System.out.println(value);
                    System.out.println("NULLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
                }
                return value;
            }
        });
        env.execute("kafka consumer message unbounded Stream");
    }


    private static FlinkKafkaConsumer<String> getDataSource(String topicName, final Properties properties) {
        return new FlinkKafkaConsumer<>(topicName, getKafkaMessageDeserialiser(),
                properties);
    }

    private static Properties getKafkaConsumerProperties(final ParameterTool parameterTool) {
        return parameterTool.getProperties();
    }

    /**
     * Gets deserialiser that's used to convert between Kafka's byte messages and Flink's objects
     *
     * @return
     */
    private static SimpleStringSchema getKafkaMessageDeserialiser() {
        return new SimpleStringSchema();
    }

    /**
     * Returns the name of a topic from which the subscriber should consume messages
     *
     * @param parameterTool
     * @return
     */
    private static String getTopicName(final ParameterTool parameterTool) {
        return parameterTool.getRequired("topic");
    }

    public static class FilterNull implements FilterFunction<String> {
        @Override
        public boolean filter(String value) {
            return value != null;
        }
    }
}