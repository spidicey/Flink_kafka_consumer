package org.testflink.flinkkafkaConsumer;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


public class KafkaConsumer {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    private static final String kafkaSever = "10.1.9.77:19092";
    private static final String kafkaTopic = "postgres-connect.public.raw_product";
    //private static JSONDeserialiser jsonMapper = new JSONDeserialiser();

    public static void main(String[] args) throws Exception {
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                Time.of(10, TimeUnit.SECONDS) // delay
        ));
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaSever); // set Kafka server
        properties.setProperty("auto.offset.reset", "earliest");

//        JSONDeserializationSchema jsonDeserializationSchema = JSONDeserializationSchema.builder()
//                .failOnMissingField(true)
//                .ignoreParseErrors()
//                .build();
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                kafkaTopic,
                new SimpleStringSchema(),
                properties
        );
        DataStream<String> stream = env.addSource(kafkaConsumer);

//        stream.filter(Objects::nonNull).map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                System.out.println(value);
//                return value;
//            }
//        });
        DataStream<String> filteredOrders = stream.map(element -> {
            JSONObject json = new JSONObject(element);
            if (!json.isNull("after")){
                System.out.println(element);
                JSONObject technology = json.getJSONObject("after");
                System.out.println(technology);
            }
            else {
                System.out.println(element);
                System.out.println("NULLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL");
            }
            return element;
        });
//        stream.print();
//        filteredOrders.print();
        env.execute("ádfc");
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