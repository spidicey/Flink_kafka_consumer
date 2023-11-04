package org.testflink.flinkkafkaConsumer;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaSever);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // number of restart attempts
                org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS) // delay
        ));
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafkaSever)
                .setTopics(kafkaTopic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setBounded(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new org.apache.flink.api.common.serialization.SimpleStringSchema())
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringDeserializer.class))
                .build();

        DataStream<String> stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

//        stream.windowAll(TumblingEventTimeWindows.of(Time.seconds(100))).process();
//        DataStream<Tuple2<String, String>> tupleStream = stream.map(new JsonToTupleMapper());
//        tupleStream.print();
        stream.filter(Objects::nonNull).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                JSONObject json = new JSONObject(value);
                Result result = null;
                if (!json.isNull("after")) {
//                System.out.println(element);
                    JSONObject after = json.getJSONObject("after");
                    result = new Result();
                    result.product_code = after.getString("product_code");
                    result.product_name = after.getString("product_name");
                    result.category_id = after.getInt("category_id");
                    result.original_price = after.getDouble("original_price");
                    if (!after.isNull("unit")) {
//                        System.out.print(after.getString("unit"));
                        result.unit = after.getString("unit");
                    }
                    System.out.println(result);
//                System.out.println(after);
                } else {
                    System.out.println(value);
                    System.out.println("NULL");
                }
                return value;
            }
        });
        env.execute("kafka consumer message unbounded Stream");
    }
//    public static class JSONParser implements MapFunction<Tuple5<String, String, Double, Integer, String>, Result> {
//        @Override
//        public Result map(Tuple5<String, String, Double, Integer, String> value) throws Exception {
//            Result result = new Result();
//            result.product_code = value.f0;
//            result.product_name = value.f1;
//            result.category_id = value.f2;
//            result.quantity_output = value.f3;
//            result.sale_time = value.f4;
//            // You can further process the parsed data or perform any necessary calculations here
//            return result;
//        }
//    }

    // Define a class to hold the parsed data
    public static class Result {
        public String product_code;

        @Override
        public String toString() {
            return "Result{" +
                    "product_code='" + product_code + '\'' +
                    ", product_name='" + product_name + '\'' +
                    ", category_id=" + category_id +
                    ", original_price=" + original_price +
                    ", unit='" + unit + '\'' +
                    '}';
        }

        public String product_name;
        public Integer category_id;
        public Double original_price;
        public String unit;
    }
}