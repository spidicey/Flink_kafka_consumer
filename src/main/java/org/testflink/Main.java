package org.testflink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public class Main {

    private static final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "10.1.9.97:9092"); // Thay đổi thành máy chủ Kafka thực tế
//        properties.setProperty("group.id", "flink-consumer-group"); // idNhóm tiêu thụ
        properties.setProperty("auto.offset.reset", "earliest");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<PizzaOrder> orders = env.addSource(new PizzaOrderGenerator());
        DataStream<PizzaOrder> filteredOrders = orders
                // keep only those rides and both start and end in NYC
                .filter(new ShippedFilter());
        System.out.println("**************************************************************");

        filteredOrders.print();
        env.execute("Flink Streaming Java API Skeleton");
    }
    public static class ShippedFilter implements FilterFunction<PizzaOrder> {
        @Override
        public boolean filter(PizzaOrder Order) {
            return Order.status.equals("Shipped");
        }
    }

}