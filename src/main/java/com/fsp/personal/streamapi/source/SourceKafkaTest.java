package com.fsp.personal.streamapi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description
 * @Author ZhongYangyixiong
 * @Date 2022/6/1 9:41 PM
 */
public class SourceKafkaTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 5. Kafka数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        DataStreamSource<String> kafakaStream = env.addSource(
                // 创建kafak消费者
                new FlinkKafkaConsumer<String>("clicks-topic", new SimpleStringSchema(), properties)
        );

        env.execute();

    }
}
