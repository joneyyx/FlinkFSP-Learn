package com.fsp.personal.streamapi.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.*;

/**
 * @Description 从文件中读取; 从集合中读取; 从元素读取数据
 * @Author ZhongYangyixiong
 * @Date 2022/5/31 9:35 PM
 */
public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1. 从文件中读取-->String
        // 最常见的读取有界流
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        // 2. 从集合中读取-->多用于测试
        List<Integer> nums = Arrays.asList(2, 5);
        DataStreamSource<Integer> numStreams = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary", "./home", 1000L));
        events.add(new Event("Henry", "./PornHub", 100L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        // 3. 从元素读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Henry", "./PornHub", 100L)
        );

        // 4. Socket文本流
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 9999);


        //--------------------------------------------------------------------------
        stream1.print("1");
        numStreams.print("nums");
        stream2.print("2");
        stream3.print("3");

        //--------------------------------------------------------------------------
        env.execute();

    }


}
