package com.fsp.personal.streamapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description
 * @Author ZhongYangyixiong
 * @Date 2022/6/1 9:39 PM
 */
public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 自定义数据源
        DataStreamSource<Event> customerSource = env.addSource(new ClickSource()).setParallelism(2);

        customerSource.print("customization");

        env.execute();
    }

}
