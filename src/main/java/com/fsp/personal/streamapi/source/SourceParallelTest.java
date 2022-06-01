package com.fsp.personal.streamapi.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * @Description
 * @Author ZhongYangyixiong
 * @Date 2022/6/1 9:19 PM
 */
public class SourceParallelTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 自定义并行数据源
        DataStreamSource<Integer> parallelSource = env.addSource(new ParallelCustomSource());

        parallelSource.print();

        env.execute();

    }

    // 实现自定义的并行sourceFunction
    public static class ParallelCustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                ctx.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {

        }
    }
}
