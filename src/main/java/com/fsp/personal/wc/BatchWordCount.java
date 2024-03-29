package com.fsp.personal.wc;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Description 批处理
 * @Author ZhongYangyixiong
 * @Date 2022/5/16 9:31 PM
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文件中读取数据
        DataSource<String> lineDataSource = env.readTextFile("input/words.txt");

        // 3. 具体处理逻辑， 每行数据进行分词，转换为2元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndNumberTuple = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            // 将一行文本进行分词
            String[] words = line.split(" ");
            // 每行单词转换为二元祖输出
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4. 按照word进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndNumberGroup = wordAndNumberTuple.groupBy(0);

        // 5. 分组内进行聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndNumberGroup.sum(1);

        // 6. 输出
        sum.print();
    }
}
