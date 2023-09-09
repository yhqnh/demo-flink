package org.example.com.yhq.demo.flink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class chapter2 {

    public static void main(String[] args) throws Exception {
        //1.创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //2.读取文件数据
        DataStreamSource<String> lines = env.readTextFile("./data/words.txt");
        //3.切分单词，设置KV格式数据
        SingleOutputStreamOperator<Tuple2<String, Long>> kvWordsDS =
                lines.flatMap((String line, Collector<Tuple2<String, Long>> collector) -> {
                    String[] words = line.split(" ");
                    for (String word : words) {
                        collector.collect(Tuple2.of(word, 1L));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //4.分组统计获取 WordCount 结果
        kvWordsDS.keyBy(tp -> tp.f0).sum(1).print();
        //5.流式计算中需要最后执行execute方法
        env.execute();
    }
}
