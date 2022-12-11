package com.leolian.fink.learning.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {

    private static final String[] WORDS = new String[]{
            "To be, or not to be,--that is the question:--",
            "Whether 'tis nobler in the mind to suffer"
    };

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));

        env.fromElements(WORDS)
                // 将字符串进行分隔然后收集，组装后的数据格式是 (word、1)，1 代表 word 出现的次数为 1
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] splits = value.toLowerCase().split("\\W+");
                        for (String split : splits) {
                            if (split.trim().length() > 0) {
                                out.collect(new Tuple2<>(split, 1));
                            }
                        }
                    }
                })
                // 根据 word 关键字进行分组（0 代表对第一个字段分组，也就是对 word 进行分组）
                .keyBy(0)
                // 对单个 word 进行计数操作
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> new Tuple2<>(value1.f0, value1.f1 + value1.f1))
                // 打印所有的数据流，格式是 (word，count)，count 代表 word 出现的次数
                .print();

        env.execute("leolian — word count streaming demo");
    }

}
