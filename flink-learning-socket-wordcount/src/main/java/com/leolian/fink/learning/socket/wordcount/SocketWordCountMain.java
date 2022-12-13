package com.leolian.fink.learning.socket.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketWordCountMain {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>");
            return;
        }

        String hostname = args[0];
        int port = Integer.parseInt(args[1]);

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = environment.socketTextStream(hostname, port);

        SingleOutputStreamOperator<Tuple2<String, Integer>> operator = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] array = s.toLowerCase().split("\\W+");
                for (String val : array) {
                    if (val.trim().length() > 0) {
                        collector.collect(new Tuple2<>(val, 1));
                    }
                }
            }
        })
                .keyBy(0)
                .sum(1);

        operator.print();

        environment.execute("Socket wordCount");
    }

}
