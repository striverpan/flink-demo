package com.pan.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> lines = env.socketTextStream("127.0.0.1", 9000, '\n', 1);
        DataStream<Tuple2<String, Integer>> wordsCount = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Integer>> collector) {
                String[] words = lines.split("\\s");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).keyBy(0);

        wordsCount.print();
        env.execute("WordCount");
    }
}
