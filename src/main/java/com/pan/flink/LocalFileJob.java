package com.pan.flink;

import com.pan.flink.common.JobInterface;
import com.pan.flink.sink.LogSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class LocalFileJob implements JobInterface {

    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> dataStream = env.readTextFile("/Users/panx/IdeaProjects/flink-demo/src/main/resources/data.txt");

        DataStream wordCount = dataStream
                .flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        for (String word : s.split(" ")) {
                            collector.collect(new Tuple2<String,Integer>(word, 1));
                        }
                    }
                })
                .keyBy(value -> value.f0)
                .sum(1);

        //wordCount.print();
        wordCount.addSink(new LogSink()).name("sink-alert");

        env.execute("Fraud-Detector");
    }
}
