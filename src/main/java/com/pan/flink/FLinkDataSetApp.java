package com.pan.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FLinkDataSetApp {

    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> lines = env.fromElements(
                "FlinkSparkStorm",
                "FlinkFlinkFlink",
                "SparkSparkSpark",
                "StormStormStorm"
        );
        DataSet words = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = line.split(",");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).groupBy(0).sum(1);

        words.printToErr();
    }
}
