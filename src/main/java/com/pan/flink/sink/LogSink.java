package com.pan.flink.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PublicEvolving
public class LogSink implements SinkFunction<Tuple2<String,Integer>> {

    private static Logger logger = LoggerFactory.getLogger(LogSink.class);

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        System.out.println(value.f0 + "->" + value.f1);
        logger.info(value.f0 + "->" + value.f1);
    }
}
