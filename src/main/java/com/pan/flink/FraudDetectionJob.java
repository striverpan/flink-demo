package com.pan.flink;

import com.pan.flink.common.JobInterface;
import com.pan.flink.process.FraudDetector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.walkthrough.common.entity.Alert;
import org.apache.flink.walkthrough.common.entity.Transaction;
import org.apache.flink.walkthrough.common.sink.AlertSink;
import org.apache.flink.walkthrough.common.source.TransactionSource;

public class FraudDetectionJob implements JobInterface {
    @Override
    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Transaction> dataStream = env.addSource(new TransactionSource()).name("transaction");

        DataStream<Alert> alerts = dataStream
                .keyBy(Transaction::getAccountId)
                .process(new FraudDetector())
                .name("fraud-detector");

        alerts.addSink(new AlertSink()).name("sink-alert");

        env.execute("Fraud-Detector");
    }
}
