package com.pan.flink;



public class FlinkStreamingApp {

    public static void main(String[] args) throws Exception {
        //new FraudDetectionJob().execute();
        new LocalFileJob().execute();
    }
}
