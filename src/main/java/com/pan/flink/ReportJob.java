package com.pan.flink;

import com.pan.flink.common.JobInterface;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class ReportJob implements JobInterface {

    @Override
    public void execute() throws Exception {

        EnvironmentSettings settings = EnvironmentSettings.newInstance().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.sqlQuery("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tEnv.sqlQuery("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
                "   'table-name' = 'spend_report',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'sql-demo',\n" +
                "   'password'   = 'demo-sql'\n" +
                ")");

    }
}
