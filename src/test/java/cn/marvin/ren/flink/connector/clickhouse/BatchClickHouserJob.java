package cn.marvin.ren.flink.connector.clickhouse;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

public class BatchClickHouserJob {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance().inBatchMode()
                .build();

        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.executeSql("CREATE TABLE metrics (" +
                "metric_code STRING, " +
                "instance_code STRING, " +
                "ts TIMESTAMP(3), " +
                "v FLOAT" +
                ") WITH (" +
                "'connector' = 'clickhouse-r',\n" +
                "'url' = 'jdbc:clickhouse://192.168.48.12:8123/aiops',\n" +
                "'username' = 'aiops',\n" +
                "'password' = 'aiops_2021',\n" +
                "'table-name' = 'f_metrics_ods'\n" +
                ")"
        );

        final Table tab = tEnv.sqlQuery("select * from metrics WHERE metric_code = 'instance:node_cpu:avg_rate5m' AND instance_code='192.168.48.15' ORDER BY ts asc LIMIT 200");
        tab.execute().print();


    }
}
