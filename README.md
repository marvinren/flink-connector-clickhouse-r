# flink clickhouse connector source

本工程是fink的clickhouse的读取的库，目前还属于实验版，可以实现初步的读取

PS：该连接库使用的是flink **1.13.2**, 请匹配版本使用

## 编译生成
```
mvn clean package
```
生成的库在`out/flink-connector-clickhouse-r-0.1.jar`下

## flink中使用
可以通过SQL直接建立source表，然后通过sql读取表中的对应的数据，如下是调用例子：
```java

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
```

## 相关参数说明

|名称|描述|备注|
|:-----|:-----|:-----|
|connector|连接器名称|clickhouse-r|
|url|jdbc的连接地址|jdbc:clickhouse://xxxxxx:xxx|
|username|jdbc连接的用户名||
|password|jdbc连接的密码||
|table-name|clickhouse的表名||
|database-name|clickhouse的库名|默认适用default库|



# 参考工程
* https://github.com/gmmstrive/flink-connector-clickhouse
* https://github.com/ivi-ru/flink-clickhouse-sink
* https://github.com/liekkassmile/flink-connector-clickhouse-1.13