# flink clickhouse connector source

本工程是fink的clickhouse的读取的库，目前还属于实验版，可以实现初步的读取

PS：该连接库使用的是flink **1.13.2**, 请匹配版本使用

## 编译生成
```
mvn clean package
```
生成的库在`out/flink-connector-clickhouse-r-0.1.jar`下


# 参考工程
* https://github.com/gmmstrive/flink-connector-clickhouse
* https://github.com/ivi-ru/flink-clickhouse-sink
* https://github.com/liekkassmile/flink-connector-clickhouse-1.13