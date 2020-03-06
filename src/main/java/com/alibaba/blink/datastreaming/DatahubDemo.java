package com.alibaba.blink.datastreaming;


import com.alibaba.flink.connectors.datahub.datastream.source.DatahubSourceFunction;
import com.aliyun.datahub.client.model.RecordEntry;
import com.aliyun.datahub.client.model.TupleRecordData;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;

public class DatahubDemo implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(DatahubDemo.class);

    //设置datahub相关的参数，注意在本地测试时需要使用公网访问endpoint，在bayes上运行时建议使用内网endpoint
    private static String endPoint = "inner endpoint";//内网访问
//    private static String endPoint ="public endpoint";//公网访问
    private static String projectName = "yourProject";
    private static String topicSourceName = "yourTopic";
    private static String accessId = "yourAK";
    private static String accessKey = "yourAS";
    private static Long datahubStartInMs = 0L;//设置消费的启动位点对应的时间

    public static void main(String[] args) throws Exception {
        DatahubDemo datahubDemo = new DatahubDemo();
    //用于通过bayes作业参数获取对应datahub相关参数，若在代码中已经配置，可不使用
//        final ParameterTool params = ParameterTool.fromArgs(args);
//        String configFilePath = params.get("configFile");
//        Properties properties = new Properties();
//        properties.load(new StringReader(new String(Files.readAllBytes(Paths.get(configFilePath)), StandardCharsets.UTF_8)));
//        datahubStartInMs = Long.valueOf((String) properties.get("blink.job.datahubstartms"));
        datahubDemo.runExample();
    }

    public void runExample() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //set Source Function
        DatahubSourceFunction datahubSource =
                new DatahubSourceFunction(endPoint, projectName, topicSourceName, accessId, accessKey, datahubStartInMs,
                        Long.MAX_VALUE, 1, 1, 1);

        env.addSource(datahubSource)
                .flatMap(new FlatMapFunction<List<RecordEntry>, Tuple4<String,String,Double,Long>>() {
                    public void flatMap(List<RecordEntry> ls, Collector<Tuple4<String,String,Double,Long>> collector) throws Exception {
                        for(RecordEntry recordEntry : ls){
                            collector.collect(getTuple4(recordEntry));
                        }
                    }
                })
                .returns(new TypeHint<Tuple4<String,String,Double,Long>>() {})
                .print()
                .setParallelism(1);

        env.execute("datahub_demo");
    }

    private Tuple4<String,String,Double, Long> getTuple4(RecordEntry recordEntry) {
        Tuple4<String,String,Double, Long> tuple4 = new Tuple4<String, String, Double, Long>();
        TupleRecordData recordData = (TupleRecordData) (recordEntry.getRecordData());
        tuple4.f0 = (String) recordData.getField(0);
        tuple4.f1 = (String) recordData.getField(1);
        tuple4.f2 = (Double) recordData.getField(2);
        tuple4.f3 = (Long) recordData.getField(3);
        return tuple4;
    }
}