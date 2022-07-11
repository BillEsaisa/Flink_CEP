package com.atguigu.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @PackageName:com.atguigu.source
 * @ClassNmae:KafkaSource
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 2:29
 */


public class KafkaSource {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092,hadoop103:9092,hadoop104:9092");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
        properties.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG,"true");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"para");
        DataStreamSource<String> kafkadatastream = env.addSource(new FlinkKafkaConsumer<String>("para", new SimpleStringSchema(), properties)).setParallelism(3);

        //打印流
        kafkadatastream.print();

        //提交job
        env.execute("KafkaSource");


    }
}
