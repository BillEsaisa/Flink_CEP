package com.atguigu.windows;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Collection;
import java.util.Properties;

/**
 * @PackageName:com.atguigu.windows
 * @ClassNmae:WindowsOpration
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/1 5:22
 */


public class WindowsOpration {
    public static void main(String[] args) throws Exception {
        //创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(2);


        //从socekt端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //转换数据格式
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] s = value.split(" ");
                return new Sensor(s[0], Long.parseLong(s[1]), Double.parseDouble(s[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
            @Override
            public long extractTimestamp(Sensor element, long recordTimestamp) {

                return element.getTs()*1000L;
            }
        }));

        //按照传感器id进行分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //开窗（滚动窗口，窗口大小为5s）
        WindowedStream<Sensor, Tuple, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //增量聚合
        SingleOutputStreamOperator<Sensor> reduce = window.reduce(new ReduceFunction<Sensor>() {
            @Override
            public Sensor reduce(Sensor value1, Sensor value2) throws Exception {

                return new Sensor(value1.getId(), value2.getTs(), value1.getValue() + value2.getValue());
            }
        });

        //打印输出
        reduce.print();

        //提交job
        env.execute();

    }


}
