package com.atguigu.Cep;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @PackageName:com.atguigu.Cep
 * @ClassNmae:Test1
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/11 13:52
 *
 * 需求：连续三条记录温度大于30.输出报警信息
 */


public class Test1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Sensor> withWmDs = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");

                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
            @Override
            public long extractTimestamp(Sensor element, long recordTimestamp) {

                return element.getTs()*1000L;
            }
        }));

        //定义规则
        Pattern<Sensor, Sensor> pattern = Pattern.<Sensor>begin("start").where(new SimpleCondition<Sensor>() {
            @Override
            public boolean filter(Sensor value) throws Exception {

                return value.getValue() > 30;
            }
        }).times(3).within(Time.seconds(10));

        //将规则作用在流上
        PatternStream<Sensor> sensorPatternStream = CEP.<Sensor>pattern(withWmDs, pattern);

        //提取匹配到的结果
        OutputTag<List<Sensor>> timeOut = new OutputTag<>("TimeOut", Types.LIST(Types.GENERIC(Sensor.class)));
        SingleOutputStreamOperator<String> select = sensorPatternStream.select(timeOut, new PatternTimeoutFunction<Sensor,List<Sensor>>() {
            @Override
            public List<Sensor> timeout(Map<String, List<Sensor>> pattern, long timeoutTimestamp) throws Exception {
                List<Sensor> start = pattern.get("start");

                System.out.println(start.get(0));
//                System.out.println(start.get(1));
//                System.out.println(start.get(2));

                return start;
            }
        }, new PatternSelectFunction<Sensor, String>() {
            @Override
            public String select(Map<String, List<Sensor>> pattern) throws Exception {
                List<Sensor> start = pattern.get("start");
                System.out.println(start.get(0));
                System.out.println(start.get(1));
                System.out.println(start.get(2));
                return pattern.toString();
            }
        });
        select.print("select>>>>>>>>>>>>>");
        select.getSideOutput(timeOut).print("timeout>>>>>>>>>>");


        env.execute();


    }
}
