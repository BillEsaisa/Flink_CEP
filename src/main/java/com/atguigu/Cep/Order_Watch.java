package com.atguigu.Cep;

import com.atguigu.Bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @PackageName:com.atguigu.Cep
 * @ClassNmae:Order_Watch
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/11 20:05
 *
 *
 * 统计创建订单到下单中间超过15分钟的超时数据以及正常的数据
 */


public class Order_Watch {
    public static void main(String[] args) throws Exception {
        //TODO 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取文件数据
        DataStreamSource<String> readTextFile = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\OrderLog.csv");


        //TODO 3.将数据格式转化为JavaBean
        SingleOutputStreamOperator<OrderEvent> mapwithWmDs = readTextFile.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
            @Override
            public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                return element.getEventTime();
            }
        }));


        //TODO 4.按照用户id 进行分组
        KeyedStream<OrderEvent, Tuple> keyedStream = mapwithWmDs.keyBy("orderId");

        //TODO 5.创建模式序列
        Pattern<OrderEvent, OrderEvent> pattern = Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                })
                .next("end")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(15));

        //TODO 6.将模式序列作用在流上

        PatternStream<OrderEvent> pattern1 = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取匹配到的数据
        OutputTag<OrderEvent> outputTag = new OutputTag<OrderEvent>("TimeOut") {
        };
        SingleOutputStreamOperator<List<OrderEvent>> select = pattern1.select(outputTag, new PatternTimeoutFunction<OrderEvent, OrderEvent>() {
            @Override
            public OrderEvent timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                OrderEvent start = pattern.get("start").get(0);
//                OrderEvent end = pattern.get("end").get(0);
                return start;
            }
        }, new PatternSelectFunction<OrderEvent, List<OrderEvent>>() {
            @Override
            public List<OrderEvent> select(Map<String, List<OrderEvent>> pattern) throws Exception {
                OrderEvent start = pattern.get("start").get(0);
                OrderEvent end = pattern.get("end").get(0);
                ArrayList<OrderEvent> result = new ArrayList<>();
                result.add(start);
                result.add(end);

                return result;
            }
        });



        //TODO 8.打印流
        select.print("NormalData>>>>>>>>>>>>>>>>>");
        select.getSideOutput(outputTag).print("TimeOutData>>>>>>>>>>>>>>>>>>>>>>>");


        //TODO 9.启动任务
        env.execute();







    }

}
