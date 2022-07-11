package com.atguigu.Cep;

import com.atguigu.Bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @PackageName:com.atguigu.Cep
 * @ClassNmae:Login_Warnning
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/11 16:54
 *
 * 恶意登录检测
 * 用户2秒内连续两次登录失败则判定为恶意登录
 *
 */


public class Login_Warnning {
    public static void main(String[] args) throws Exception {
        // TODO 1.创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.从文件读取数据
        DataStreamSource<String> textFileDs = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\LoginLog.csv");

        //TODO 3.将数据格式转换成JavaBean
        SingleOutputStreamOperator<LoginEvent> mapwithWmDs = textFileDs.map(new MapFunction<String, LoginEvent>() {
            @Override
            public LoginEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new LoginEvent(Long.parseLong(split[0]),split[1],split[2],Long.parseLong(split[3]));
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
            @Override
            public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                return element.getEventTime();
            }
        }));

        //TODO 4.按照用户id进行分组
        KeyedStream<LoginEvent, Tuple> keyedStream = mapwithWmDs.keyBy("userId");

        //TODO 5.定义模式序列
        Pattern<LoginEvent, LoginEvent> pattern = Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent value) throws Exception {
                        return "fail".equals(value.getEventType());
                    }
                })
                .times(2)
                .consecutive()
                .within(Time.seconds(2));

        //TODO 6.将模式序列作用在流上

        PatternStream<LoginEvent> pattern1 = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取匹配到的结果
        SingleOutputStreamOperator<String> select = pattern1.<String>select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> start = pattern.get("start");
                LoginEvent begin = start.get(0);
                LoginEvent end = start.get(1);
                return "用户" + begin.getUserId() + "在" + begin.getEventTime() + "到" + end.getEventTime() + "内连续两次失败，定义为恶意登录";
            }
        });

        //TODO 8.打印输出
        select.print();

        //TODO 9.启动作业
        env.execute();


    }

}
