package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:SplitStream_OnSlideoutput
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/8 0:28
 *
 *
 * 基于侧输出流的分流操作（高于30的输出到主流，其他温度输出到侧输出流）
 */


public class SplitStream_OnSlideoutput {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认时间就是处理时间


        //从自定义source里读取数据
        DataStreamSource<Sensor> addSource = env.addSource(new On_precessingtime_Timer_test.Mysource());

        //分流操作
        SingleOutputStreamOperator<Sensor> process = addSource.process(new ProcessFunction<Sensor, Sensor>() {
            @Override
            public void processElement(Sensor value, ProcessFunction<Sensor, Sensor>.Context ctx, Collector<Sensor> out) throws Exception {
                if (value.getValue() > 50) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<Sensor>("low_temp") {
                    }, value);
                }

            }
        });
        process.print("主流——高温流");
        process.getSideOutput(new OutputTag<Sensor>("low_temp") {
        }).print("侧输出流-低温流");

        //提交job
        env.execute();
    }



}
