package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:On_Processingtime_Warning
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 22:21
 *
 * 监控水位传感器的水位值，如果水位值在五秒钟之内(processing time)连续上升，则报警
 */


public class On_Processingtime_Warning {
    public static void main(String[] args) throws Exception {
        //创建流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //设置watermark和分配时间戳


        //将数据格式转换为pojo
        SingleOutputStreamOperator<Sensor> map = socketTextStream.map(new MapFunction<String, Sensor>() {
            @Override
            public Sensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new Sensor(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        })
        .assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Sensor>() {
            @Override
            public long extractTimestamp(Sensor element, long recordTimestamp) {
                // System.out.println(element.getTs());
                return element.getTs() * 1000L;
            }
        }));

        //按照传感器的id进行分组
        KeyedStream<Sensor, Tuple> keyedStream = map.keyBy("id");

        //逻辑处理
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, String>() {
            //定义状态
            private  ValueState<Double> valueState;
            private ValueState<Long> TimerState;
            @Override
            public void open(Configuration parameters) throws Exception {
             //初始化状态
                valueState=getRuntimeContext().getState(new ValueStateDescriptor<>("last_value",Double.class,Double.MIN_VALUE));
                TimerState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("Timer_state",Long.class));

            }

            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, String>.Context ctx, Collector<String> out) throws Exception {
                Long ts=ctx.timestamp()+5000L;
                System.out.println(ctx.timestamp());
                //如果后面的值大于前面的值，并且定时器状态为空，那么注册一个定时器，更新定时器状态，更新值状态
                if (value.getValue()>valueState.value() && TimerState.value()==null){
                    System.out.println("注册定时器");
                    //注册一个定时器
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新定时器状态
                    TimerState.update(ts);
                    //更新值状态
//                    valueState.update(value.getValue());

                }

                //如果后面的值小于前面的值，定时器不为空，删除定时器，清空定时器状态，更新值状态
                if (value.getValue()<=valueState.value() && TimerState.value()!=null){
                    System.out.println("删除定时器");
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(TimerState.value());
                    //清空定时器状态
                    TimerState.clear();
                    //更新值状态
//                    valueState.update(value.getValue());

                }

                //持续更新值状态
                valueState.update(value.getValue());





            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, Sensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                System.out.println(timestamp);
                //定时器触发操作
                out.collect("温度持续五秒上升警报");
                //清空定时器状态
                TimerState.clear();


            }

            @Override
            public void close() throws Exception {
                TimerState.clear();
                valueState.clear();
            }
        }).print("温度报警");

        //提交job
        env.execute();
    }
}