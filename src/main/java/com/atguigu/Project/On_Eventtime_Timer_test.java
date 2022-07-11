package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Random;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Timer_test
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/7 17:39
 *
 * 代码中实现定时器(状态编程)
 * 需求：检测传感器温度变化值，如果温度连续十秒上升，则输出报警信息（侧输出流输出）
 */


public class On_Eventtime_Timer_test {
    public static void main(String[] args) throws Exception {
        //创建流处理的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认时间就是处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //从自定义source里读取数据
        DataStreamSource<Sensor> addSource = env.addSource(new Mysource());
        SerializableTimestampAssigner<Sensor> serializableTimestampAssigner = new SerializableTimestampAssigner<Sensor>() {
            @Override
            public long extractTimestamp(Sensor element, long recordTimestamp) {
                return element.getTs();
            }
        };
        //设置watermark 生成策略，以及时间戳
        addSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Sensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(serializableTimestampAssigner));




        //按照传感器进行分组
        KeyedStream<Sensor, Tuple> keyedStream = addSource.keyBy("id");

        //使用定时器和状态api完成需求
        keyedStream.process(new KeyedProcessFunction<Tuple, Sensor, String>() {
            //定义状态
            private ValueState<Double>  TempState;
            private ValueState<Long> TimerState;
            @Override
            public void open(Configuration parameters) throws Exception {
                //注册状态
//              TempState= getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_value",Double.class,Double.MIN_VALUE));

                TempState= getRuntimeContext().getState(new ValueStateDescriptor<Double>("last_temp",TypeInformation.<Double>of(Double.class),Double.MIN_VALUE));


                TimerState=getRuntimeContext().getState(new ValueStateDescriptor<Long>("Timer_state",Long.class));
            }

            @Override
            public void processElement(Sensor value, KeyedProcessFunction<Tuple, Sensor, String>.Context ctx, Collector<String> out) throws Exception {
                System.out.println(ctx.timestamp());
                ctx.timerService().currentWatermark();
                //取出状态
                Double lasttemp = TempState.value();
                Long Timerts=TimerState.value();

                //设置定时器时间
                Long ts= ctx.timerService().currentWatermark()+10000L;

                //第一条数据进来必然大于上一条记录的温度值，直接可以创建一个Timer,不需要判空
                if(value.getValue()>lasttemp && Timerts==null){
                    //注册一个定时器
                    ctx.timerService().registerEventTimeTimer(ts);
                    //更新定时器状态
                    TimerState.update(ts);
                    //更新温度值状态
                    TempState.update(value.getValue());

                }

                if (value.getValue()>lasttemp && Timerts!=null){
                    //不需要重复注册定时器，更新温度值状态
                    TempState.update(value.getValue());
                }
                //如果后续的温度值小于上一个温度值，删除定时器，所以此时必须定时器状态不为空
                if (value.getValue()<lasttemp && Timerts!=null){
                    //删除定时器
                    ctx.timerService().deleteEventTimeTimer(Timerts);
                    //清空定时器状态
                    TimerState.clear();
                    //此时的温度状态不需要清空，更新状态
                    TempState.update(value.getValue());

                }


            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<Tuple, Sensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                //定时器任务
                out.collect("传感器"+ctx.getCurrentKey().getField(0)+"连续十秒温度上升警报");
                //清空定时器状态
                TimerState.clear();

            }
        }).print();

        //提交job
        env.execute();


    }
    public static class Mysource implements ParallelSourceFunction<Sensor>{

        Random random = new Random();
        Boolean bool =true;

        @Override
        public void run(SourceContext<Sensor> ctx) throws Exception {

            while (bool){
                ctx.collect(new Sensor("sensor"+random.nextInt(1000),random.nextLong(),random.nextDouble()*100));
                Thread.sleep(1000);

            }


        }

        @Override
        public void cancel() {
            bool=false;

        }
    }

}
