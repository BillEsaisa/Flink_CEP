package com.atguigu.Transform;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @PackageName:com.atguigu.Transform
 * @ClassNmae:FilterFromAddSource
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 7:35
 */


public class FilterFromAddSource {
    public static void main(String[] args) throws Exception {
        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 从自定义Source 读取数据
        DataStreamSource<Sensor> addSource = env.addSource(new MySource());

        //TODO 将sensorID 以“sensor1”开头的数据过滤掉
        SingleOutputStreamOperator<Sensor> filter = addSource.filter(new FilterFunction<Sensor>() {
            @Override
            public boolean filter(Sensor sensor) throws Exception {
                boolean bool = sensor.getId().startsWith("sensor1");
                return !bool;
            }
        });

        //打印流
        filter.print();

        //提交job
        env.execute();

    }


    public static class MySource implements ParallelSourceFunction<Sensor>{
        private Random random=new Random();
        private Boolean isrunning=true;
        @Override
        public void run(SourceContext<Sensor> sourceContext) throws Exception {
            while (isrunning){
                sourceContext.collect(new Sensor("sensor"+random.nextInt(15),random.nextLong(),random.nextDouble()*100));
                Thread.sleep(200);
            }


        }

        @Override
        public void cancel() {
            isrunning=false;
        }
    }
}
