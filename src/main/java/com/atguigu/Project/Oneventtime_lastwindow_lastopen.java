package com.atguigu.Project;

import com.atguigu.Bean.Sensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Oneventtime_lastwindow_lastopen
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/9 11:36
 */


public class Oneventtime_lastwindow_lastopen {
    public static void main(String[] args) {


    }

    public static class MywatermarkStrategy implements WatermarkGenerator<Sensor> {
        Long outoftime=3000L;
        private Long maxwatermark =Long.MIN_VALUE + outoftime + 1L;
       private Tuple2<Long,Boolean> state= org.apache.flink.api.java.tuple.Tuple2.of(0L,true);
        @Override
        public void onEvent(Sensor event, long eventTimestamp, WatermarkOutput output) {
            maxwatermark=Math.max(maxwatermark,event.getTs());
            state=Tuple2.of(System.currentTimeMillis(),false);

        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {

            if (System.currentTimeMillis()- state.f0>50000L){
                output.emitWatermark(new Watermark(maxwatermark+5000L));
                state.f1=true;

            }else {
                output.emitWatermark(new Watermark(maxwatermark-outoftime-1L));
            }


        }
    }
}
