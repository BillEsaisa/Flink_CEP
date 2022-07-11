package com.atguigu.Project;

import com.atguigu.Bean.OrderEvent;
import com.atguigu.Bean.TxEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @PackageName:com.atguigu.Project
 * @ClassNmae:Order
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 19:17
 *
 * 需求:实时对账
 *
 *
 */


public class Order {


    public static void main(String[] args) throws Exception {

        //创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从文件读取日志数据
        DataStreamSource<String> orderStream = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\OrderLog.csv");
        DataStreamSource<String> TxStream = env.readTextFile("E:\\Ideaproject\\Flink_0212\\src\\ReceiptLog.csv");
        //将数据转换成javabean 方便对数据的处理
        SingleOutputStreamOperator<OrderEvent> ordermap = orderStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");

                return new OrderEvent(Long.parseLong(split[0]), split[1], split[2], Long.parseLong(split[3]));
            }
        });

        SingleOutputStreamOperator<TxEvent> txmap = TxStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");

                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });



        //对orde表的数据做一个过滤，只要支付的数据
        SingleOutputStreamOperator<OrderEvent> filterOrderEvent = ordermap.filter(new FilterFunction<OrderEvent>() {

            @Override
            public boolean filter(OrderEvent value) throws Exception {

                return "pay".equals(value.getEventType());

            }
        });

       /* //计数
        filterOrderEvent.map(new MapFunction<OrderEvent, Integer>() {
            int count=0;
            @Override
            public Integer map(OrderEvent value) throws Exception {

                count+=1;
                Integer countt = Integer.valueOf(count);
                System.out.println(count);
                return countt ;
            }
        }).setParallelism(1);*/



        //对两条流合流
        ConnectedStreams<OrderEvent, TxEvent> connect = filterOrderEvent.connect(txmap);

        //按交易码进行分组分区
        ConnectedStreams<OrderEvent, TxEvent> keyBy = connect.keyBy("txId", "txId");

        //实时对账
        //创建集合缓存数据
        HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
        HashMap<String, TxEvent> txEventHashMap = new HashMap<>();
        SingleOutputStreamOperator<String> process = keyBy.process(new CoProcessFunction<OrderEvent, TxEvent, String>() {
            @Override
            public void processElement1(OrderEvent value, CoProcessFunction<OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //去对方的缓存区查看是否有相同的交易码
                if (txEventHashMap.containsKey(value.getTxId())) {
                    out.collect("交易" + value.getTxId() + "对账成功");
                    txEventHashMap.remove(value.getTxId());
                } else {
                    //将交易码存放到缓冲区
                    orderEventHashMap.put(value.getTxId(), value);

                }
            }

            @Override
            public void processElement2(TxEvent value, CoProcessFunction<OrderEvent, TxEvent, String>.Context ctx, Collector<String> out) throws Exception {
                //去对方的缓存区中查找是否有相同的交易码
                if (orderEventHashMap.containsKey(value.getTxId())) {
                    //对账成功
                    out.collect("交易" + value.getTxId() + "对账成功");
                    orderEventHashMap.remove(value.getTxId());

                } else {
                    //没有查到，将自己缓存
                    txEventHashMap.put(value.getTxId(), value);
                }


            }
        });

        process.print();
        System.out.println(orderEventHashMap.size());
        System.out.println(txEventHashMap.size());


        //提交job
        env.execute();
    }

}
