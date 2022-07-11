package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:OrderEvent
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 19:20
 */
@Data
@NoArgsConstructor
@AllArgsConstructor

public class OrderEvent {
    private Long orderId;
    private String eventType;
    //交易码
    private String txId;
    private Long eventTime;

}
