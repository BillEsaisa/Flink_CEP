package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:TxEvent
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 19:21
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TxEvent {
    private String txId;
    private String payChannel;
    private Long eventTime;

}
