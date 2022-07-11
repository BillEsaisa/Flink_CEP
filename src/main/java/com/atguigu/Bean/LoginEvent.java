package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:LoginEvent
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/11 19:14
 */

@Data
@AllArgsConstructor
@NoArgsConstructor

public class LoginEvent {
    private Long userId;
    private String ip;
    private String eventType;
    private Long eventTime;

}
