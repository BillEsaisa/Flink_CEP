package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:AppLog
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 22:46
 */
@Data
@AllArgsConstructor
@NoArgsConstructor

public class AppLog {
    private Long userId;
    private String behavior;
    private String channel;
    private Long timestamp;

}
