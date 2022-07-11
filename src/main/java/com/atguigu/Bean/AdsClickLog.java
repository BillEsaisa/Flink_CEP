package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:AdsClickLog
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 22:21
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AdsClickLog {
    private Long userId;
    private Long adId;
    private String province;
    private String city;
    private Long timestamp;

}
