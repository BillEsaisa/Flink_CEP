package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:Sensor
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/3 6:01
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Sensor {
    private String id;
    private Long ts;
    private Double value;
}
