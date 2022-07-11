package com.atguigu.Bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @PackageName:com.atguigu.Bean
 * @ClassNmae:UserBehavior
 * @Dscription
 * @author:Esaisa
 * @date:2022/7/4 23:32
 */

@NoArgsConstructor
@Data
@AllArgsConstructor
public class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

}
