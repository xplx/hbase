package com.example.hbasedemo.mode;

import lombok.Data;

import javax.validation.constraints.NotNull;

/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/22 16:25
 */
@Data
public class Order {
    private String id;

    private String orderNo;

    private Integer shopId;

    private Integer userId;
}
