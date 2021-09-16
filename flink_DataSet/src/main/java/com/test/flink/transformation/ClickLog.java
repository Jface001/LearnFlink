package com.test.flink.transformation;

import lombok.Data;

/**
 * @Author: Jface
 * @Date: 2021/9/5 20:34
 * @Desc: 创建POJO类，封装数据
 */
@Data
public class ClickLog {
    //频道ID
    private long channelId;
    //产品的类别ID
    private long categoryId;
    //产品ID
    private long produceId;
    //用户的ID
    private long userId;
    //国家
    private String country;
    //省份
    private String province;
    //城市
    private String city;
    //网络方式
    private String network;
    //来源方式
    private String source;
    //浏览器类型
    private String browserType;
    //进入网站时间
    private Long entryTime;
    //离开网站时间
    private Long leaveTime;
}
