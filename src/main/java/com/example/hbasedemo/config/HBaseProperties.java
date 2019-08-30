package com.example.hbasedemo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Map;

/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/21 15:25
 */
@ConfigurationProperties(prefix = "hbase")
public class HBaseProperties {
    private Map<String, String> config;

    public Map<String, String> getConfig() {
        return config;
    }

    public void setConfig(Map<String, String> config) {
        this.config = config;
    }
}
