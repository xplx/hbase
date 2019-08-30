package com.example.hbasedemo.config;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.hbase.HbaseTemplate;

import java.net.URISyntaxException;
import java.util.Map;
import java.util.Set;

/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/21 15:27
 */
@Configuration
@EnableConfigurationProperties(HBaseProperties.class)
public class HBaseConfig {
    private final HBaseProperties properties;

    public HBaseConfig(HBaseProperties properties) {
        this.properties = properties;
    }

    @Bean
    public HbaseTemplate hbaseTemplate() throws URISyntaxException {
        // 加载HBase的配置
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

        // 读取配置文件
        configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
        configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
        HbaseTemplate hbaseTemplate = new HbaseTemplate();
        hbaseTemplate.setConfiguration(configuration);
        hbaseTemplate.setAutoFlush(true);
        return hbaseTemplate;
    }

    public org.apache.hadoop.conf.Configuration configuration() {

        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();

        Map<String, String> config = properties.getConfig();
        Set<String> keySet = config.keySet();
        for (String key : keySet) {
            configuration.set(key, config.get(key));
        }

        return configuration;
    }
}
