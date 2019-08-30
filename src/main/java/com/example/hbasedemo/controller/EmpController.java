package com.example.hbasedemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.sound.sampled.Line;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/21 15:40
 */
@RestController
@RequestMapping("/emp")
@Slf4j
public class EmpController {
    @Autowired
    private HbaseTemplate hbaseTemplate;

    private static final String TABLE_NAME = "emp";

    /**
     * 通过表名和key获取一行数据
     *
     * @param tableName
     * @param rowName
     * @author : zhangai
     * @date : 14:21 2018/6/22
     * @description：
     */
    @RequestMapping("/getDateByRow")
    public Map<String, Object> getDateByRow(String tableName, String rowName) {
        return hbaseTemplate.get(tableName, rowName, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(Result result, int i) throws Exception {
                List<Cell> ceList = result.listCells();
                Map<String, Object> map = new HashMap<String, Object>(16);
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        map.put(Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()) +
                                        "_" + Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                }
                return map;
            }
        });
    }

    /**
     * 通过表名  key 和 列族 和列 获取一个数据
     *
     * @param tableName
     * @param rowName
     * @param familyName
     * @param qualifier
     * @return
     */
    @RequestMapping("/getEmp")
    public String get(String tableName, String rowName, String familyName, String qualifier) {
        return hbaseTemplate.get(tableName, rowName, familyName, qualifier, new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int i) throws Exception {
                List<Cell> ceList = result.listCells();
                String res = "";
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        res = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    }
                }
                return res;
            }
        });
    }

    public List<Map<String, Map<String, String>>> query(String id) {
        List<Map<String, Map<String, String>>> md = hbaseTemplate.find(TABLE_NAME, id, (result, rowNum) -> {
            Cell[] cells = result.rawCells();
            Map<String, Map<String, String>> data = new HashMap<>(16);
            for (Cell c : cells) {
                String columnFamily = new String(CellUtil.cloneFamily(c));
                String rowName = new String(CellUtil.cloneQualifier(c));
                String value = new String(CellUtil.cloneValue(c));

                Map<String, String> obj = data.get(columnFamily);
                if (null == obj) {
                    obj = new HashMap<>(16);
                }
                obj.put(rowName, value);
            }
            return data;
        });
        return md;
    }

    /**
     * 数据插入
     *
     * @param tableName 表名
     * @author : zhangai
     * @date : 11:42 2018/6/22
     * @description：
     */
    @RequestMapping("/save")
    public String execute(String tableName, String row, String value) {
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(tableName), Bytes.toBytes(value));
        hbaseTemplate.put(tableName, row, "cf1", tableName, Bytes.toBytes(value));
        return "success";
    }
}
