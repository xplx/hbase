package com.example.hbasedemo.service.impl;


import java.beans.PropertyDescriptor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.example.hbasedemo.service.HbaseTemplateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.PropertyAccessorFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.springframework.data.hadoop.hbase.TableCallback;
import org.springframework.stereotype.Service;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/22 15:59
 */

@Service
@Slf4j
public class HbaseTemplateServiceImpl implements HbaseTemplateService {
    @Autowired
    private HbaseTemplate hbaseTemplate;

    @SuppressWarnings({"resource", "deprecation"})
    @Override
    public boolean createTable(String tableName, String... column) {
        HBaseAdmin admin;
        try {
            // 从hbaseTemplate 获取configuration对象,用来初始化admin
            admin = new HBaseAdmin(hbaseTemplate.getConfiguration());
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (int i = 0; i < column.length; i++) {
                tableDescriptor.addFamily(new HColumnDescriptor(column[i]));
            }
            admin.createTable(tableDescriptor);
            return admin.tableExists(tableName);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public <T> List<T> searchAll(String tableName, Class<T> c) {
        return hbaseTemplate.find(tableName, new Scan(), new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                T pojo = c.newInstance();
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                List<Cell> ceList = result.listCells();
                for (Cell cellItem : ceList) {
                    String cellName = new String(CellUtil.cloneQualifier(cellItem));
                    if (!"class".equals(cellName)) {
                        beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
                    }
                }
                return pojo;
            }
        });
    }

    @Override
    public Object createPro(Object pojo, String tableName, String column, String rowKey) {
        if (pojo == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(column)) {
            return null;
        }
        return hbaseTemplate.execute(tableName, new TableCallback<Object>() {
            @Override
            public Object doInTable(HTableInterface table) throws Throwable {
                PropertyDescriptor[] pds = BeanUtils.getPropertyDescriptors(pojo.getClass());
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                Put put = new Put(Bytes.toBytes(rowKey));
                for (PropertyDescriptor propertyDescriptor : pds) {
                    String properName = propertyDescriptor.getName();
                    String value = String.valueOf(beanWrapper.getPropertyValue(properName));
                    if (!StringUtils.isBlank(value)) {
                        put.addColumn(Bytes.toBytes(column), Bytes.toBytes(properName), Bytes.toBytes(value));
                    }
                }
                table.put(put);
                return null;
            }
        });
    }

    @Override
    public Object deleteData(String tableName, String rowName, String familyName, String qualifier){
        hbaseTemplate.delete(tableName, rowName, familyName, qualifier);
        return "delete success !";
    }

    @Override
    public <T> T getOneToClass(Class<T> c, String tableName, String rowkey) {
        if (c == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(rowkey)) {
            return null;
        }
        return hbaseTemplate.get(tableName, rowkey, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList = result.listCells();
                JSONObject obj = new JSONObject();
                T item = c.newInstance();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
//                        cell.getTimestamp();
//                        //rowKey
//                        CellUtil.cloneRow(cell);
                        obj.put(
                                Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                        cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                } else {
                    return null;
                }
                item = JSON.parseObject(obj.toJSONString(), c);
                return item;
            }
        });
    }

    @Override
    public <T> List<T> getListByCondition(Class<T> c, String tableName, FilterList filterList) {
        if (c == null || StringUtils.isBlank(tableName)) {
            return null;
        }
//        List<Filter> list = new ArrayList<>();
//        String targetSet = jsonObject.getString("targetSet");
//        String targetSonSet = jsonObject.getString("targetSonSet");
//        String target= jsonObject.getString("target");
//        if(StringUtils.isNotBlank(targetSet)){
//            list.add(new SingleColumnValueFilter(Bytes.toBytes("targetSet"),null,
//                    CompareFilter.CompareOp.EQUAL,Bytes.toBytes(targetSet)));
//        }
//        if(StringUtils.isNotBlank(targetSonSet)){
//            list.add(new SingleColumnValueFilter(Bytes.toBytes("targetSonSet"),null,
//                    CompareFilter.CompareOp.EQUAL,Bytes.toBytes(targetSonSet)));
//        }
//        if(StringUtils.isNotBlank(target)){
//            list.add(new SingleColumnValueFilter(Bytes.toBytes("target"),null,
//                    CompareFilter.CompareOp.EQUAL,Bytes.toBytes(target)));
//        }
//        FilterList filterList = new FilterList(list);

        Scan scan = new Scan();
        scan.setFilter(filterList);
        return hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                T pojo = c.newInstance();
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                List<Cell> ceList = result.listCells();
                for (Cell cellItem : ceList) {
                    String cellName = new String(CellUtil.cloneQualifier(cellItem));
                    if (!"class".equals(cellName)) {
                        beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
                    }
                }
                return pojo;
            }
        });
    }


    @Override
    public Map<String, Object> getOneToMap(String tableName, String rowName) {
        return hbaseTemplate.get(tableName, rowName, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(Result result, int i) throws Exception {
                List<Cell> ceList = result.listCells();
                Map<String, Object> map = new HashMap<String, Object>();
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

    @Override
    public String getColumn(String tableName, String rowkey, String family, String column) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(family)
                || StringUtils.isBlank(rowkey) || StringUtils.isBlank(column)) {
            return null;
        }
        return hbaseTemplate.get(tableName, rowkey, family, column, new RowMapper<String>() {
            @Override
            public String mapRow(Result result, int rowNum) throws Exception {
                List<Cell> ceList = result.listCells();
                String res = "";
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        res =
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    }
                }
                return res;
            }
        });
    }

    @Override
    public <T> List<T> findByRowRange(Class<T> c, String tableName, String startRow, String endRow) {
        if (c == null || StringUtils.isBlank(tableName) || StringUtils.isBlank(startRow)
                || StringUtils.isBlank(endRow)) {
            return null;
        }
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        scan.setCacheBlocks(false);
        scan.setCaching(2000);
        return hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
                T pojo = c.newInstance();
                BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
                List<Cell> ceList = result.listCells();
                for (Cell cellItem : ceList) {
                    String cellName = new String(CellUtil.cloneQualifier(cellItem));
                    if (!"class".equals(cellName)) {
                        beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
                    }
                }
                return pojo;
            }
        });
    }

    @Override
    public <T> List<T> searchAllByFilter(Class<T> clazz, String tableName, SingleColumnValueFilter scvf) {
        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes(family));
        scan.setFilter(scvf);
        return hbaseTemplate.find(tableName, scan, new RowMapper<T>() {
            @Override
            public T mapRow(Result result, int rowNum) throws Exception {
//        T pojo = c.newInstance();
//        BeanWrapper beanWrapper = PropertyAccessorFactory.forBeanPropertyAccess(pojo);
//        List<Cell> ceList = result.listCells();
//        for (Cell cellItem : ceList) {
//          String cellName = new String(CellUtil.cloneQualifier(cellItem));
//          if (!"class".equals(cellName)) {
//            beanWrapper.setPropertyValue(cellName, new String(CellUtil.cloneValue(cellItem)));
//          }
//        }
//        return pojo;
                List<Cell> ceList = result.listCells();
                JSONObject obj = new JSONObject();
                T item = clazz.newInstance();
                if (ceList != null && ceList.size() > 0) {
                    for (Cell cell : ceList) {
                        obj.put(
                                Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                        cell.getQualifierLength()),
                                Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength()));
                    }
                } else {
                    return null;
                }
                item = JSON.parseObject(obj.toJSONString(), clazz);
                return item;
            }
        });
    }
}
