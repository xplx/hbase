package com.example.hbasedemo.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.hbasedemo.mode.Order;
import com.example.hbasedemo.mode.TargetParamsVo;
import com.example.hbasedemo.service.HbaseTemplateService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * @author wuxiaopeng
 * @description:
 * @date 2019/8/22 16:08
 */
@Slf4j
@RestController
@RequestMapping("/hbaseTemplate")
public class HbaseTemplateController {
    @Autowired
    private HbaseTemplateService hbaseTemplateService;

    /**
     * 创建表
     *
     * @param tableName
     * @return
     */
    @RequestMapping("/createTable")
    public String createTable(String tableName) {
        String[] familyA = {"cf1", "cf2"};
        Boolean isSuccess = hbaseTemplateService.createTable(tableName, familyA);
        if (isSuccess) {
            return "create table success !";
        }
        return "create table fail !";
    }

    /**
     * 保存数据
     *
     * @param tableName
     * @param family
     * @return
     */
    @RequestMapping("/save")
    public String save(String tableName, String family, String rowKey) {
        Order order = new Order();
        order.setId(rowKey);
        order.setOrderNo("orderNo" + 1);
        order.setShopId(1);
        order.setUserId(123);
        Object isSuccess = hbaseTemplateService.createPro(order, tableName, family, rowKey);
        return "save data success !";
    }

    /**
     * 删除数据
     * @param tableName
     * @param rowName
     * @param familyName
     * @param qualifier
     * @return
     */
    @RequestMapping("/delete")
    public String delete(String tableName, String rowName, String familyName, String qualifier) {
        Object isSuccess = hbaseTemplateService.deleteData(tableName, rowName, familyName, qualifier);
        return "delete data success !";
    }

    /**
     * 查询表所有数据
     *
     * @param tableName
     * @return
     */
    @RequestMapping("/orderList")
    public List<Order> orderList(String tableName) {
        List<Order> orderList = hbaseTemplateService.searchAll(tableName, Order.class);
        return orderList;
    }

    /**
     * 限制行数查询(范围查询)
     * @param tableName
     * @param begin
     * @param end
     * @return
     */
    @RequestMapping("/orderLimitList")
    public List<Order> orderLimitList(String tableName, String begin,String end) {
        List<Order> orderList = hbaseTemplateService.findByRowRange(Order.class, tableName,begin, end);
        return orderList;
    }

    /**
     * 限制行数查询(范围查询)
     * @param tableName
     * @return
     */
    @RequestMapping("/orderFilterList")
    public List<Order> orderFilterList(String tableName) throws IOException {
        Order order = new Order();
        order.setId("row1");
        order.setOrderNo("orderNo" + 1);
        order.setShopId(1);
        order.setUserId(1);
        List<Filter> list = new ArrayList<>();
        String targetSet = JSON.toJSONString(order);
        if(StringUtils.isNotBlank(targetSet)){
            list.add(new SingleColumnValueFilter(Bytes.toBytes("cf1"),null,
                    CompareFilter.CompareOp.GREATER, Bytes.toBytes(targetSet)));
        }
        FilterList filterList = new FilterList(list);
        List<Order> orderList = hbaseTemplateService.getListByCondition(Order.class, tableName, filterList);
        return orderList;
    }

    public Serializable getTargetInfos(String jsonString) throws Exception {
        JSONObject jsonObject= JSONObject.parseObject(jsonString);
        List<TargetParamsVo> paramsVoList=jsonObject.getJSONArray("targetParams").toJavaList(TargetParamsVo.class);


//        List<Filter> list=new ArrayList<>();
//        List<Filter> cellIdList=new ArrayList<>();
//        List<Filter> targetSetList=new ArrayList<>();
//        List<Filter> targetSonSetList=new ArrayList<>();
//        List<Filter> targetList=new ArrayList<>();

        String targetSet=null;
        String targetSonSet=null;
        String target=null;
        String cellId=null;
        FilterList filterList=new FilterList(FilterList.Operator.MUST_PASS_ONE);
        FilterList queryFilters=null;
        for(TargetParamsVo tpv:paramsVoList){
            targetSet = tpv.getTargetSet();
            targetSonSet = tpv.getTargetSonSet();
            target = tpv.getTarget();
            cellId = tpv.getCellId();

            queryFilters=new FilterList(FilterList.Operator.MUST_PASS_ALL);
            if(StringUtils.isNotBlank(cellId)){
                queryFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("cellId"),
                        CompareFilter.CompareOp.EQUAL,new SubstringComparator(cellId)));

            }
            if(StringUtils.isNotBlank(targetSet)){
                queryFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("targetSet"),
                        CompareFilter.CompareOp.EQUAL,new SubstringComparator(targetSet)));
            }
            if(StringUtils.isNotBlank(targetSonSet)){
                queryFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("targetSonSet"),
                        CompareFilter.CompareOp.EQUAL,new SubstringComparator(targetSonSet)));

            }
            if(StringUtils.isNotBlank(target)){
                queryFilters.addFilter(new SingleColumnValueFilter(Bytes.toBytes("info"),Bytes.toBytes("target"),
                        CompareFilter.CompareOp.EQUAL,new SubstringComparator(target)));

            }
//        list.add( new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("cell")));

//        https://blog.csdn.net/lr131425/article/details/72676254
            //     new BinaryPrefixComparator(value) //匹配字节数组前缀
////       new RegexStringComparator(expr) // 正则表达式匹配
//     new SubstringComparator(substr)// 子字符串匹配
//            new FamilyFilter(CompareFilter.CompareOp.EQUAL,new BinaryComparator(Bytes.toBytes("info")));

            filterList.addFilter(queryFilters);

        }

        //配置查询过滤器
//        FilterList cellIdFilters=new FilterList(FilterList.Operator.MUST_PASS_ONE,cellIdList);
//        FilterList targetSetFilters=new FilterList(FilterList.Operator.MUST_PASS_ONE,targetSetList);
//        FilterList targetSonSetFilters=new FilterList(FilterList.Operator.MUST_PASS_ONE,targetSonSetList);
//        FilterList targetFilters=new FilterList(FilterList.Operator.MUST_PASS_ONE,targetList);

        List<Order> rsList = hbaseTemplateService.getListByCondition(Order.class,"target",filterList);

        return (Serializable)rsList;
    }
}
