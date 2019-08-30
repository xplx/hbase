package com.example.hbasedemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.mortbay.util.ajax.JSON;
import org.omg.CORBA.portable.ValueOutputStream;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @author DELL
 * @Date 2019-08-22
 */
@Slf4j
public class HelloHBase {
    private static Connection connection;

    public static void main(String[] args) throws URISyntaxException {
        try {
           // deleteTable("student");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void deleteTableData(String name) throws IOException {
        //获取表
        TableName tableName = TableName.valueOf(name);
        //连接数据库
        Connection connection = ConnectionDb();
        // 获取表对象
        Table table = connection.getTable(tableName);
        // 创建一个删除请求
        Delete delete = new Delete(Bytes.toBytes("row2"));
        // 可以自定义一些筛选条件
        delete.addFamily(Bytes.toBytes("mycf"));
        table.delete(delete);
    }

    public static String queryTableOneData(String name, String row) throws IOException {
        //获取表
        TableName tableName = TableName.valueOf(name);
        //连接数据库
        Connection connection = ConnectionDb();
        // 获取表对象
        Table table = connection.getTable(tableName);

        // 创建一个查询请求，查询一行数据
        Get get = new Get(Bytes.toBytes("row1"));
        // 由于HBase的一行可能非常大，所以限定要取出的列族
        get.addFamily(Bytes.toBytes("mycf"));
        // 创建一个结果请求
        Result result = table.get(get);
        // 从查询结果中取出name列，然后打印（这里默认取最新版本的值，如果要取其他版本要使用Cell对象）
        byte[] tableInfo = result.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name"));
        log.info("表数据：{}", Bytes.toString(tableInfo));
        return Bytes.toString(tableInfo);
    }

    public static void queryTableListData(String name, String row) throws IOException {
        //获取表
        TableName tableName = TableName.valueOf(name);
        //连接数据库
        Connection connection = ConnectionDb();
        // 获取表对象
        Table table = connection.getTable(tableName);

        // 创建一个扫描请求，查询多行数据
        Scan scan = new Scan(Bytes.toBytes("row1"));
        // 设置扫描器的缓存数量，遍历数据时不用发多次请求，默认100，适当的缓存可以提高性能
        scan.setCaching(150);
        // 创建扫描结果，这个时候不会真正从HBase查询数据，下面的遍历才是去查询
        ResultScanner resultScanner = table.getScanner(scan);
        for (Result r : resultScanner) {
            String data = Bytes.toString(r.getValue(Bytes.toBytes("mycf"), Bytes.toBytes("name")));
            System.out.println(data);
        }
        // 使用完毕关闭
        resultScanner.close();
    }

    /**
     *
     * @param name 表明
     * @throws IOException
     */
    public static void deleteTable(String name) throws IOException {
        //获取表
        TableName tableName = TableName.valueOf(name);
        //连接数据库
        Connection connection = ConnectionDb();
        // 获得执行操作的管理接口
        Admin admin = connection.getAdmin();
        //停用表
        admin.disableTable(tableName);
        // 删除列族
        //admin.deleteColumnFamily(tableName, "mycf".getBytes());
        // 删除表
        admin.deleteTable(tableName);
    }

    public static void insertData(String name, String row, String familyName, String value) throws IOException {
        //获取表
        TableName tableName = TableName.valueOf(name);
        //连接数据库
        Connection connection = ConnectionDb();
        // 获取表对象
        Table table = connection.getTable(tableName);

        // 创建一个put请求，用于添加数据或者更新数据
        Put put = new Put(Bytes.toBytes(row));
        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(name), Bytes.toBytes(value));
        table.put(put);
    }

//    /**
//     * 通用方法-> 创建表
//     *
//     * @param tableName    表名
//     * @param columnFamily 列族集合
//     * @return
//     * @throws Exception
//     */
//    private static int createTable(String tableName, List<String> columnFamily) throws Exception {
//        Connection connection = ConnectionDb();
//        // 获得执行操作的管理接口
//        Admin admin = connection.getAdmin();
//        if (admin.tableExists(TableName.valueOf(tableName))) {
//            log.debug(MessageFormat.format("创建HBase表名：{0} 在HBase数据库中已经存在", tableName));
//            return 2;
//        } else {
//            //创建列族
//            List<ColumnFamilyDescriptor> familyDescriptors = new ArrayList<>(columnFamily.size());
//            for (String column : columnFamily) {
//                familyDescriptors.add(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(column)).build());
//            }
//
//            //创建表明
//            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableName))
//                    .setColumnFamilies(familyDescriptors).build();
//            admin.createTable(tableDescriptor);
//            log.info(MessageFormat.format("创建表成功：表名：{0}，列簇：{1}", tableName, columnFamily.toString()));
//            return 1;
//        }
//    }

    public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        // 获取table名
        TableName tableName = table.getTableName();
        // 判断table是否存在，如果存在则先停用并删除
        if (admin.tableExists(tableName)) {
            // 停用表
            admin.disableTable(tableName);
            // 删除表
            admin.deleteTable(tableName);
        }
        // 创建表
        admin.createTable(table);
    }

    private static void createTable(String name, String familyName, String addFamilyName) throws IOException {
        Connection connection = ConnectionDb();
        // 获得执行操作的管理接口
        Admin admin = connection.getAdmin();
        // 新建一个表名为mytable的表
        TableName tableName = TableName.valueOf(name);

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

        // 新建一个列族名为mycf的列族
        HColumnDescriptor mycf = new HColumnDescriptor(familyName);

        // 将列族添加到表中
        tableDescriptor.addFamily(mycf);
        // 执行建表操作
        createOrOverwrite(admin, tableDescriptor);

        // 设置列族的压缩方式为GZ
        mycf.setCompactionCompressionType(Compression.Algorithm.GZ);
        // 设置最大版本数量（ALL_VERSIONS实际上就是Integer.MAX_VALUE）
        mycf.setMaxVersions(HConstants.ALL_VERSIONS);
        // 列族更新到表中
        tableDescriptor.modifyFamily(mycf);
        // 执行更新操作
        admin.modifyTable(tableName, tableDescriptor);

        // 新增一个列族
        HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("newcf");
        hColumnDescriptor.setCompactionCompressionType(Compression.Algorithm.GZ);
        hColumnDescriptor.setMaxVersions(HConstants.ALL_VERSIONS);
        // 执行新增操作
        //admin.addColumnFamily(tableName, hColumnDescriptor);
        admin.addColumn(tableName, hColumnDescriptor);
    }

    /**
     * 连接数据库
     *
     * @return
     */
    private static Connection ConnectionDb() {
        if (connection == null) {
            // 加载HBase的配置
            Configuration configuration = HBaseConfiguration.create();
            try {
                // 读取配置文件
                configuration.addResource(new Path(ClassLoader.getSystemResource("hbase-site.xml").toURI()));
                configuration.addResource(new Path(ClassLoader.getSystemResource("core-site.xml").toURI()));
                connection = ConnectionFactory.createConnection(configuration);
            } catch (URISyntaxException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
}

