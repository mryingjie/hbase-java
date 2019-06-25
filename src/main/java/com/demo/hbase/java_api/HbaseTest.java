package com.demo.hbase.java_api;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @Author ZhengYingjie
 * @Date 2019/1/19
 * @Description
 */
public class HbaseTest {

    private static final Logger logger = Logger.getLogger(HbaseTest.class);
    private static Configuration conf = null;
    private Connection connection = null;
    private Table table = null;

    /**
     * 初始化连接
     * @throws IOException
     */
    @Before
    public void init() throws IOException {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","hadoop1001,hadoop1002,hadoop1003");
        conf.set("hbase.zookeeper.property", "2181");
        connection = ConnectionFactory.createConnection(conf);
        table = connection.getTable(TableName.valueOf("user"));
        logger.info("初始化表连接完成！");
        System.out.println("初始化表连接完成！");
    }

    /**
     * 创建表
     * @throws IOException
     */
    @Test
    public void createTable() throws IOException {
        //创建表管理类
//        HBaseAdmin admin = new HBaseAdmin(conf);
        Admin admin = connection.getAdmin();

        //创建表描述类
        TableName tableName = TableName.valueOf("test3");
        if(admin.tableExists(tableName)){
            logger.error("表已经存在!");
        }
        HTableDescriptor desc = new HTableDescriptor(tableName);

        //创建列祖描述类
        HColumnDescriptor famliy1= new HColumnDescriptor("info1");
        HColumnDescriptor famliy2= new HColumnDescriptor("info2");

        //将列族添加到表
        desc.addFamily(famliy1);
        desc.addFamily(famliy2);

        //创建表
        admin.createTable(desc);
        logger.info("创建成功");
    }

    /**
     * 插入数据
     * @throws IOException
     */
    @Test
    public void insert() throws IOException {
        //创建数据封装类
        Put zhangsan= new Put(Bytes.toBytes("zhangsan"));//rowkey
        zhangsan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("zhangsan"));
        zhangsan.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("18"));
        zhangsan.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
        zhangsan.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("address"), Bytes.toBytes("北京"));

        Put lisi= new Put(Bytes.toBytes("lisi"));
        lisi.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
        lisi.addColumn(Bytes.toBytes("info1"), Bytes.toBytes("age"), Bytes.toBytes("23"));
        lisi.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("sex"), Bytes.toBytes("女"));
        lisi.addColumn(Bytes.toBytes("info2"), Bytes.toBytes("address"), Bytes.toBytes("上海"));

        List<Put> puts = new ArrayList<>();
        puts.add(zhangsan);
        puts.add(lisi);
        table.put(puts);
    }

    @Test
    public void update(){
        //其实是覆盖 增加新值即可
    }

    /**
     * 删除
     * @throws IOException
     */
    @Test
    public void deleteDate() throws IOException {
        Delete zhangsan = new Delete(Bytes.toBytes("zhangsan"));
        zhangsan.addColumn(Bytes.toBytes("info1"),Bytes.toBytes( "name"));
        zhangsan.addFamily("info2".getBytes());
        ArrayList<Delete> deletes = new ArrayList<>();
        deletes.add(zhangsan);
        table.delete(deletes);
    }

    /**
     * 查询单条数据
     * @throws IOException
     */
    @Test
    public void queryOneData() throws IOException {
        //封装查询条件的类
        Get get = new Get(Bytes.toBytes("zhangsan"));
//        get.addColumn(, )
//        get.addFamily()
        Result result = table.get(get);

        byte[] value = result.getValue(Bytes.toBytes("info1"), Bytes.toBytes("name"));


        System.out.println("查询数据：");
        System.out.println(Bytes.toString(value));
//        System.out.println(new String(value));
    }

    /**
     * 扫描
     * @throws IOException
     */
    @Test
    public void scanData() throws IOException {
        Scan scan = new Scan();
//        scan.setStartRow() //起始行 rowkey字典排序
//        scan.setStopRow()
//        scan.addFamily("info2".getBytes());   //只查某列族
        scan.addColumn("info2".getBytes(),"address".getBytes()); //只查某列
        ResultScanner scanner = table.getScanner(scan);
        listValue(scanner);
    }

    private void listValue(ResultScanner scanner) {
        for (Result next : scanner) {
            byte[] value = next.getValue("info2".getBytes(), "address".getBytes());
            byte[] name = next.getValue("info1".getBytes(), "name".getBytes());
            System.out.println(new String(value));
            System.out.println(new String(name));
            System.out.println(new String(next.getRow()));
            System.out.println("***************");

        }
    }

    /**
     * 过滤查询 列值过滤器 SingleColumnValueFilter
     */
    @Test
    public void queryByFilter1() throws IOException {
        Scan scan = new Scan();

        //列值小于19
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter("info1".getBytes(),"age".getBytes(),CompareFilter.CompareOp.LESS,"19".getBytes());
//        FilterList filterList = new FilterList();
//        filterList.addFilter();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner scanner = table.getScanner(scan);
        listValue(scanner);
    }



    /**
     * 过滤查询 列名前缀过滤器 ColumnPrefixFilter
     */
    @Test
    public void queryByFilter2() throws IOException {
        Scan scan = new Scan();

        // 以“n”开头的列
        ColumnPrefixFilter filter =
                new ColumnPrefixFilter("a".getBytes());
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        listValue(scanner);
    }

    /**
     * 过滤查询 多个列名前缀过滤器 MultipleColumnPrefixFilter
     */
    @Test
    public void queryByFilter3() throws IOException {
        Scan scan = new Scan();

        // 以“n”开头的列 和以"a"开头的列
        byte[][] prefix = {("n".getBytes()),("a".getBytes())};
        MultipleColumnPrefixFilter filter =
                new MultipleColumnPrefixFilter(prefix);
        FilterList filterList = new FilterList();
        filterList.addFilter(filter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        listValue(scanner);
    }

    /**
     * 过滤查询 rowKey过滤器 RowFilter
     */
    @Test
    public void queryByFilter4() throws IOException {
        Scan scan = new Scan();

        //以l开头的rowkey
        RowFilter filter =
                new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("^l"));
        //与
//        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        // 或
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        filterList.addFilter(filter);
        scan.setFilter(filterList);
        ResultScanner scanner = table.getScanner(scan);
        listValue(scanner);
    }

    @After
    public void closeCOnnection(){
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.info("关闭连接！！");
        System.out.println("关闭连接！！");
    }




}
