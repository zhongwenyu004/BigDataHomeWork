package com.Hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.Arrays;


public class HomeWork {
    private static Logger logger =Logger.getLogger(HomeWork.class);

    public static void main(String[] args) throws Exception {
        System.out.print("项目开始启动");
        //建立HBASE 链接
        Configuration configuration = HBaseConfiguration.create();
        //configuration.set("hbase.zookeeper.quorum","emr-worker-2.cluster-285604,emr-worker-1.cluster-285604,emr-header-1.cluster-285604");
        configuration.set("hbase.zookeeper.quorum","emr-worker-2,emr-worker-1,emr-header-1");
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //configuration.set("hbase.master","emr-header-1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();//取到admin

        //判断命名空间是否存在
        if (!Arrays.stream(admin.listNamespaceDescriptors()).anyMatch((namespaceDescriptor) -> {
            return "zhongwenyu".equals(namespaceDescriptor.getName());
        })) {
            admin.createNamespace(NamespaceDescriptor.create("zhongwenyu").build());
            System.out.println("创建namspace: zhongwenyu, 成功！");
        }

        TableName tableName = TableName.valueOf("zhongwenyu:student");//表名 姓名：student
        String col1 = "name";
        String col2 = "info";
        String col3 = "score";

        if(!admin.tableExists(tableName)){//如果表格不存在，就建表
            HTableDescriptor hTable = new HTableDescriptor(tableName);
            HColumnDescriptor hColumn1 = new HColumnDescriptor(col1);//列族1
            HColumnDescriptor hColumn2 = new HColumnDescriptor(col2);//列族2
            HColumnDescriptor hColumn3 = new HColumnDescriptor(col3);//列族2
            hTable.addFamily(hColumn1);
            hTable.addFamily(hColumn2);
            hTable.addFamily(hColumn3);
            admin.createTable(hTable);
            System.out.print(tableName.getNameAsString()+"创建表成功");
            //插入数据
            Put put = new Put(Bytes.toBytes(1));//rowKey
            put.addColumn(col1.getBytes(),"name".getBytes(),"Tom".getBytes());
            put.addColumn(col2.getBytes(),"student_id".getBytes(),"20210000000001".getBytes());
            put.addColumn(col2.getBytes(),"class".getBytes(),"1".getBytes());
            put.addColumn(col3.getBytes(),"understanding".getBytes(),"75".getBytes());
            put.addColumn(col3.getBytes(),"programming".getBytes(),"82".getBytes());
            conn.getTable(tableName).put(put);
            Get get = new Get(Bytes.toBytes(1));//自己的信息
            Result result= conn.getTable(tableName).get(get);
            for (Cell cell:result.rawCells()){
                System.out.println(new String(CellUtil.getCellKeyAsString(cell)));
                System.out.println(new String(CellUtil.cloneFamily(cell)));
                System.out.println(new String(CellUtil.cloneQualifier(cell)));
                System.out.println(new String(CellUtil.cloneValue(cell)));
                System.out.println(cell.getTimestamp());
            }
        }
        System.out.print(tableName.getNameAsString()+"表已经存在");

        //插入数据
        Table table =conn.getTable(tableName);
        Put putZ = new Put(Bytes.toBytes(2));//rowKey
        putZ.addColumn(col1.getBytes(),"name".getBytes(),"传说中的胖子".getBytes());
        putZ.addColumn(col2.getBytes(),"student_id".getBytes(),"G20190343010030".getBytes());
        putZ.addColumn(col2.getBytes(),"class".getBytes(),"5".getBytes());
        putZ.addColumn(col3.getBytes(),"understanding".getBytes(),"0".getBytes());
        putZ.addColumn(col3.getBytes(),"programming".getBytes(),"0".getBytes());
        table.put(putZ);

        //查询数据
        Get get = new Get(Bytes.toBytes(2));//自己的信息
        Result result= table.get(get);
        for (Cell cell:result.rawCells()){
            System.out.println(new String(CellUtil.getCellKeyAsString(cell)));
            System.out.println(new String(CellUtil.cloneFamily(cell)));
            System.out.println(new String(CellUtil.cloneQualifier(cell)));
            System.out.println(new String(CellUtil.cloneValue(cell)));
            System.out.println(cell.getTimestamp());
        }
        //删除数据
        Delete delete = new Delete(Bytes.toBytes(1));
        table.delete(delete);
        Get getDel = new Get(Bytes.toBytes(1));//自己的信息
        Result resultDel= table.get(getDel);
        System.out.print("Tom的数据已经"+resultDel.toString());
        conn.close();
    }





}
