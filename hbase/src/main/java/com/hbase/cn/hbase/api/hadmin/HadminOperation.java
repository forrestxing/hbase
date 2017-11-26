package com.hbase.cn.hbase.api.hadmin;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import com.hbase.cn.hbase.api.comm.HbaseCommon;
public class HadminOperation {
	/**
	 * 创建hbase表
	 * @param tablename
	 * @param columnFamily
	 * @throws Exception
	 */
	public void createTable(String tablename, String columnFamily) throws Exception {
        HBaseAdmin admin =HbaseCommon.getHadmin();
        if(admin.tableExists(tablename)) {
            System.out.println("Table exists!");
            System.exit(0);
        }
        else {
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));
            tableDesc.addFamily(new HColumnDescriptor(columnFamily));
            admin.createTable(tableDesc);
            System.out.println("create table success!");
        }
        admin.close();

    }
	/**
	 * 删除hbase表
	 * @param tablename
	 * @return
	 * @throws Exception
	 */
	public boolean deleteTable(String tablename) throws Exception {
        HBaseAdmin admin =HbaseCommon.getHadmin();
        if(admin.tableExists(tablename)) {
            try {
                admin.disableTable(tablename);
                admin.deleteTable(tablename);
            } catch (Exception e) {
                // TODO: handle exception
                e.printStackTrace();
                admin.close();
                return false;
            }
        }
        admin.close();
        return true;
    }
	
	

}
