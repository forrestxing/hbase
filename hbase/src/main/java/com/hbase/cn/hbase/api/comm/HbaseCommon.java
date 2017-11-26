package com.hbase.cn.hbase.api.comm;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

public class HbaseCommon {
	static Configuration conf = HBaseConfiguration.create();
    Connection connection;
	static{
		// 设置连接参数：HBase数据库所在的主机IP
        conf.set("hbase.zookeeper.quorum", "192.168.137.13");
        // 设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
	
	public static HTable getHtable(String tableName) throws IOException{
		HTable hTable = new HTable(conf,tableName);
//		return connection.getTable(TableName.valueOf(tableName));
		return hTable;
	}
	public static HBaseAdmin getHadmin() throws IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		return admin;
	}
	
	public Table getTableNew(String tableName) throws IOException{
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {

			e.printStackTrace();
		}
		return connection.getTable(TableName.valueOf(tableName));
	}
	public Admin getAdminNew() throws IOException{
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {

			e.printStackTrace();
		}
		return connection.getAdmin();
	}
	
}
