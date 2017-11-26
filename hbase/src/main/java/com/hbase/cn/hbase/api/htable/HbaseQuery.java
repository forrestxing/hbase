package com.hbase.cn.hbase.api.htable;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseQuery {
	/**
	 * get a row identified by rowkey
	 * @param tableName
	 * @param rowKey
	 * @return
	 * @throws Exception
	 */
	public Result getRow(HTable htable, String rowKey) throws Exception {
        Get get = new Get(Bytes.toBytes(rowKey));
        Result result = htable.get(get);
        return result;
    }
	/**
	 * return all row from a table
	 * @param table
	 * @return
	 * @throws Exception
	 */
	public ResultScanner scanAll(HTable table) throws Exception {
        Scan s =new Scan();
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
	/**
	 * 范围查询
	 * @param table
	 * @param startrow
	 * @param endrow
	 * @return
	 * @throws Exception
	 */
	public static ResultScanner scanRange(HTable table,String startrow,String endrow) throws Exception {
        Scan s =new Scan(Bytes.toBytes(startrow),Bytes.toBytes(endrow));
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
	/**
	 * 范围条件过滤查询
	 * @param table
	 * @param startrow
	 * @param filter
	 * @return
	 * @throws Exception
	 */
	public static ResultScanner scanFilter(HTable table,String startrow, Filter filter) throws Exception {
        Scan s =new Scan(Bytes.toBytes(startrow),filter);
        ResultScanner rs = table.getScanner(s);
        return rs;
    }
	
}
