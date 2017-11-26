package com.hbase.cn.hbase.api.htable;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
public class HbaseAdd {
	/**
	 * put a cell data into a row identified by rowKey,columnFamily,identifier 
	 * @param tableName
	 * @param rowKey
	 * @param columnFamily
	 * @param identifier
	 * @param data
	 * @throws Exception
	 */
	public void putCell(HTable htable, String rowKey, String columnFamily, String identifier, String data) throws Exception{
        Put p1 = new Put(Bytes.toBytes(rowKey));
        p1.add(Bytes.toBytes(columnFamily), Bytes.toBytes(identifier), Bytes.toBytes(data));
        htable.put(p1);
    }
}
