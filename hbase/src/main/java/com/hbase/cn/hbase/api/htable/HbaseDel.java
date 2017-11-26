package com.hbase.cn.hbase.api.htable;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseDel {
	/**
	 * delete a row identified by rowkey
	 * @param table
	 * @param rowKey
	 * @throws Exception
	 */
	public void deleteRow(HTable table, String rowKey) throws Exception {
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }
}
