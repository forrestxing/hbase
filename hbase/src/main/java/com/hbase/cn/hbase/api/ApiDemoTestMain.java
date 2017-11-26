package com.hbase.cn.hbase.api;

import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import com.hbase.cn.hbase.api.comm.HbaseCommon;
import com.hbase.cn.hbase.api.htable.HbaseQuery;

public class ApiDemoTestMain {

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		queryTable();
	}

	public static void queryTable() throws Exception {
		HTable table = HbaseCommon.getHtable("test");
		HbaseQuery hbaseQuery = new HbaseQuery();
		ResultScanner scanner = hbaseQuery.scanAll(table);

		// 循环输出表中的数据
		for (Result result : scanner) {

			byte[] row = result.getRow();
			System.out.println("row key is:" + new String(row));

			List<Cell> listCells = result.listCells();
			for (Cell cell : listCells) {

				byte[] familyArray = cell.getFamilyArray();
				byte[] qualifierArray = cell.getQualifierArray();
				byte[] valueArray = cell.getValueArray();

				System.out.println("row value is:" + new String(familyArray)
						+ new String(qualifierArray) + new String(valueArray));
			}
		}
		System.out.println("---------------查询整表数据 END-----------------");

	}

}
