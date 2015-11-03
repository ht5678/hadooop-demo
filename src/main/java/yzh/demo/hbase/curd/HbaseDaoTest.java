package yzh.demo.hbase.curd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;


/**
 * hbase的增删改查
 * @author sdwhy
 *
 */
public class HbaseDaoTest {

	private static Configuration conf = null;
	
	
	/**
	 * 初始化hbase的连接
	 */
	@Before
	public void before(){
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.10.163:2181,192.168.10.164:2181,192.168.10.165:2181");
	}
	
	
	
	/**
	 * 测试添加
	 * @throws IOException 
	 */
	@Test
	public void put() throws IOException{
		//获取表格
		HTable table = new HTable(conf, "people");
		//创建一个put,主键为kr0001
		Put put = new Put(Bytes.toBytes("kr0001"));
		//添加数据
		put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsanfeng"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("30"));
		put.add(Bytes.toBytes("info"), Bytes.toBytes("money"), Bytes.toBytes(30000000));
		//将一列数据添加到table里边
		table.put(put);
		table.close();
	}
	
	
	

	/**
	 * 测试一百万条数据插入
	 */
	@Test
	public void testAll()throws Exception{
		long startTime = System.currentTimeMillis();
		//获取表格
		HTable table = new HTable(conf, "people");
		
		List<Put> puts = new ArrayList<>(10000);
		
		for(int i = 1;i<1000000 ; i++){
			//创建一个put,主键为kr0001
			Put put = new Put(Bytes.toBytes("kr"+i));
			//添加数据
			put.add(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("zhangsanfeng"+i));
			puts.add(put);
			
			//每一万条提交到hbase一次
			if(i % 10000 == 0){
				//将一列数据添加到table里边
				table.put(puts);
				puts = new ArrayList<>(10000);
			}
		}
		table.put(puts);
		table.close();
		System.out.println(System.currentTimeMillis()-startTime);
	}
	
	
	
	/**
	 * 测试查询
	 */
	@Test
	public void testGet()throws Exception{
		//获取表格
		HTable table = new HTable(conf, "people");
		Get get = new Get(Bytes.toBytes("kr999999"));
		Result result = table.get(get);
		byte[] bytes = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
		String str = Bytes.toString(bytes);
		System.out.println(str);
		table.close();
	}
	
	
	
	/**
	 * 测试scan查询
	 */
	@Test
	public void testScan()throws Exception{
		//获取表格
		HTable table = new HTable(conf, "people");
		Scan scan = new Scan(Bytes.toBytes("kr299990"),Bytes.toBytes("kr300000"));
		
		ResultScanner rs = table.getScanner(scan);
		for(Result rt : rs){
			String str = Bytes.toString(rt.getValue(Bytes.toBytes("info"), Bytes.toBytes("name")));
			System.out.println(str);
		}
		table.close();
	}
	
	
	
	
	/**
	 * 测试删除
	 */
	@Test
	public void testDelete()throws Exception{
		//获取表格
		HTable table = new HTable(conf, "people");
		
		Delete del = new Delete(Bytes.toBytes("kr999999"));
		table.delete(del);
		
		table.close();
		
	}
	
	
	
}
