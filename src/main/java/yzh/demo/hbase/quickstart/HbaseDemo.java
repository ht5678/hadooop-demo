package yzh.demo.hbase.quickstart;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;



/**
 * 
 * 连接hbase集群，并且新增表映射
 * @author sdwhy
 *
 */
public class HbaseDemo {
	
	
	public static void main(String[] args) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.10.163:2181,192.168.10.164:2181,192.168.10.165:2181");
		
		
		//admin用户
		HBaseAdmin admin = new HBaseAdmin(conf);
		//table
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people"));
		//info列簇
		HColumnDescriptor hcd_info = new HColumnDescriptor("info");
		hcd_info.setMaxVersions(3);
		//data列簇
		HColumnDescriptor hcd_data = new HColumnDescriptor("data");
		
		//将列簇添加到htable中
		htd.addFamily(hcd_info);
		htd.addFamily(hcd_data);
		
		//创建表
		admin.createTable(htd);
		
		//关闭连接
		admin.close();
		
		
	}

}
