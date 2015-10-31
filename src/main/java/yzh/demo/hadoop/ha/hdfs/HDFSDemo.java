package yzh.demo.hadoop.ha.hdfs;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;


/**
 * ha环境下的hdfs 下载demo
 * @author sdwhy
 *
 */
public class HDFSDemo {

	
	public static void main(String[] args) throws IOException, URISyntaxException {
		
		Configuration conf = new Configuration();
		//ha环境下在hdfs-site.xml中配置的nameservie名称
		conf.set("dfs.nameservices", "ns1");
		//ha环境下载hdfs-site.xml中配置的namenode
		conf.set("dfs.ha.namenodes.ns1", "nn1,nn2");
		//namenode1的rpc地址和端口
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "192.168.10.160:9000");
		//namenode2的rpc地址和端口
		conf.set("dfs.namenode.rpc-address.ns1.nn1", "192.168.10.161:9000");
		
		conf.set("dfs.client.failover.proxy.provider.ns1", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
		
		FileSystem fs = FileSystem.get(new URI("hdfs://ns1"),conf);
		
		InputStream in = fs.open(new Path("/soft/apache-maven-3.3.3-bin.tar.gz"));
		
		OutputStream out = new FileOutputStream("e://test/apache-maven-3.3.3-bin.tar.gz");
		
		IOUtils.copyBytes(in, out, 4096,true);
		
		
	}
	
	
}
