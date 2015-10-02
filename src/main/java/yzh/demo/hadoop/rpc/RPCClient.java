package yzh.demo.hadoop.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

/**
 * 
 * @author sdwhyue
 *
 */
public class RPCClient{


	public static void main(String[] args) throws IOException {
		
		BizService bizService = RPC.getProxy(BizService.class, 10100, new InetSocketAddress("192.168.10.102",9898), new Configuration());
		String str = bizService.sayHello("zhangsan");
		System.out.println(str);
		
	}
	
}
