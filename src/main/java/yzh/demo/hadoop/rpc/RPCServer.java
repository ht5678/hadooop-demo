package yzh.demo.hadoop.rpc;

import java.io.IOException;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;


/**
 * rpc
 * @author sdwhyue
 *
 */
public class RPCServer implements BizService{

	
	public String sayHello(String name){
		return "hello , "+name;
	}
	
	
	public static void main(String[] args) throws HadoopIllegalArgumentException, IOException {
		Configuration conf = new Configuration();
		Server server = new RPC.Builder(conf).setProtocol(BizService.class)
				.setInstance(new RPCServer())
				.setBindAddress("192.168.10.102")
				.setPort(9898)
				.build();
		server.start();
		
	}
	
}
