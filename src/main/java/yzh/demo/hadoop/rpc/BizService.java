package yzh.demo.hadoop.rpc;

/**
 * 
 * @author sdwhy
 *
 */
public interface BizService {
	
	/** 当服务端和客户端的version不同步的时候，就会出现异常，
	 * 测试的时候客户端的服务端分开，使用各自独立的bizservice，然后在修改版本号，就会出现这个问题
	 */
	public static final int versionID = 10100;
	
	public String sayHello(String name);

}
