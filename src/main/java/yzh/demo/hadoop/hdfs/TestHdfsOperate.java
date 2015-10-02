package yzh.demo.hadoop.hdfs;

import java.io.FileInputStream;
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
import org.junit.Before;
import org.junit.Test;


/**
 * 测试上传下载
 * @author sdwhyue
 *
 */
public class TestHdfsOperate {

	
	private FileSystem fs = null;
	
	/**
	 * 初始化
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws InterruptedException 
	 */
	@Before
	public void init() throws IOException, URISyntaxException, InterruptedException{
		//这种方式需要在host文件中设置
//		fs = FileSystem.get(new URI("hdfs://hadoop115:9000"), new Configuration());
		
//		fs = FileSystem.get(new URI("hdfs://192.168.10.115:9000"), new Configuration());
		
		//设置伪用户获取上传权限
		fs = FileSystem.get(new URI("hdfs://192.168.10.115:9000"), new Configuration(),"root");
	}
	
	
	
	/**
	 * 创建目录
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testCreateDir() throws IllegalArgumentException, IOException{
		boolean flag = fs.mkdirs(new Path("test"));
		System.out.println(flag);
	}
	
	
	
	/**
	 * 尝试删除
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testDel() throws IllegalArgumentException, IOException{
		//true代表文件夹下递归删除，false表示不递归
		boolean flag = fs.delete(new Path("/spring-doc.pdf"), false);
		System.out.println(flag);
	}
	
	
	/**
	 * 尝试拷贝文件
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testCopyFileToLocal() throws IllegalArgumentException, IOException{
		fs.copyToLocalFile(new Path("/spring-doc.pdf"), new Path("g:/test.pdf"));
	}
	
	
	
	/**
	 * 测试上传
	 * @throws IOException 
	 * @throws IllegalArgumentException 
	 */
	@Test
	public void testUpload() throws IllegalArgumentException, IOException{
		//文件本地路径
		InputStream in = new FileInputStream("f:/spring-framework-reference.pdf");
		//文件hadoop的路径
		OutputStream out = fs.create(new Path("/spring-doc.pdf"));
		IOUtils.copyBytes(in, out, 4096, true);
	}
	
	
	/**
	 * 测试下载
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDownload() throws IllegalArgumentException, IOException{
		//文件hadoop的路径
		InputStream is = fs.open(new Path("/blf_20150801.log"));
		//文件本地路径
		OutputStream out = new FileOutputStream("d://test.log");
		IOUtils.copyBytes(is, out, 4096, true);
	}
	
	
}
