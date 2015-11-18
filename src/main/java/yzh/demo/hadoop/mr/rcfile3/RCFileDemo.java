package yzh.demo.hadoop.mr.rcfile3;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * 
 * 导出runnable jar
 * 运行：
 * hadoop jar xxx.jar
 * @author yuezhihua
 * 
 */
public class RCFileDemo {

	public static void main(String[] args) throws IOException {
		conf = new Configuration();
//		Path src = new Path("/user/hive/warehouse/demo/000000_0");
//		Path src = new Path("/lenovo/*");
//		Path src = new Path(args[0]);
		//0-9
		for(int  i = 0 ; i <= 9 ; i++){
			Path src = new Path("/lenovo/00000"+i+"_0");
			readRcFile(src, conf);
		}
		//10-99
		for(int  i = 10 ; i <= 99 ; i++){
			Path src = new Path("/lenovo/0000"+i+"_0");
			readRcFile(src, conf);
		}
		//100-239
		for(int  i = 100 ; i <= 239 ; i++){
			Path src = new Path("/lenovo/000"+i+"_0");
			readRcFile(src, conf);
		}
		//2100-2399
		for(int  i = 2100 ; i <= 2399 ; i++){
			Path src = new Path("/lenovo/00"+i+"_0");
			readRcFile(src, conf);
		}
//		createRcFile(src, conf);
		
	}

	private static Configuration conf;
	private static final String TAB = "\t";
	private static final String ENTER = "\n";
	private static int i = 0;

	private static String strings[] = { "1,true,123.123,2012-10-24 08:55:00",
			"2,false,1243.5,2012-10-25 13:40:00",
			"3,false,24453.325,2008-08-22 09:33:21.123",
			"4,false,243423.325,2007-05-12 22:32:21.33454",
			"5,true,243.325,1953-04-22 09:11:33 " };

	/**
	 * 生成一个RCF file
	 * 
	 * @param src
	 * @param conf
	 * @throws IOException
	 */
	private static void createRcFile(Path src, Configuration conf)
			throws IOException {
		conf.setInt(RCFile.COLUMN_NUMBER_CONF_STR, 4);// 列数
		conf.setInt(RCFile.Writer.COLUMNS_BUFFER_SIZE_CONF_STR, 4 * 1024 * 1024);// 决定行数参数一
		conf.setInt(RCFile.RECORD_INTERVAL_CONF_STR, 3);// 决定行数参数二
		FileSystem fs = FileSystem.get(conf);
		RCFile.Writer writer = new RCFile.Writer(fs, conf, src);
		BytesRefArrayWritable cols = new BytesRefArrayWritable(4);// 列数，可以动态获取
		BytesRefWritable col = null;
		for (String s : strings) {
			String splits[] = s.split(",");
			int count = 0;
			for (String split : splits) {
				col = new BytesRefWritable(Bytes.toBytes(split), 0,
						Bytes.toBytes(split).length);
				cols.set(count, col);
				count++;
			}
			writer.append(cols);
		}
		writer.close();
		fs.close();
	}

	/**
	 * 读取解析一个RCF file
	 * 
	 * @param src
	 * @param conf
	 * @throws IOException
	 */
	private static void readRcFile(Path src, Configuration conf)
			throws IOException {
		// 需要获取的列，必须指定，具体看ColumnProjectionUtils中的设置方法
		ColumnProjectionUtils.setFullyReadColumns(conf);
		FileSystem fs = FileSystem.get(conf);
		RCFile.Reader reader = new RCFile.Reader(fs, src, conf);
		// readerByRow(reader);
		readerByCol(reader);
		reader.close();
	}

	protected static void readerByRow(RCFile.Reader reader) throws IOException {
		// 已经读取的行数
		LongWritable rowID = new LongWritable();
		// 一个行组的数据
		BytesRefArrayWritable cols = new BytesRefArrayWritable();
		while (reader.next(rowID)) {
			reader.getCurrentRow(cols);
			// 包含一列的数据
			BytesRefWritable brw = null;
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < cols.size(); i++) {
				brw = cols.get(i);
				// 根据start 和 length 获取指定行-列数据
				sb.append(Bytes.toString(brw.getData(), brw.getStart(),
						brw.getLength()));
				if (i < cols.size() - 1) {
					sb.append(ENTER);
				}
			}
			System.out.println(sb.toString());
		}
	}

	protected static synchronized void readerByCol(RCFile.Reader reader) throws IOException {
		// 一个行组的数据
		BytesRefArrayWritable cols = new BytesRefArrayWritable();
		while (reader.nextBlock()) {
			for (int count = 0; count < 4; count++) {
				cols = reader.getColumn(count, cols);
				BytesRefWritable brw = null;
				StringBuilder sb = new StringBuilder();
				for (int i = 0; i < cols.size(); i++) {
					brw = cols.get(i);
					// 根据start 和 length 获取指定行-列数据
					String phoneStr = Bytes.toString(brw.getData(), brw.getStart(),
							brw.getLength());
					if(!phoneStr.matches("(((\\+86)|(86))?(1)\\d{10})")){
						continue;
					}
					sb.append(phoneStr);
					if (i < cols.size() - 1) {
						sb.append(ENTER);
					}
					if(i==10){
						break;
					}
				}
//				System.out.println("=============================start--- ("+i+")======================================");
				System.out.println(sb.toString());
//				System.out.println("==============================end--- ("+i+")======================================");
				i++;
			}
		}
	}

}
