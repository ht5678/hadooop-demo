package yzh.demo.hadoop.mr.datacount;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import yzh.demo.hadoop.utils.RegexUtils;


/**
 * 
 * 执行方式：（export  jar （如果导出的时候指定了main类，就使用下列指令，如果没有指定，就需要在dc.jar后添加main类的完全限定名：yzh.demo.hadoop.mr.datacount.DataCount））
 * hadoop jar dc.jar  /test.log /count
 * 
 * 执行方式，在导出jar的时候不指定main类，执行的时候指定main类
 * hadoop jar dc_partition.jar yzh.demo.hadoop.mr.datacount.DataCount  /access-20150822.log /dc_p4 4
 * 
 * hadoop使用mapreduce统计日志信息
 * @author sdwhy
 *
 */
public class DataCount {
	
	//正则表达式
	//1:url
	//2:appId
	//3:app版本号
	//4:手机系统版本号
	//5.手机型号
	//6.下载的渠道
	private static String regex = "(/blf_api/.*) HTTP.* (DVD2015\\w+) (A\\|(\\d.\\d.\\d)\\|(\\d.\\d.\\d)\\|(.*)\\|(\\w+) ){0,1}";
	
	
	/**
	 * 执行
	 * @param args
	 */
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(DataCount.class);
		
		//mapper,如果是在一个文件中，mapper必须是static的，否则报错
		job.setMapperClass(DCMapper.class);
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//reducer,如果是在一个文件中，reducer必须是static的，否则报错
		job.setReducerClass(DCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DataBean.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//设置分区规则和reduce的数量
		//分区的时间是在mapper 和 reducer之间,这之间用jps查看可以看到临时的yarnchild进程
		//reducetask的数量和分区的文件多少有关系，如果设置成2，那么结果文件会出现两个：par0001，par0002...
		job.setPartitionerClass(ProviderPartitioner.class);
		job.setNumReduceTasks(Integer.valueOf(args[2]));
		
		//等待并且打印信息
		job.waitForCompletion(true);
	}
	
	
	
	/**
	 * 分区规则修改
	 * @author sdwhy
	 *
	 */
	public static class ProviderPartitioner extends Partitioner<Text, DataBean>{

		
		/**
		 * 这个地方最好根据key来进行分区，因为value还没有到reduce阶段，所以value在这个时候不是最终的value
		 */
		@Override
		public int getPartition(Text key, DataBean value, int numPartitions) {
//			System.out.println("output:"+value.getCount());
//			if(value.getCount()<10000){
//				return 0;
//			}else if(value.getCount()>=10000 && value.getCount()<20000){
//				return 1;
//			}else{
//				return 2;
//			}
			if(value.getCount()<10000){
				return 1;
			}else{
				return 2;
			}
		}
		
	}
	
	
	
	/**
	 * data -- map
	 * @author sdwhy
	 *
	 */
	public static class DCMapper extends Mapper<LongWritable, Text, Text, DataBean>{

		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			if(line == null || StringUtils.isEmpty(line)){
				return;
			}
			List<String> results = RegexUtils.matchPattern(line, regex);
			if(results!=null && results.size()>2){
				DataBean db = new DataBean();
				db.setUrl(results.get(0));
				db.setAppId(results.get(1));
				if(results.size()>2){
					db.setAppVersion(results.get(3));
					db.setOs(results.get(4));
					db.setDevice(results.get(5));
					db.setChannel(results.get(6));
				}
				//key--value  计算每个设备的访问次数
				context.write(new Text(db.getAppId()), db);
			}
		}
		
	}
	
	
	/**
	 * data -- reducer
	 * @author sdwhy
	 *
	 */
	public static class DCReducer extends Reducer<Text, DataBean, Text, DataBean>{

		@Override
		protected void reduce(Text key, Iterable<DataBean> v2s,
				Reducer<Text, DataBean, Text, DataBean>.Context context)
				throws IOException, InterruptedException {
			long urlCount = 0;
			long appIdCount = 0;
			for(DataBean db : v2s){
				appIdCount++;
			}
			DataBean db = new DataBean();
			db.setCount(appIdCount);
			//key--value 计算每个设备的访问次数
			context.write(key, db);
		}
		
	}
	

}
