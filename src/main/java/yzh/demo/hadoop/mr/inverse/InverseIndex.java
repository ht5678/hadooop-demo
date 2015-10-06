package yzh.demo.hadoop.mr.inverse;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import yzh.demo.hadoop.utils.RegexUtils;


/**
 * 
 * 倒排索引
 * 
 * 倒排索引的数据结构：
 * 你好{1,3,4}
 * 世界{1,1,1}
 * 
 * 
 * 应用场景：
 * a.txt
 * 你好 啊 这个 世界
 * 不好 不好
 * 
 * b.txt
 * 你好 你好
 * 
 * 输出：
 * 你好:{a.txt:1,b.xtx:2}
 * 啊{a.txt:1,b.txt:0}
 * 不好{a.txt:2,b.txt:0}
 * ... ... 
 * 
 * 
 * 运行：
 * hadoop jar dc_inverse.jar yzh.demo.hadoop.mr.inverse.InverseIndex /access-20150903.log /dc_inverse
 * 
 * @author sdwhy
 *
 */
public class InverseIndex {
	
	
	
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(InverseIndex.class);
		
		//mapper
		job.setMapperClass(InverseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//reducer
		job.setReducerClass(InverseCombiner.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//combiner
		job.setCombinerClass(InverseCombiner.class);
		
		job.waitForCompletion(true);
	}
	
	
	
	
	public static class InverseCombiner extends Reducer<Text, LongWritable, Text, LongWritable>{

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			long count = 0;
			for(LongWritable lw : values){
				count = count + lw.get();
			}
			
			
			context.write(key, new LongWritable(count));
			
		}

	}
	
	
	
	
	
	
	public static class InverseMapper extends Mapper<LongWritable, Text, Text, LongWritable>{
		//正则表达式
		//1:url
		//2:appId
		//3:app版本号
		//4:手机系统版本号
		//5.手机型号
		//6.下载的渠道
		private static String regex = "(DVD2015\\w+)";

		@Override
		protected void map(LongWritable key,Text value,Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			//路径
			FileSplit fs = (FileSplit) context.getInputSplit();
			String path = fs.getPath().toString();
			
			//数据处理
			String line = value.toString();
			List<String> results = RegexUtils.matchPattern(line, regex);
			if(results == null || results.size()==0){
				return;
			}
			String appId = results.get(0);
			
			
			IndexCount ic = new IndexCount();
			ic.setAppId(appId);
			ic.setPath(path);
			
			context.write(new Text(ic.toString()), new LongWritable(1));
		}

		
	}
	
	

}
