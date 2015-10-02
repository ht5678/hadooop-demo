package yzh.demo.hadoop.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * 进行测试的时候要启动dfs和yarn，否则不能正常使用
 * 代码里边的path指的是hdfs里边的path，不是linux系统里边的path
 * 
 * @author sdwhy
 *
 */
public class WordCount {
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.client.submit.file.replication", 20);
		Job job = Job.getInstance(conf);
		//notice
		job.setJarByClass(WordCount.class);
		//set mapper's property
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path("/blf_20150801.log"));
		
		//set reducer's property
		job.setReducerClass(WCReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("/wcount.txt"));
		
		//submit
		job.waitForCompletion(true);
	}

}
