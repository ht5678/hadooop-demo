package yzh.demo.hadoop.mr.rcfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 
 * @author sdwhy
 *
 */
public class ReadRcFileToText {

	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		conf.setInt("mapreduce.client.submit.file.replication", 20);
		Job job = Job.getInstance(conf);
		//notice
		job.setJarByClass(ReadRcFileToText.class);
//		job.setInputFormatClass(RCFileInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		//set mapper's property
		job.setMapperClass(ReadTestMapper.class);
		FileInputFormat.setInputPaths(job, new Path("/user/hive/warehouse/demo/000000_0"));
		
		//set reducer's property
		job.setReducerClass(WriteTestReducer.class);
		FileOutputFormat.setOutputPath(job, new Path("/rcfile"));
		
		//submit
		job.waitForCompletion(true);
	}
	
}
