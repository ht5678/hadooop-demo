package yzh.demo.hadoop.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * 排序,先由<b>DataCount</b>来计算每个appId的访问次数
 * 然后在通过<a>SortStep</a>来根据访问次数排序
 * 
 * 
 * 运行的时候需要制定main类
 *  hadoop jar  dc_sort.jar  yzh.demo.hadoop.mr.sort.SortStep /dc_p4/part-r-00001 /dc_sort
 * 
 * hdfs:根目录路径
 * hdfs://hadoop115:9000/
 * 
 * @author sdwhy
 *
 */
public class SortStep {
	
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SortStep.class);
		
		//mapper
		job.setMapperClass(SortMapper.class);
		job.setMapOutputKeyClass(InfoBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		//2.在linux操作系统下进行操作可以进行断点
//		FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.10.115:9000/dc_p4/part-r-00001"));
		
		
		//reducer
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(InfoBean.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//2.在linux操作系统下进行操作可以进行断点
//		FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.10.115:9000/dc_sort"));
		
		//设置combiner,可以用于在node节点的相加，过滤等不影响reducer结果的操作，在node进行部分cobiner操作可以增加效率
//		job.setCombinerClass(SortReducer.class);
		
		job.waitForCompletion(true);
		
	}

}
