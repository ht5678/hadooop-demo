package yzh.demo.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * 
 * @author sdwhy
 *
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		//accept
		String line = value.toString();
		//split
		String[] words = line.split(" ");
		//loop
		for(String w : words){
			context.write(new Text(w), new LongWritable(1));
		}
		
	}

	
	
}
