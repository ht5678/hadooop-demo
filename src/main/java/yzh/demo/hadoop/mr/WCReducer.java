package yzh.demo.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author sdwhy
 *
 */
public class WCReducer extends Reducer<Text, LongWritable, Text, LongWritable>{

	
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		long counter = 0;
		//loop
		for(LongWritable l : values){
			counter += l.get();
		}
		//write
		context.write(key, new LongWritable(counter));
	}

	
	
}
