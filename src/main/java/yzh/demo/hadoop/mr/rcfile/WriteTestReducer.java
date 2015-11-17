package yzh.demo.hadoop.mr.rcfile;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * 
 * @author sdwhy
 *
 */
public class WriteTestReducer extends Reducer<Text, NullWritable, Text, NullWritable>{

	
	
	@Override
	protected void reduce(Text key, Iterable<NullWritable> value,
			Reducer<Text, NullWritable, Text, NullWritable>.Context context)
			throws IOException, InterruptedException {
		
		context.write(key, NullWritable.get());
	}

	
	
}
