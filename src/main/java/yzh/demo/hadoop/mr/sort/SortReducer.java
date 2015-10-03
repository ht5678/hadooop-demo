package yzh.demo.hadoop.mr.sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * reducer
 * @author sdwhy
 *
 */
public class SortReducer extends Reducer<InfoBean, NullWritable, Text, InfoBean>{

	@Override
	protected void reduce(InfoBean key, Iterable<NullWritable> values,
			Reducer<InfoBean, NullWritable, Text, InfoBean>.Context context)
			throws IOException, InterruptedException {
		String appId = key.getAppId();
		context.write(new Text(appId), key);
	}

	
}
