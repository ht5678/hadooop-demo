package yzh.demo.hadoop.mr.sort;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import yzh.demo.hadoop.utils.RegexUtils;



/**
 * mapper
 * @author sdwhy
 *
 */
public class SortMapper extends Mapper<LongWritable, Text, InfoBean, NullWritable>{

	private static String regexStr = "(\\w+)	\\w+ \\[url=(\\w+), device=(\\w+), os=(\\w+), appVersion=(\\w+), appId=(\\w+), channel=(\\w+), count=(\\d+)\\]";
	
	private InfoBean k = new InfoBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, InfoBean, NullWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		List<String> results = RegexUtils.matchPattern(line, regexStr);
		
		String appId = results.get(0);
		String url = results.get(1);
		String count = results.get(7);
		
		k.set(url, appId, new Integer(count));
		
		context.write(k, NullWritable.get());
	}


}
