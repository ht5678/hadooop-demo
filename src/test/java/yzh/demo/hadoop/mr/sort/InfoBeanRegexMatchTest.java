package yzh.demo.hadoop.mr.sort;

import java.util.List;

import org.junit.Test;

import yzh.demo.hadoop.utils.RegexUtils;


/**
 * 正则匹配提交
 * @author sdwhy
 *
 */
public class InfoBeanRegexMatchTest {
	
	private static String str = "DVD2015042520574475493	DataBean [url=null, device=null, os=null, appVersion=null, appId=null, channel=null, count=67]";
	
	
	
	@Test
	public void testRegexMatch(){
		String regexStr = "(\\w+)	\\w+ \\[url=(\\w+), device=(\\w+), os=(\\w+), appVersion=(\\w+), appId=(\\w+), channel=(\\w+), count=(\\d+)\\]";
		List<String> results = RegexUtils.matchPattern(str, regexStr);
		for(String result : results){
			System.out.println(result);
		}
	}

}
