package yzh.demo.hadoop.mr.datacount;

import java.util.List;

import org.junit.Test;

import yzh.demo.hadoop.utils.RegexUtils;


/**
 * 测试匹配log的正则
 * @author sdwhy
 *
 */
public class RegexMatchTest {

	
	@Test
	public void testRegex(){
//		String line =  "223.104.5.234 - - [02/Jul/2015:00:01:32 +0800] \"POST /blf_api/book/rank HTTP/1.1\" 200 2532 pageIndex=4& \"-\" \"Dalvik/1.6.0 (Linux; U; Android 4.4.2; GT-I9205 Build/KOT49H)\" \"-\"";
//		String regex = "(/blf_api/.*) HTTP.*; ([Android|iOS]{1}.*); (.*) Build";
//		List<String> results = RegexUtils.matchPattern(line, regex);
		
		String line180 = "220.161.12.11 - - [23/Aug/2015:00:01:03 +0800] \"POST /blf_api/book/detail/v15 HTTP/1.1\" 200 973 userId=UID20150822212455863d5&bookId=BKD201505191219ab1ef& \"-\" \"Dalvik/2.1.0 (Linux; U; Android 5.0.2; PLK-AL10 Build/HONORPLK-AL10)\" \"-\" DVD20150822085113265152f1 A|1.8.0|5.0.2|PLK-AL10|m91 0.015";
		String regex = "(/blf_api/.*) HTTP.* (DVD2015\\w+) (A\\|(\\d.\\d.\\d)\\|(\\d.\\d.\\d)\\|(.*)\\|(\\w+) ){0,1}";
		List<String> results = RegexUtils.matchPattern(line180, regex);
		
		String lineiOs = "223.73.38.144 - - [23/Aug/2015:00:01:02 +0800] \"POST /blf_api/feed/list HTTP/1.1\" 200 8492 lastId=16674&userId=UID201508131137223bd47 \"-\" \"Boluofan/1.7.0 (iPhone; iOS 8.4.1; Scale/2.00)\" \"-\" DVD20150813113606001378f7 - 0.461";
//		List<String> results = RegexUtils.matchPattern(lineiOs, regex);
		
		for(String result : results){
			System.out.println(result);
		}
		
	}
	
}
