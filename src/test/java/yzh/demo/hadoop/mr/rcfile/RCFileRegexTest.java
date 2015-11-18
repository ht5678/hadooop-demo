package yzh.demo.hadoop.mr.rcfile;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Test;

import yzh.demo.hadoop.utils.RegexUtils;

/**
 * 
 * @author yuezhihua
 *
 */
public class RCFileRegexTest {
	
	
	
	@Test
	public void testUtil(){
		String regex = "(((\\d)|(\\+\\d))?\\d{11}) ";
		String str = "+281887316719   +281887316719   13825273335     18616611157     1861661115715889604962      13901700150     13472639371     +281743625829   886938889888    18659880055    +281887316719   +281887316719   13825273335     18616611157     1861661115715889604962      13901700150     13472639371     +281743625829   886938889888    18659880055     13605980809     281752492180    18629314104 ";
		List<String> list =  RegexUtils.matchFirstGroupPattern(str, regex);
		for(String s : list){
			System.out.println(s);
		}
		
	}

	
	
	@Test
	public void testRegex(){
//		String regex = "(((\\d)|(\\+\\d))?\\d{11}) ";
		String regex = "(((\\+86)|(86))?(1)\\d{10}) ";
		
//		String regex = ".*";
		String str = "+281887316719   +281887316719   13825273335     18616611157     1861661115715889604962      13901700150     13472639371     +281743625829   886938889888    18659880055    +281887316719   +281887316719   13825273335     18616611157     1861661115715889604962      13901700150     13472639371     +281743625829   886938889888    18659880055     13605980809     281752492180    18629314104 ";
		Pattern pattern = Pattern.compile(regex);
		Matcher matcher = pattern.matcher(str);
//		int count = matcher.groupCount();
//		System.out.println(count);
//		matcher.
		
		int i = 0;
		
		while(matcher.find()){
			System.out.println(matcher.group(0));
			System.out.println(matcher.group(0).length());
			i++;
		}
		System.out.println(i);
	}
	
}
