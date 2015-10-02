package yzh.demo.hadoop.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * 类CommonUtils.java的实现描述：常用工具类
 * @author yuezhihua 2015年1月30日 上午10:48:42
 */
public abstract class RegexUtils {
    
    
    /**
     * 匹配字符串
     * @param original
     * @param pattern
     * @return
     */
    public static List<String> matchPattern(String original , String regex){
        //空字符串处理
        if(StringUtils.isEmpty(original)){
            return null;
        }
        List<String> retVals = new ArrayList<String>();
        //编译正则表达式
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(original);
        int count = matcher.groupCount();
        int i =1;
        while (matcher.find(0) && i<=count) {
            String group = matcher.group(i);
            i++;
            retVals.add(group);
        }
        return retVals;
    }

}
