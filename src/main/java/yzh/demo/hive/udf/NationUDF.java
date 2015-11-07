package yzh.demo.hive.udf;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;



/**
 * 
 * 
 * hive自定义函数练习
 * 
 * 步骤：
 * 1.创建自定义实现udf的类,要集成UDF并且实现evaluate方法
 * 2.打包导出，然后上传到hive所在的服务器（文件位置没有要求，最好在hive目录下创建一个udf目录来存放）
 * 3.将jar添加到hive中
 * 		add jar /opt/hive0130/udf/nation_udf.jar; 
 * 4.为jar资源创建自定义函数名称，（getNation为函数名称，可以自己修改）
 * 		 create temporary function getNation as 'yzh.demo.hive.udf.NationUDF' ;
 * 5.然后就可以正常使用定义的函数了
 * 		select id , name ,size , getNation(nation) from beauties;
 * @author sdwhy
 *
 */
public class NationUDF extends UDF{

	
	public static Map<String, String> nationMap = new HashMap<>();
	
	static{
		nationMap.put("china", "中国");
		nationMap.put("japan", "日本");
	}
	

	
	/**
	 * 函数的自定义业务实现
	 * @param nation
	 * @return
	 */
	public Text evaluate(Text nation){
		String nation_en = nation.toString();
		String nation_cn = nationMap.get(nation_en);
		if(nation_cn == null ){
			nation_cn = "火星人";
		}
		return new Text(nation_cn);
	}
	
	
	
}
