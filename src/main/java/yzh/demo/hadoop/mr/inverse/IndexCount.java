package yzh.demo.hadoop.mr.inverse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * 
 * @author sdwhy
 *
 */
public class IndexCount implements Writable{
	
	
	
	
	private String appId;
	
	private String path;
	
	
	
	
	
	public IndexCount() {
	}
	
	
	

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(path);
		out.writeUTF(appId);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		path = in.readUTF();
		appId = in.readUTF();
	}
	


	@Override
	public String toString() {
		return "IndexCount [appId=" + appId + ", path=" + path + "]";
	}
	
	
	
	
	

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public String getAppId() {
		return appId;
	}

	public void setAppId(String appId) {
		this.appId = appId;
	}



}
