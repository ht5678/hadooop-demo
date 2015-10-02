package yzh.demo.hadoop.mr.datacount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;


/**
 * 序列化对象
 * @author sdwhy
 *
 */
public class DataBean implements Writable{
	
	private String url;
	
	private String device;
	
	private String os;
	
	private String appVersion;
	
	private String appId;
	
	private String channel;
	
	private long count;
	
	
	
	
	
	public DataBean(){
		
	}
	
	
	
	
	//序列化
	public void readFields(DataInput input) throws IOException {
		this.appId = input.readUTF();
		this.device = input.readUTF();
		this.os = input.readUTF();
		this.url = input.readUTF();
		this.channel = input.readUTF();
		this.count = input.readLong();
		this.appVersion = input.readUTF();
	}

	
	//反序列化
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.getAppId()==null?"":this.getAppId());
		out.writeUTF(this.getDevice()==null?"":this.getDevice());
		out.writeUTF(this.getOs()==null?"":this.getOs());
		out.writeUTF(this.getUrl()==null?"":this.getUrl());
		out.writeUTF(this.getChannel()==null?"":this.getChannel());
		out.writeLong(this.getCount());
		out.writeUTF(this.getAppVersion()==null?"":this.getAppVersion());
	}
	
	
	
	/*----------------------get & set ----------------------*/
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDevice() {
		return device;
	}
	public void setDevice(String device) {
		this.device = device;
	}
	public String getOs() {
		return os;
	}
	public void setOs(String os) {
		this.os = os;
	}
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public String getAppVersion() {
		return appVersion;
	}
	public void setAppVersion(String appVersion) {
		this.appVersion = appVersion;
	}
	public String getChannel() {
		return channel;
	}
	public void setChannel(String channel) {
		this.channel = channel;
	}
	public long getCount() {
		return count;
	}
	public void setCount(long count) {
		this.count = count;
	}




	@Override
	public String toString() {
		return "DataBean [url=" + url + ", device=" + device + ", os=" + os
				+ ", appVersion=" + appVersion + ", appId=" + appId
				+ ", channel=" + channel + ", count=" + count + "]";
	}
	
	
	

}
