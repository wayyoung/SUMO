package sumo.analysis;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.codecs.pojo.annotations.BsonIgnore;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({ "_messageid", "_sourcecategory", "_sourcename", "_source", "component", "fw_version",
		"_receipttime", "_messagetime", "_rtime", "_mtime", "_raw", "_collector", "_sourcehost" })
public class SumoMessage implements Serializable {

	@JsonProperty("_messageid")
	private Long messageid;
	@JsonProperty("_sourcecategory")
	private String sourcecategory;
	@JsonProperty("_sourcename")
	private String sourcename;
	@JsonProperty("_source")
	private String source;
	@JsonProperty("component")
	private String component;
	@JsonProperty("fw_version")
	private String fwVersion;
	@JsonProperty("_receipttime")
	private Long receipttime;
	@JsonProperty("_messagetime")
	private Long messagetime;
	@JsonProperty("_rtime")
	private java.util.Date rtime;
	@JsonProperty("_mtime")
	private java.util.Date mtime;
	@JsonProperty("_raw")
	private String raw;
	@JsonProperty("_collector")
	private String collector;
	@JsonProperty("_sourcehost")
	private String sourcehost;
	@BsonIgnore
	@JsonIgnore
	private Map<String, String> additionalProperties = new HashMap<String, String>();
	private final static long serialVersionUID = 5055059130270793603L;

	/**
	 * No args constructor for use in serialization
	 * 
	 */
	public SumoMessage() {
	}

	/**
	 * 
	 * @param raw
	 * @param collector
	 * @param fwVersion
	 * @param source
	 * @param component
	 * @param rtime
	 * @param messagetime
	 * @param receipttime
	 * @param mtime
	 * @param sourcename
	 * @param sourcecategory
	 * @param messageid
	 * @param sourcehost
	 */
	public SumoMessage(Long messageid, String sourcecategory, String sourcename, String source, String component,
			String fwVersion, Long receipttime, Long messagetime, java.util.Date rtime, java.util.Date mtime, String raw,
			String collector, String sourcehost) {
		super();
		this.messageid = messageid;
		this.sourcecategory = sourcecategory;
		this.sourcename = sourcename;
		this.source = source;
		this.component = component;
		this.fwVersion = fwVersion;
		this.receipttime = receipttime;
		this.messagetime = messagetime;
		this.rtime = rtime;
		this.mtime = mtime;
		this.raw = raw;
		this.collector = collector;
		this.sourcehost = sourcehost;
	}

//	@JsonProperty("_messageid")
	public Long getMessageid() {
		return messageid;
	}

//	@JsonProperty("_messageid")
	public void setMessageid(Long messageid) {
		this.messageid = messageid;
	}

	public SumoMessage withMessageid(Long messageid) {
		this.messageid = messageid;
		return this;
	}

//	@JsonProperty("_sourcecategory")
	public String getSourcecategory() {
		return sourcecategory;
	}

//	@JsonProperty("_sourcecategory")
	public void setSourcecategory(String sourcecategory) {
		this.sourcecategory = sourcecategory;
	}

	public SumoMessage withSourcecategory(String sourcecategory) {
		this.sourcecategory = sourcecategory;
		return this;
	}

//	@JsonProperty("_sourcename")
	public String getSourcename() {
		return sourcename;
	}

//	@JsonProperty("_sourcename")
	public void setSourcename(String sourcename) {
		this.sourcename = sourcename;
	}

	public SumoMessage withSourcename(String sourcename) {
		this.sourcename = sourcename;
		return this;
	}

//	@JsonProperty("_source")
	public String getSource() {
		return source;
	}

//	@JsonProperty("_source")
	public void setSource(String source) {
		this.source = source;
	}

	public SumoMessage withSource(String source) {
		this.source = source;
		return this;
	}

//	@JsonProperty("component")
	public String getComponent() {
		return component;
	}

//	@JsonProperty("component")
	public void setComponent(String component) {
		this.component = component;
	}

	public SumoMessage withComponent(String component) {
		this.component = component;
		return this;
	}

//	@JsonProperty("fw_version")
	public String getFwVersion() {
		return fwVersion;
	}

//	@JsonProperty("fw_version")
	public void setFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
	}

	public SumoMessage withFwVersion(String fwVersion) {
		this.fwVersion = fwVersion;
		return this;
	}

//	@JsonProperty("_receipttime")
	public Long getReceipttime() {
		return receipttime;
	}

//	@JsonProperty("_receipttime")
	public void setReceipttime(Long receipttime) {
		this.receipttime = receipttime;
	}

	public SumoMessage withReceipttime(Long receipttime) {
		this.receipttime = receipttime;
		return this;
	}

//	@JsonProperty("_messagetime")
	public Long getMessagetime() {
		return messagetime;
	}

//	@JsonProperty("_messagetime")
	public void setMessagetime(Long messagetime) {
		this.messagetime = messagetime;
	}

	public SumoMessage withMessagetime(Long messagetime) {
		this.messagetime = messagetime;
		return this;
	}

//	@JsonProperty("_rtime")
	public java.util.Date getRtime() {
		return rtime;
	}

//	@JsonProperty("_rtime")
	public void setRtime(java.util.Date rtime) {
		this.rtime = rtime;
	}

	public SumoMessage withRtime(java.util.Date rtime) {
		this.rtime = rtime;
		return this;
	}

//	@JsonProperty("_mtime")
	public java.util.Date getMtime() {
		return mtime;
	}

//	@JsonProperty("_mtime")
	public void setMtime(java.util.Date mtime) {
		this.mtime = mtime;
	}

	public SumoMessage withMtime(java.util.Date mtime) {
		this.mtime = mtime;
		return this;
	}

//	@JsonProperty("_raw")
	public String getRaw() {
		return raw;
	}

//	@JsonProperty("_raw")
	public void setRaw(String raw) {
		this.raw = raw;
	}

	public SumoMessage withRaw(String raw) {
		this.raw = raw;
		return this;
	}

//	@JsonProperty("_collector")
	public String getCollector() {
		return collector;
	}

//	@JsonProperty("_collector")
	public void setCollector(String collector) {
		this.collector = collector;
	}

	public SumoMessage withCollector(String collector) {
		this.collector = collector;
		return this;
	}

//	@JsonProperty("_sourcehost")
	public String getSourcehost() {
		return sourcehost;
	}

//	@JsonProperty("_sourcehost")
	public void setSourcehost(String sourcehost) {
		this.sourcehost = sourcehost;
	}

	public SumoMessage withSourcehost(String sourcehost) {
		this.sourcehost = sourcehost;
		return this;
	}

	@JsonAnyGetter
	public Map<String, String> getAdditionalProperties() {
		return this.additionalProperties;
	}

	@JsonAnySetter
	public void setAdditionalProperty(String name, String value) {
		this.additionalProperties.put(name, value);
	}

	public SumoMessage withAdditionalProperty(String name, String value) {
		this.additionalProperties.put(name, value);
		return this;
	}

	private Pattern dmp = Pattern.compile("(\\d{2}-\\d{2})");
	Calendar cal=Calendar.getInstance(TimeZone.getTimeZone("UTC"));
	static SimpleDateFormat df = new SimpleDateFormat("MM-dd");
	static {
		df.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public boolean correctDrifttedMtime() {
		Matcher m = dmp.matcher(raw);
		if (m.find()) {
			String str = df.format(mtime);
			if (!m.group(0).equals(str)) {
//				System.out.println("UPDATE MTIME. " + str + "->" + m.group(0) + ". [" + messageid + "]:" + raw);
				
				cal.setTime(mtime);
				cal.set(Calendar.MONTH,Integer.parseInt(str.substring(0, 2)));
				cal.set(Calendar.DAY_OF_MONTH,Integer.parseInt(str.substring(3, 5)));
				mtime=cal.getTime();
			}
			
			return true;
		}
		return false;
	}

}