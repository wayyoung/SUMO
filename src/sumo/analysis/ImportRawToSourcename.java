package sumo.analysis;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Projections.*;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.Block;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;

public class ImportRawToSourcename extends SumoMongo {
	private static final Logger LOGGER = Logger.getLogger( ImportRawToSourcename.class.getName() );
	
	
	boolean enableMessageidCacheFilter=true;
	HashMap<String,TreeSet<Long>> existedMsgIdMap=new HashMap<String,TreeSet<Long>>();
	
	public void doImport(File rawFile) throws JsonParseException, FileNotFoundException, IOException {
		TreeSet<String> sourcenameList=(TreeSet<String>) this.querySourcenameList();
		ObjectMapper mapper = new ObjectMapper();
		JsonFactory jsonFactory = mapper.getFactory(); 
		String fname=rawFile.getName();
		int tc=0;
		if(fname.contains("#"))tc=Integer.parseInt(fname.substring(fname.indexOf("#")+1, fname.indexOf(".")));
		LOGGER.log(Level.INFO,"Importing "+fname);
		JsonParser jp = jsonFactory.createParser(new GZIPInputStream(new FileInputStream(rawFile)));
		JsonToken tk=null;
		int count=0;
		int inserted=0;
		int abandoned=0;
		
		
		while((tk=jp.nextToken())!=null && (!tk.equals(JsonToken.END_OBJECT))) {
			if(JsonToken.START_ARRAY.equals(tk)) {tk=jp.nextToken();}
			if(tk.equals(JsonToken.END_ARRAY)) {continue;}
			SumoMessage msg=jp.readValueAs(SumoMessage.class);
			msg.withMtime(new java.util.Date(msg.getMessagetime())).withRtime(new java.util.Date(msg.getReceipttime()));
			msg.correctDrifttedMtime();		
			String sname=msg.getSourcename();
			
			MongoCollection<SumoMessage> scol=null;
			if(!sourcenameList.contains(sname)) {
				this.createSourceameCollection(sname);
				sourcenameList=(TreeSet<String>) this.querySourcenameList();
			}
			scol=mdb.getCollection(sname,SumoMessage.class);
			if(enableMessageidCacheFilter) {				
				if(!existedMsgIdMap.containsKey(sname)) {
					TreeSet<Long> ss=new TreeSet<Long>();
					scol.aggregate(Arrays.asList(
							project(fields(excludeId(),include("messageid")))
							)).forEach(new Block<SumoMessage>() {
						          @Override
						          public void apply(final SumoMessage msg) {
						        	  ss.add(msg.getMessageid());
						          }
						      });
					existedMsgIdMap.put(sname, ss);
					
				}
			}
			
			TreeSet<Long> existedMsgIdSet=existedMsgIdMap.get(sname);
			
			if((!enableMessageidCacheFilter) || (!existedMsgIdSet.contains(msg.getMessageid()))){
				try {
					
					scol.insertOne(msg);
					 existedMsgIdSet.add(msg.getMessageid());
					inserted++;
				}catch(MongoWriteException mex) {
					if(11000!=mex.getCode()) {
						throw mex;
					}
					abandoned++;
				}
			}else {
				abandoned++;
			}
			count++;
			if(0==count%100000)LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted);
		}
		LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted+", abandoned="+abandoned);
		LOGGER.log(Level.INFO,"Import "+fname+" finished.\n");
	}

	public static void main(String[] args) throws Exception{
		ImportRawToSourcename imts=new ImportRawToSourcename();
		
		imts.setDbname("SUMO_DEV1_510");
		imts.setUrl("localhost");
		imts.init();
		File[] flist=new File("K:/SUMO/DEV1_510/exports/tmp").listFiles();
//		File[] flist= {new File("K:/SUMO/DEV1_510/exports/2018-04-27#3656999.json.gz"),
//				new File("K:/SUMO/DEV1_510/exports/2018-04-30#3283402.json.gz")};
		Arrays.sort(flist);
		for(File f :flist) {
			//System.out.println(f.getName());
			imts.doImport(f);
		}
		
	}

}
