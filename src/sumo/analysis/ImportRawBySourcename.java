package sumo.analysis;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;
import com.jcabi.jdbc.SingleOutcome;
import com.mongodb.Block;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Projections.*;

public class ImportRawBySourcename extends SumoMySQL {
	private static final Logger LOGGER = Logger.getLogger( ImportRawBySourcename.class.getName() );
	
	
	boolean enableMessageidCacheFilter=true;
	HashMap<String,TreeSet<Long>> existedMsgIdMap=new HashMap<String,TreeSet<Long>>();
	
	public void doImport(File rawFile) throws JsonParseException, FileNotFoundException, IOException,SQLException {
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
		
		JdbcSession jcse=new JdbcSession(dataSource);
		jcse.autocommit(false);
		while((tk=jp.nextToken())!=null && (!tk.equals(JsonToken.END_OBJECT))) {
			if(JsonToken.START_ARRAY.equals(tk)) {tk=jp.nextToken();}
			if(tk.equals(JsonToken.END_ARRAY)) {continue;}
			SumoMessage msg=jp.readValueAs(SumoMessage.class);
			msg.withMtime(new java.util.Date(msg.getMessagetime())).withRtime(new java.util.Date(msg.getReceipttime()));
			msg.correctDrifttedMtime();		
			String sname=msg.getSourcename();



			if(!sourcenameList.contains(sname)) {
				this.createSourceame(sname);
				sourcenameList=(TreeSet<String>) this.querySourcenameList();
			}

//			scol=mdb.getCollection(sname,SumoMessage.class);
			if(enableMessageidCacheFilter) {
				if(!existedMsgIdMap.containsKey(sname)) {
					TreeSet<Long> ss=new TreeSet<Long>();
					jse.sql("SELECT messageid from `"+sname+"`").select(new Outcome<Collection<Long>>() {
						@Override
						public Collection<Long> handle(ResultSet resultSet, Statement statement) throws SQLException {
							while(resultSet.next()) {
								ss.add(resultSet.getLong(1));
							}
							return ss;
						}
					});

					existedMsgIdMap.put(sname, ss);

				}
			}


			TreeSet<Long> existedMsgIdSet=existedMsgIdMap.get(sname);
			
			if((!enableMessageidCacheFilter) || (!existedMsgIdSet.contains(msg.getMessageid()))){
				try {

					jcse.sql("INSERT INTO `"+dbname+"`.`"+sname+"`  (messageid,sourcename,sourcecategory,source,component,fw_version,sourcehost,receipttime,messagetime,mtime,rtime,raw) VALUES (?, ? , ? , ? , ? , ? , ? , ? , ? , ? ,?, ? )")
							.set(msg.getMessageid())
							.set(msg.getSourcename())
							.set(msg.getSourcecategory())
							.set(msg.getSource())
							.set(msg.getComponent())
							.set(msg.getFwVersion())
							.set(msg.getSourcehost())
							.set(msg.getReceipttime())
							.set(msg.getMessagetime())
							.set(new java.sql.Date(msg.getMtime().getTime()))
							.set(new java.sql.Date(msg.getRtime().getTime()))
							.set(msg.getRaw())
							.update(new SingleOutcome<Long>(Long.class));
					if(enableMessageidCacheFilter) {
						existedMsgIdSet.add(msg.getMessageid());
					}
					inserted++;
				}catch(SQLException mex) {
					if(!mex.getMessage().contains("Duplicate entr")) {
						throw mex;
					}
					abandoned++;
				}
			}else {
				abandoned++;
			}
			count++;
			if(0==count%10000){LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted); if(inserted > 0){ jcse.commit();}}
		}
		LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted+", abandoned="+abandoned);
		LOGGER.log(Level.INFO,"Import "+fname+" finished.\n");
	}

	public static void main(String[] args) throws Exception{
		ImportRawBySourcename imts=new ImportRawBySourcename();
		
		imts.setDbname("sumo_dev1_510");
//		imts.setUrl("localhost");
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
