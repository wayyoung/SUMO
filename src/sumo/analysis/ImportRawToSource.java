package sumo.analysis;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.io.File;
import java.io.FileInputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;

public class ImportRawToSource extends SumoMongo{
	public void doImport() {
		
	}
	private static final Logger LOGGER = Logger.getLogger( ImportRawToSource.class.getName() );
	public static void main(String[] args) throws Exception{
		// TODO Auto-generated method stub
		MongoClient mcl=null;
		// Create a CodecRegistry containing the PojoCodecProvider instance.
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		
		try {
//			String fname="K:/SUMO/exports/2018-04-02.json.gz"; 
			mcl = new MongoClient("localhost",MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build());
			MongoDatabase mdb=mcl.getDatabase("SUMO_DEV1_510");
			MongoCollection<SumoMessage> mcol=mdb.getCollection("source",SumoMessage.class);
			mcol.createIndex(new BasicDBObject("messageid",1),new IndexOptions().unique(true));
			
			ObjectMapper mapper = new ObjectMapper();
			JsonFactory jsonFactory = mapper.getFactory(); // or, for data binding, org.codehaus.jackson.mapper.MappingJsonFactory 
			File[] flist=new File("K:/SUMO/DEV1_510/exports/").listFiles();
//			File[] flist= {new File("K:/SUMO/DEV_510/exports/2018-04-01.json.gz")};
			
			for(int i=0;i<flist.length;i++) {
				String fname=flist[i].getCanonicalPath();
				int tc=0;
				if(fname.contains("#"))tc=Integer.parseInt(fname.substring(fname.indexOf("#")+1, fname.indexOf(".")));
				LOGGER.log(Level.INFO,"Importing "+fname);
				JsonParser jp = jsonFactory.createParser(new GZIPInputStream(new FileInputStream(fname)));
				JsonToken tk=null;
				int count=0;
				int inserted=0;
				while((tk=jp.nextToken())!=null && (!tk.equals(JsonToken.END_OBJECT))) {
					if(JsonToken.START_ARRAY.equals(tk)) {tk=jp.nextToken();}
					if(tk.equals(JsonToken.END_ARRAY)) {continue;}
					SumoMessage msg=jp.readValueAs(SumoMessage.class);
					msg.withMtime(new java.util.Date(msg.getMessagetime())).withRtime(new java.util.Date(msg.getReceipttime()));
					msg.correctDrifttedMtime();		
					try {
						mcol.insertOne(msg);
						inserted++;
					}catch(MongoWriteException mex) {
						if(11000!=mex.getCode()) {
							throw mex;
						}
					}
					count++;
					if(0==count%100000)LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted);
				}
				LOGGER.log(Level.INFO,"  count="+count+(0==tc?"":("/"+tc))+", inserted="+inserted);
			}
		
		}finally {
			try{mcl.close();}catch(Exception exp) {}
		}
		
	}

}
