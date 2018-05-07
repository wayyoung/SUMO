package sumo.analysis;

import static com.mongodb.client.model.Aggregates.match;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.Arrays;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;

public class QuerySourceName {
	private static final Logger LOGGER = Logger.getLogger( QuerySourceName.class.getName() );
	public static void main(String args[])throws Exception{
		MongoClient mcl=null;
		// Create a CodecRegistry containing the PojoCodecProvider instance.
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
                fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		
		try {
//			String fname="K:/SUMO/exports/2018-04-02.json.gz"; 
			mcl = new MongoClient("localhost",MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build());
			MongoDatabase mdb=mcl.getDatabase("SUMO_DEV1_510");
			
			MongoCollection<SumoMessage> mcol=mdb.getCollection("source",SumoMessage.class);
			MongoCursor<String> scsr=mcol.distinct("sourcename",String.class).iterator();
			while(scsr.hasNext()) {
				String sname=scsr.next();
				
				System.out.println(sname);
				MongoCollection<SumoMessage> scol=null;
				try {
					mdb.createCollection(sname);
				}catch(MongoCommandException exx) {
					if(48!=exx.getCode()) {
						throw exx;
					}
				}
				
				scol=mdb.getCollection(sname,SumoMessage.class);
				scol.createIndex(new BasicDBObject("messageid",1),new IndexOptions().unique(true));
				scol.createIndex(new BasicDBObject("sourcename",1));
				scol.createIndex(new BasicDBObject("fwVersion",1));
				scol.createIndex(new BasicDBObject("mtime",1));
				scol.createIndex(new BasicDBObject("sourcecategory",1));
				scol.createIndex(new BasicDBObject("connectionIndex",1));
				scol.createIndex(Indexes.text("raw"));
				
				
				
				
				
				mcol.aggregate(Arrays.asList(
						
			              match(Filters.eq("sourcename", sname)),
			              new Document("$out",sname)
			              
			      )).forEach(new Block<SumoMessage>() {
			          @Override
			          public void apply(final SumoMessage msg) {
			              
			          }
			      } );
				
				
			}
			
		}finally {
			try{mcl.close();}catch(Exception exp) {}
		}
	}
}
