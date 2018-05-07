package sumo.analysis;

import static com.mongodb.client.model.Aggregates.match;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.util.Arrays;
import java.util.logging.Logger;

import com.sumologic.client.searchjob.SearchJobClient;
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

public class Test extends SumoMongo {
	private static final Logger LOGGER = Logger.getLogger(Test.class.getName());

	public static void main(String args[]) throws Exception {
//		SearchJobClient sjc=new SearchJobClient();
//		Test t = new Test();
//		t.init();
//		t.mdb.getCollection("tttt").drop();
//		t.querySourcenameList().forEach((v) -> {
//			try {
//				t.mdb.getCollection(v + "_ERR").drop();
//			} catch (Exception ex) {
//			}
//		});

		// MongoClient mcl=null;
		// // Create a CodecRegistry containing the PojoCodecProvider instance.
		// CodecRegistry pojoCodecRegistry =
		// fromRegistries(MongoClient.getDefaultCodecRegistry(),
		// fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		//
		// try {
		//// String fname="K:/SUMO/exports/2018-04-02.json.gz";
		// mcl = new
		// MongoClient("localhost",MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build());
		// MongoDatabase mdb=mcl.getDatabase("SUMO_DEV_510");
		//
		// MongoCollection<SumoMessage>
		// mcol=mdb.getCollection("source",SumoMessage.class);
		// MongoCursor<String> scsr=mcol.distinct("sourcename",String.class).iterator();
		// while(scsr.hasNext()) {
		// String sname=scsr.next();
		// if(sname.equals("source"))continue;
		// sname+="_ERR";
		// System.out.println(sname);
		// MongoCollection<SumoMessage> scol=null;
		// try {
		// mdb.createCollection(sname);
		// }catch(MongoCommandException exx) {
		// if(48!=exx.getCode()) {
		// throw exx;
		// }
		// }
		//
		// scol=mdb.getCollection(sname,SumoMessage.class);
		// scol.createIndex(new BasicDBObject("messageid",1),new
		// IndexOptions().unique(true));
		// scol.createIndex(new BasicDBObject("sourcename",1));
		// scol.createIndex(new BasicDBObject("fwVersion",1));
		// scol.createIndex(new BasicDBObject("mtime",1));
		// scol.createIndex(new BasicDBObject("sourcecategory",1));
		// scol.createIndex(Indexes.text("raw"));
		//
		//
		//
		// }
		//
		// }finally {
		// try{mcl.close();}catch(Exception exp) {}
		// }
	}
}
