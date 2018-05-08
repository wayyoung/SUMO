package sumo.analysis;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.project;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

import java.text.SimpleDateFormat;
import java.util.*;

import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import sumo.analysis.servlet.QueryProxyConnection;

public class SumoMongo {
	
	public static final String DATABASE_DEV1_510="SUMO_DEV1_510";
	public static final String KEYWORD_PROXY_CONNECTED="proxyConnected";
	public static final String KEYWORD_PROXY_DICCONNNECTED="proxyDisConnnected";
	
	
	String dbname=DATABASE_DEV1_510;
	String username;
	String password;
	String url="localhost";
	
	protected MongoClient mclient=null;
	protected MongoDatabase mdb=null;

	Block<Document> printBlock = new Block<Document>() {
		@Override
		public void apply(final Document document) {
			System.out.println(document.toJson());
		}
	};
	
	public void init() {
		// Create a CodecRegistry containing the PojoCodecProvider instance.
		CodecRegistry pojoCodecRegistry = fromRegistries(MongoClient.getDefaultCodecRegistry(),
				fromProviders(PojoCodecProvider.builder().automatic(true).build()));
		mclient = new MongoClient(url,MongoClientOptions.builder().codecRegistry(pojoCodecRegistry).build());
		mdb=mclient.getDatabase(dbname);
		

	}
	
	public MongoCollection<Document> createSourceameCollection(String sname)throws MongoCommandException {

		try {
			mdb.createCollection(sname);
		}catch(MongoCommandException exx) {
			if(48!=exx.getCode()) {
				throw exx;
			}
		}
		
		MongoCollection<Document> scol=mdb.getCollection(sname,Document.class);
		scol.createIndex(new BasicDBObject("messageid",1),new IndexOptions().unique(true));
		scol.createIndex(new BasicDBObject("sourcename",1));
		scol.createIndex(new BasicDBObject("fwVersion",1));
		scol.createIndex(new BasicDBObject("mtime",1));
		scol.createIndex(new BasicDBObject("sourcecategory",1));
		scol.createIndex(new BasicDBObject("connectionIndex",1));
		scol.createIndex(Indexes.text("raw"));
		return scol;
	}
	
	public TreeSet<String> querySourcenameList() {
		TreeSet<String> names=new TreeSet<String>();
		mdb.listCollectionNames().forEach(new Block<String>() {
			          @Override
			          public void apply(final String name) {
			        	  if((!name.equals("source") )&& (!name.endsWith("_ERR")) && (!name.startsWith("OV_"))) {
			        		  names.add(name);
			        	  }
			          }
			      });
		return names;
	}

	public MongoCollection<Document> getColletionBySourcename(String sname){
		return this.mdb.getCollection(sname,Document.class);
	}
	
	@Override
	public void finalize() {
		try{mclient.close();}catch(Exception exp) {}
	}
	
	public String getDbname() {
		return dbname;
	}

	public void setDbname(String dbname) {
		this.dbname = dbname;
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public static void main(String[] args)throws Exception{
		SumoMongo sm=new SumoMongo();
		sm.init();
		SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sm.queryProxyDisconnectedSortedMap(sdf.parse("2018-05-03 00:00:00"),sdf.parse("2018-05-08 00:00:00"));
//		sm.querySourcenameList().forEach((v)->{System.out.println(v);});
	}


	public  static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean ascending) {
		List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
		list.sort(Map.Entry.comparingByValue());

		Map<K, V> result = new LinkedHashMap<>();
		if(ascending){
			for (Map.Entry<K, V> entry : list) {
				result.put(entry.getKey(), entry.getValue());
			}
		}else{
			int index=list.size();
			for(;index>0;index--){
				Map.Entry<K, V> entry=list.get(index-1);
				result.put(entry.getKey(), entry.getValue());
			}
		}

		return result;
	}

	public TreeMap<String,Integer> queryProxyDisconnectedSortedMap(Date start, Date end){
		TreeMap<String,Integer> tm=new TreeMap<String,Integer>();
		for(String s: this.querySourcenameList()){
			tm.put(s,0);
			this.getColletionBySourcename(s).aggregate(Arrays.asList(
					match(and(exists("connectionIndex"),gte("mtime",start),lt("mtime",end))),
					project(fields(include("connectionIndex"))),
					group("$connectionIndex"),
					group(1,sum("count",1))
			)).forEach(new Block<Document>() {
				@Override
				public void apply(final Document document) {
					tm.put(s,document.getInteger("count"));
				}
			});


	//                    sort(orderBy(ascending("mtime"),descending("messageid"))),
	//                    project(fields(include("mtime"),include("connectionIndex")))
		}

		this.sortByValue(tm,false).forEach((k,v)->{System.out.println(k+":"+v);});

		return tm;
	}


}
