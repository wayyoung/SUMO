package sumo.analysis;


import static com.mongodb.client.model.Aggregates.match;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.text;
import static com.mongodb.client.model.Sorts.*;
import static com.mongodb.client.model.Sorts.orderBy;
import static com.mongodb.client.model.Updates.combine;
import static com.mongodb.client.model.Updates.set;
import static com.mongodb.client.model.Updates.unset;

import java.util.Arrays;
import java.util.logging.Logger;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.Block;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;

public class FilterToMarkConnectionIndex extends SumoMongo{
	private static final Logger LOGGER = Logger.getLogger( FilterToMarkConnectionIndex.class.getName() );



	int connectionIndex=1;
	boolean connected=false;
	boolean firstConnectedFound=false;

	Document lastConnectedMsg=null;
	
	
	
	public void filterOnSourceName(String sname) {
		LOGGER.info("Start: "+sname);
		MongoCollection<Document> mcol=mdb.getCollection(sname, Document.class);
		AggregateIterable<Document> mitr=mcol.aggregate(Arrays.asList(
				match(text("proxyDisconnnected proxyConnected")),
				sort(orderBy(ascending("mtime"),descending("messageid")))
			)).allowDiskUse(true);
		System.out.println("Aggregate Done!!");

		connectionIndex=1;
		connected=false;
		firstConnectedFound=false;
		lastConnectedMsg=null;
		
		mitr.forEach(new Block<Document>() {
			          @Override
			          public void apply(final Document msg) {
			        	  if(msg.getString("raw").contains(KEYWORD_PROXY_CONNECTED)) {
//			        		  System.out.println(msg.getString("raw"));

		        			  connected=true;
		        			  firstConnectedFound=true;
		        			  lastConnectedMsg=msg;
		        			  

			        	  }else {
			        		  if(!firstConnectedFound) {
			        			  System.out.println("!!!! " +msg.getString("raw"));
			        			  return;
			        		  }
			        		  if(connected) {
			        			  ObjectId startId=lastConnectedMsg.getObjectId("_id");
			        			                   
			        			  connected=false;
			        			  lastConnectedMsg=null;
			        			  mcol.updateOne(eq(("_id"), startId),combine(set("connectionIndex",connectionIndex),unset("err_proxyDuplicated")));
			        			  mcol.updateOne(eq(("_id"), msg.getObjectId("_id")),combine(set("connectionIndex",connectionIndex),unset("err_proxyDuplicated")));
			        			  connectionIndex++;
			        		  }
			        		  
			        		  
			        	  }
//			        		  connected=true;
//			        	  else connected=false;
////			              System.out.println(msg.getRaw());
//			        	  if similar to previous 
//			        	     if(gap <1s)
//			        	     mark as proxy_dulicated
//			        	     else
//			        	    	 warning
//			        	    	 move to error collection
//			        	  else
//			        		  change previous
//			          }
			      }
		});
		LOGGER.info("Finish: "+sname);
	}
	

	public static void main(String args[])throws Exception{
		
		
		try {
				FilterToMarkConnectionIndex frp=new FilterToMarkConnectionIndex();
				frp.init();
				frp.querySourcenameList().forEach((v)->{
					
					frp.filterOnSourceName(v);
				});
		}finally {

		}
	}
}
