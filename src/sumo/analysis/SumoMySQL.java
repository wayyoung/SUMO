package sumo.analysis;

import com.jcabi.jdbc.JdbcSession;
import com.jcabi.jdbc.Outcome;
import com.jolbox.bonecp.BoneCPDataSource;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.codejargon.fluentjdbc.api.FluentJdbc;
import org.codejargon.fluentjdbc.api.FluentJdbcBuilder;

import javax.sql.DataSource;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class SumoMySQL {

	public static final String DATABASE_DEV1_510 = "sumo_dev1_510";
	public static final String KEYWORD_PROXY_CONNECTED = "proxyConnected";
	public static final String KEYWORD_PROXY_DICCONNNECTED = "proxyDisConnnected";


	String dbname = DATABASE_DEV1_510;
	String username="root";
	String password="123456";
	String url = "jdbc:mysql://localhost:3306";


	protected BoneCPDataSource dataSource;
	JdbcSession jse;

	@Override
	public void finalize() {
		try {
			this.dataSource.close();
		} catch (Exception exp) {
		}
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


	public void init() {
		try {
			dataSource = new BoneCPDataSource();
			dataSource.setDriverClass("com.mysql.cj.jdbc.Driver");
			dataSource.setJdbcUrl(url + "/" + dbname+"?autoReconnect=true&useSSL=false");
			dataSource.setUser(username);
			dataSource.setPassword(password);
			jse=new JdbcSession(dataSource);

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

	public void createSourceame(String sname)  {
		try {
			String SQL_createDeviceSourrcename = "CREATE TABLE `" + dbname + "`.`" + sname + "` ("
					+ "`oid` BIGINT NOT NULL AUTO_INCREMENT,"
					+ "`messageid` BIGINT  NOT NULL,"
					+ "`sourcename` VARCHAR(128) NOT NULL,"
					+ "`sourcecategory` VARCHAR(128) NOT NULL,"
					+ "`source` VARCHAR(128) NOT NULL,"
					+ "`component` VARCHAR(128) NOT NULL,"
					+ "`fw_version` VARCHAR(128) NOT NULL,"
					+ "`sourcehost` VARCHAR(128) NULL,"
					+ "`receipttime` BIGINT NOT NULL,"
					+ "`messagetime` BIGINT NOT NULL,"
					+ "`mtime` DATETIME NOT NULL,"
					+ "`rtime` DATETIME NOT NULL,"
					+ "`raw` TEXT NULL,"
					+ "`connectionindex` INT NULL,"
					+ "`bootsync` INT NULL,"
					+ "PRIMARY KEY (`oid`),"
					+ " UNIQUE INDEX `messageid_UNIQUE` (`messageid` ASC) VISIBLE,"
					+ "INDEX `messageid_idx` (`messageid` ASC) INVISIBLE,"
					+ "INDEX `sourcename_idx` (`sourcename` ASC) INVISIBLE,"
					+ "INDEX `sourcecategory_idx` (`sourcecategory` ASC) INVISIBLE,"
					+ "INDEX `receipttime_idx` (`receipttime` ASC) INVISIBLE,"
					+ "INDEX `mtime_idx` (`mtime` ASC) INVISIBLE,"
					+ "INDEX `rtime_idx` (`mtime` ASC) INVISIBLE,"
					+ "INDEX `messagetime_idx` (`messagetime` ASC) INVISIBLE,"
					+ "INDEX `connectionindex_idx` (`connectionindex` ASC) INVISIBLE,"
					+ "INDEX `component_idx` (`component` ASC) INVISIBLE,"
					+ "INDEX `fw_version_idx` (`fw_version` ASC) INVISIBLE,"
					+ "FULLTEXT INDEX `raw_idx` (`raw`) VISIBLE);";
			System.out.println("Creating new table!");
			jse.sql(SQL_createDeviceSourrcename).update(new Outcome<Integer>() {
				@Override
				public Integer handle(ResultSet resultSet, Statement statement) throws SQLException {
					return statement.getUpdateCount();
				}
			});
			System.out.println("Done!");
		}catch(SQLException ex){
			//System.out.println(ex.getErrorCode());
		}


	}

	public TreeSet<String> querySourcenameList() {
		final TreeSet<String> names = new TreeSet<String>();
		try {
			jse.sql("show tables").select(
					new Outcome<TreeSet<String>>() {
						@Override
						public TreeSet<String> handle(ResultSet resultSet, Statement statement) throws SQLException {
							while(resultSet.next()) {
								String name = resultSet.getString(1);
								if ((!name.equals("source")) && (!name.endsWith("_ERR")) && (!name.startsWith("OV_"))) {

									names.add(name);
								}
							}
							return names;
						}


					}
			);
		}catch(Exception ex){
			ex.printStackTrace();
		}

		return names;
	}

	public void alterRawTable(){
		try {
			TreeSet<String> dl=this.querySourcenameList();
			for (String s : dl) {
				jse.sql("ALTER TABLE `"+s+"` MODIFY raw TEXT").update(new Outcome<Integer>() {
					@Override
					public Integer handle(ResultSet resultSet, Statement statement) throws SQLException {
						return null;
					}
				});
			}

		}catch(Exception ex){
			ex.printStackTrace();
		}

	}





	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, boolean ascending) {
		List<Map.Entry<K, V>> list = new ArrayList<>(map.entrySet());
		list.sort(Map.Entry.comparingByValue());

		Map<K, V> result = new LinkedHashMap<>();
		if (ascending) {
			for (Map.Entry<K, V> entry : list) {
				result.put(entry.getKey(), entry.getValue());
			}
		} else {
			int index = list.size();
			for (; index > 0; index--) {
				Map.Entry<K, V> entry = list.get(index - 1);
				result.put(entry.getKey(), entry.getValue());
			}
		}

		return result;
	}

	public TreeMap<String, Integer> queryProxyDisconnectedSortedMap(Date start, Date end) {
		TreeMap<String, Integer> tm = new TreeMap<String, Integer>();
//		for(String s: this.querySourcenameList()){
//			tm.put(s,0);
//			this.getColletionBySourcename(s).aggregate(Arrays.asList(
//					match(and(exists("connectionIndex"),gte("mtime",start),lt("mtime",end))),
//					project(fields(include("connectionIndex"))),
//					group("$connectionIndex"),
//					group(1,sum("count",1))
//			)).forEach(new Block<Document>() {
//				@Override
//				public void apply(final Document document) {
//					tm.put(s,document.getInteger("count"));
//				}
//			});

//		}

		this.sortByValue(tm, false).forEach((k, v) -> {
			System.out.println(k + ":" + v);
		});

		return tm;
	}

	public static void main(String[] args) throws Exception {
		SumoMySQL sm = new SumoMySQL();
		sm.init();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sm.alterRawTable();
//		sm.queryProxyDisconnectedSortedMap(sdf.parse("2018-05-03 00:00:00"), sdf.parse("2018-05-08 00:00:00"));
//		sm.createSourceame("TTTT");
//		sm.querySourcenameList();
//		sm.querySourcenameList().forEach((v)->{System.out.println(v);});
	}


}
