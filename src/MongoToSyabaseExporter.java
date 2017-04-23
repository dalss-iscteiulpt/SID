import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.swing.plaf.synth.SynthSeparatorUI;
import javax.xml.ws.FaultAction;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


public class MongoToSyabaseExporter implements Runnable{

	//Sybase stuff
	private Connection sybaseConn;

	private String SYBASE_DATABASE="EventosDB";
	private String SYBASE_USER = "dba";
	private String SYBASE_PASSWORD = "sql";

	//MongoDB stuff
	private MongoClient mongoClient;
	private MongoDatabase database;
	private MongoCollection<Document> collection;


	// Number of maximum reconnects
	private int MAX_RECONNECT_ATTEMPTS = 5;

	// Thread that will perform each export request
	private ExecutorService dispatcher;

	/**
	 * Starts the exporter
	 */
	public MongoToSyabaseExporter(){
		connectToMongo();
		connectToSybase();
		dispatcher = Executors.newSingleThreadExecutor();
	}



	public void connectToMongo(){
		mongoClient = new MongoClient("localhost",27017);
		database = mongoClient.getDatabase("SensorLog");
		collection = database.getCollection("SensorLogColl");
	}

	/**
	 * Tries to connect to sybase. Repeats 5  times if connection fails.
	 */
	public void connectToSybase(){

		int connectRetries = 0;

		while(connectRetries < MAX_RECONNECT_ATTEMPTS){
			try {
				sybaseConn = DriverManager.getConnection("jdbc:sqlanywhere:uid="+SYBASE_USER+";pwd="+SYBASE_PASSWORD+";eng="+SYBASE_DATABASE);
				System.out.println("Connected to Sybase");
				break;
			} catch (SQLException e) {
				System.out.println("Couldn't connect to Sybase. Reconnecting.");
				connectRetries++;
			}
		}
	}

	/**
	 * Shuts down 
	 * @throws SQLException
	 */
	public void closeConnections() throws SQLException{
		dispatcher.shutdown();
		sybaseConn.close();
		mongoClient.close();

		System.out.println("Conexões fechadas");
	}




	public void processSensorInformation(Document item) {

		Sensor sensor = new Sensor(item.getString("datapassagem"), item.getString("horapassagem"), item.getString("evento"),item.getString("sensor"));

		//Repeats if the connection was not possible
		boolean retry = false;

		do{
			try{
				Statement sybaseStatement = sybaseConn.createStatement();
				String sqlCommand = sensor.queryToTableSybase();
				if(sqlCommand != ""){
					try {
						Integer result = new Integer(sybaseStatement.executeUpdate(sqlCommand));
					} catch (SQLException e) {
						System.out.println("Error running sql sentence.");
					}
				}
			}catch(SQLException e2){
				connectToSybase();
				retry = true;
			}
			retry = true;
		} while (!retry);

		//Delete item from mongoDB
		collection.deleteOne(new Document("_id", new ObjectId(item.get("_id").toString())));
	}


	/**
	 * Task to be performed
	 */
	@Override
	public void run() {
		FindIterable<Document> search = collection.find();
		MongoCursor<Document> cursor = search.iterator();

		System.out.println("Aqui!");

		for (Document item : search) {
			processSensorInformation(item);	
		}
	}

	/**
	 * Schedules a task to perform
	 */
	public void executeExport(){
		dispatcher.submit(this);
	}

}
