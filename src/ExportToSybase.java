import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class ExportToSybase implements Runnable{

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
	private int MAX_RECONNECT_ATTEMPTS = 20;

	// Number of maximum pull attempts
	private int MAX_PULL_ATTEMPTS = 2;

	// Thread that will perform each export request
	private ExecutorService dispatcher;


	/**
	 * Starts the exporter
	 * @throws InterruptedException 
	 */
	public ExportToSybase() {
	}
	
	public void start() throws InterruptedException{
		connectToMongo();
		connectToSybase();
		dispatcher = Executors.newSingleThreadExecutor();
		executeExport();
	}


	public void connectToMongo(){
		mongoClient = new MongoClient("localhost",27017);
		database = mongoClient.getDatabase("SensorLog");
		collection = database.getCollection("SensorLogColl");
	}

	/**
	 * Tries to connect to sybase. Repeats n times if connection fails.
	 * @throws InterruptedException 
	 */
	public void connectToSybase() throws InterruptedException{

		int connectRetries = 0;

		while(connectRetries < MAX_RECONNECT_ATTEMPTS){
			try {
				sybaseConn = DriverManager.getConnection("jdbc:sqlanywhere:uid="+SYBASE_USER+";pwd="+SYBASE_PASSWORD+";eng="+SYBASE_DATABASE);
				System.out.println("Connected to Sybase");
				break;
			} catch (SQLException e) {
				System.out.println("Couldn't connect to Sybase. Reconnecting.");
				Thread.sleep(5000);
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

	public void processSensorInformation(Document item) throws SQLException {

		Sensor sensor = new Sensor(item.getString("datapassagem"), item.getString("horapassagem"), item.getString("evento"),item.getString("sensor"));

		Statement sybaseStatement = sybaseConn.createStatement();
		String sqlCommand = sensor.queryToTableSybase();
		try{
			if(sqlCommand != ""){
				Integer result = new Integer(sybaseStatement.executeUpdate(sqlCommand));
				System.out.println("Sent");
			}
		}catch(NullPointerException e1){
			System.out.println("Wrong format.");
		}


		//Delete item from mongoDB
		collection.deleteOne(new Document("_id", new ObjectId(item.get("_id").toString())));
	}


	/**
	 * Task to be performed
	 */
	@Override
	public void run() {
		FindIterable<Document> search = collection.find();

		int nrRetries=0;
		while(nrRetries  < MAX_PULL_ATTEMPTS){	
			try{
				for (Document item : search) {
					processSensorInformation(item);	
				} 
				break;
			}catch (SQLException sqlE) {
					try {
						System.out.println("Sybase Error. Trying to reconnect");
						connectToSybase();
						nrRetries++;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

			}
	}

	/**
	 * Schedules a task to perform
	 */
	public void executeExport(){
		dispatcher.submit(this);
	}

}
