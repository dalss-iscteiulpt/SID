import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import javax.xml.ws.FaultAction;

import org.bson.Document;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;


public class ExportaMongoToSybase {
	private Statement sybaseStatement;
	private Connection sybaseConn;
	private MongoClient mongoClient;
	private MongoDatabase database;
	private MongoCollection<Document> collection;
	private boolean fatalError=false;
	
	/** 
	 * @throws SQLException 
	 * Inicializa as conexões ao sybase e ao mongoDB.
	 */
	public ExportaMongoToSybase() throws SQLException{
		sybaseConn = DriverManager.getConnection("jdbc:sqlanywhere:uid=dba;pwd=sql;eng=EventosDB");
		mongoClient = new MongoClient("localhost",27017);
		database = mongoClient.getDatabase("SensorLog");
		collection = database.getCollection("SensorLogColl");
		System.out.println("Exportador Inicializado");
	}
	
	/**
	 * @param nome
	 * @throws InterruptedException 
	 * @throws SQLException
	 * A partir da ligação ao mongoDB iniciada no construtor irá buscar todos as entrada e 
	 * e irá colocar cada entrada num detector de erros que depois irá encaminha para um procedimento 
	 * que colocará a entrada no sybase
	 */
	public void start() throws InterruptedException{
		while(true){
			if(fatalError){
				System.out.println("Fatal Error. Closing.");
				return;
			}
			FindIterable<Document> search = collection.find();
			MongoCursor<Document> cursor = search.iterator();
			if(cursor.hasNext()){
				for (Document current : search) {
					errorDetector(current);
				}
			} else {
				waitSensor();
			}
		}
	}
	
	public synchronized void waitSensor() throws InterruptedException{
		wait();
		System.out.println("Boop");
	}
	
	/**
	 * Chamado no AppSensor quando é recebida uma mensagem.
	 */
	public synchronized void notifyExport(){
		notifyAll();
	}
	

	
	/**
	 * @param entry
	 * 
	 * Caso o sybase lançe uma excepeção o programa vai tentar reconectar durante 20 segundos e depois, caso 
	 * não consiga desliga.Em caso de erro nenhum dado se perderá porque estes estão guardados na mongoDB.
	 * 
	 */
	
	public void errorDetector(Document entry){
		try {

				createSensorObjectToSybase(entry);
			
		} catch (SQLException excp){
			int retry = 100;
			while(retry != 0){
				try{
				System.out.println("Error: Attempting connection to Sybase ("+retry+")");
				sybaseConn = DriverManager.getConnection("jdbc:sqlanywhere:uid=dba;pwd=sql;eng=EventosDB");
				return;
				} catch (SQLException badConn){
					try {
						Thread.sleep(5000);
						retry--;
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			fatalError=true;
			return;
		}
	}
	/**
	 * @param entrada
	 * @param nome
	 * @throws SQLException
	 * Baseado no documento que receber da exportacao do mongoDB irá criar um novo 
	 * objecto Sensor. Esse objecto sensor irá enviar um string dependendo do tipo de sensor
	 * que com a classe Statement irá criar um query capaz de colocar os dados no sybase.
	 * Apos a entrada ter sido colocada no sybase a entrada será apagada da base de dados
	 * mongo de forma a confirmar o envio.
	 */
	public void createSensorObjectToSybase(Document entrada) throws SQLException{
		try {
			Sensor sensor_tmp = new Sensor(entrada.getString("datapassagem"), entrada.getString("horapassagem"), entrada.getString("evento"),entrada.getString("sensor"));
			sybaseStatement = sybaseConn.createStatement();
			String sqlCommand = sensor_tmp.queryToTableSybase();
			Integer result = new Integer(sybaseStatement.executeUpdate(sqlCommand));
			System.out.println("Data Sent");
		} catch (NullPointerException nullExp){
			System.out.println("Wrong Format");
		}
		collection.deleteOne(new Document("_id", new ObjectId(entrada.get("_id").toString())));


	}
	
	
	/**
	 * @throws SQLException
	 * Desliga as conexoes ao sybase e mongoDB
	 */
	public void closeConnections() throws SQLException{
		sybaseConn.close();
		mongoClient.close();
		System.out.println("Conexões fechadas");
	}
}
