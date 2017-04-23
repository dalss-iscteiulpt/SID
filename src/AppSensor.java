import java.sql.SQLException;
import java.util.Vector;

import org.bson.Document;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


public class AppSensor implements MqttCallback{
	
	/**
	 * 
	 */
	private MemoryPersistence persistence = new MemoryPersistence();
	/**
	 * Cliente do broker Mosquitto e atributos necessários para conectar ao servidor.
	 */
	private MqttClient client; 
	private String topic;       
	private int qos = 0;
	private String broker;      
	private String clientId;    
	/**
	 * Cliente da driver java do mongoDB para se ligar a BD.
	 */
	private MongoClient mongoClient;
	MongoDatabase database;
	MongoCollection<Document> collection;
	/**
	 * Nome do sensor IN/OUT
	 */
	private String nomeSensor;
	
	private ExportaMongoToSybase exportEngine;
	
	/**
	 * 
	 * @param broker 			Ligação tcp que liga ao paho
	 * @param clientId			Id do cliente que se vai ligar, tem que ser sempre diferente
	 * @param topic				Topico a que o cliente se vai subscrever
	 * @param nomeSensor		Nome do sensor IN/OUT
	 * @param exportEngine 		Exportador mongoDB -> Sybase
	 *
	 */
	public AppSensor(String broker, String clientId, String topic, String nomeSensor, ExportaMongoToSybase exportEngine) { 
		this.broker=broker;
		this.clientId=clientId;
		this.topic=topic;
		this.nomeSensor=nomeSensor;
		this.exportEngine=exportEngine;
		
	}
	
	/**
	 *  Inicia as conexões do broker Mosquitto e a ligação ao mongoDB.
	 * @throws MqttException 
	 */
	public void start() throws MqttException{
		mongoClient = new MongoClient("localhost",27017);
		database = mongoClient.getDatabase("SensorLog");
		collection = database.getCollection("SensorLogColl");
		
		client = new MqttClient(broker, clientId, persistence);
		client.connect();
		client.setCallback(this);
		client.subscribe(topic);
		
		System.out.println("Conexões iniciadas correctamente ClientID: "+clientId);
		
	}

	@Override
	public void connectionLost(Throwable cause) { 
		System.out.println(cause);
	}
	
	/**
	 * Quando uma mensagem chega do Mosquitto Broker, este irá buscar a base de dados mongoDB
	 * necessária e a colecção e depois através do procedimento insertMessageToMongoDB(message,collection)
	 * irá colocar a mensagem na colecção do mongoDB.
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		try{
	        insertMessageToMongoDB(message);
		}catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
	/**
	 *Aqui o procedimento recebe uma mensagem Mqtt e a colecção da BD mongoDB onde irá colocar 
	 *essa mensagem. O procedimento passa a mensagem para um String, e nesse String irá colocar todos 
	 *os " para ' de forma a criar o formato JSON para ser feito o parsing pela driver java do
	 *MongoDB. Após ser feito o parsing a mensagem é colocada na colecção do mongoDB.
	 */
	public void insertMessageToMongoDB(MqttMessage message){
		String messageString = message.toString();
        messageString = messageString.replaceAll("\"","'");
        messageString = messageString.substring(0, messageString.length() - 1);
        if(nomeSensor.equals("IN")){
        	messageString += (", 'sensor' : 'IN' }");
        } else if(nomeSensor.equals("OUT")){
        	messageString += (", 'sensor' : 'OUT' }");
        }
        Document dbObject = Document.parse(messageString);
        collection.insertOne(dbObject);
        notifyEngine();
	}
	
	public void notifyEngine(){
		exportEngine.notifyExport();
	}
	
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
	}
	

	public static void main(String[] args) throws MqttException, InterruptedException, SQLException {
		ExportaMongoToSybase exporter = new ExportaMongoToSybase();
		AppSensor senIN  = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientIN_69178", "iscte_sid_2016_S1_6", "IN", exporter);
		AppSensor senOUT = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientOUT_69178", "iscte_sid_2016_S2_6", "OUT", exporter);	
		senOUT.start();
		senIN.start();
		exporter.start();

		
	}
//{"datapassagem" : "2013-10-15", "horapassagem" : "10:12", "evento" : "Festa ISCTE" }

}
