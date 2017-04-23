import java.sql.SQLException;
import java.util.Vector;

import javax.swing.plaf.synth.SynthSeparatorUI;

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
	 * To connect to/manage MQTT server
	 */
	
	//object that provides the services
	private MqttClient client;
	//topic to subscribe
	private String topic;
	//quality of the service (0, not reliable; 1, retries once; 2, keeps trying until)
	private int qos = 0;
	//server's address
	private String mqttAdress;
	//id to identify the client that is connecting
	private String clientId;
	
    
	/**
	 * To connect to/manage MongoDB
	 */
	private MongoClient mongoClient;
	private MongoDatabase database;
	private MongoCollection<Document> collection;
	
	/**
	 * AppSensor configurations 
	 */
	private int MAX_RECONNECT_ATTEMPTS = 5;
	
	private String sensorName;
	private boolean connectedToMQTT;
	private boolean connectedToMongoDB;
	
	/**
	 * Provides the services and performs the migration between mongo and sybase
	 */
	
	private MongoToSyabaseExporter exportEngine;
	
	/**
	 * 
	 * @param mqttAdress 			Ligação tcp que liga ao paho
	 * @param clientId			Id do cliente que se vai ligar, tem que ser sempre diferente
	 * @param topic				Topico a que o cliente se vai subscrever
	 * @param nomeSensor		Nome do sensor IN/OUT
	 * @param exportEngine 		Exportador mongoDB -> Sybase
	 *
	 */
	public AppSensor(String mqttAddress, String clientId, String topic, String nomeSensor, MongoToSyabaseExporter exportEngine) { 
		this.mqttAdress=mqttAddress;
		this.clientId=clientId;
		this.topic=topic;
		this.sensorName=nomeSensor;
		this.exportEngine=exportEngine;
	}
	
	
	
	/**
	 *  Connects to MongoDB and MQTT.  
	 */
	public void start() {
		//Number of connection attempts
		int connectRetries = 0;
		
		connectToMongo();
		
		while (connectRetries < MAX_RECONNECT_ATTEMPTS){
			try {
				connectToMQTT();
				break;
			} catch (MqttException e1) {
				System.out.println("Problem with the connection. Trying to reconnect.");
				connectRetries++;
			}	
		}
		System.out.println("Connected correctly. MQTT clientID: "+clientId);		
	}
	
	/**
	 * Sets the connection to mongoDB
	 */
	private void connectToMongo(){
		mongoClient = new MongoClient("localhost",27017);
		database = mongoClient.getDatabase("SensorLog");
		collection = database.getCollection("SensorLogColl");
	}
	
	/**
	 * Sets the connection to MQTT
	 * @throws MqttException
	 */
	private void connectToMQTT() throws MqttException{
		client = new MqttClient(mqttAdress, clientId, new MemoryPersistence());
		client.connect();
		client.setCallback(this);
		client.subscribe(topic);
	}

	/**
	 * Mandatory by the MqttCallback Interface. Not used.   
	 */
	@Override
	public void connectionLost(Throwable cause) { 
		System.out.println(cause);
	}
	
	/**
	 * Method called each time a message arrives. The message is then inserted into MongoDB. 
	 * Note: The client needs to have subscribed to a topic first.
	 */
	@Override
	public void messageArrived(String topic, MqttMessage message) throws Exception {
		try{
	        insertIntoMongoDB(message);
		}catch(Exception e){
	         System.err.println( e.getClass().getName() + ": " + e.getMessage() );
	      }
	}
	/**
	 *Changes the format of the message to JSON and adds the type of the sensor to the end of the message.
	 *The message is added to MongoDB.
	 *Note: The type of the sensor is related to the topic subscribed on the MQTT server. Each of the sensors subscribes a different topic.
	 */
	public void insertIntoMongoDB(MqttMessage message){
		String messageString = message.toString();
        messageString = messageString.replaceAll("\"","'");
        messageString = messageString.substring(0, messageString.length() - 1);
        if(sensorName.equals("IN")){
        	messageString += (", 'sensor' : 'IN' }");
        } else if(sensorName.equals("OUT")){
        	messageString += (", 'sensor' : 'OUT' }");
        }
        
        	Document dbObject = Document.parse(messageString);
            collection.insertOne(dbObject);
            exportEngine.executeExport();
	}
	
	
	/**
	 * Mandatory by the MqttCallback Interface. Not used.   
	 */
	@Override
	public void deliveryComplete(IMqttDeliveryToken token) {
	}
	
	public void closeConnections() throws MqttException{
		client.disconnect();
		client.close();
		mongoClient.close();
	}
	
//{"datapassagem" : "2013-10-15", "horapassagem" : "10:12", "evento" : "Festa ISCTE" }

}
