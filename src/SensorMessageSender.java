import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SensorMessageSender {

	static private int NUMBER_OF_MESSAGES = 20;
	
	private MqttClient client;
	private String topic;
	private int qos = 0;
	private String mqttAdress;
	private String clientId;

	private String messageToSend;
	

	public SensorMessageSender(String mqttAddress, String clientId, String topic, String messageToSend) { 
		this.mqttAdress=mqttAddress;
		this.clientId=clientId;
		this.topic=topic;
		this.messageToSend = messageToSend;
		connectToMQTT();
	}
	
	private void connectToMQTT(){
		try {
			client = new MqttClient(mqttAdress, clientId, new MemoryPersistence());
			client.connect();
			client.subscribe(topic);		
		} catch (MqttException e) {
			e.printStackTrace();
		}	
	}
	
	private void burstMessage() throws MqttPersistenceException, MqttException{
		System.out.println(messageToSend);
		for(int i = 0; i < NUMBER_OF_MESSAGES; i++){
			MqttMessage message = new MqttMessage(messageToSend.getBytes());
			message.setQos(qos);
	    	message.setRetained(false);
			client.publish(topic, message);
		}
	}
	
	public boolean connected(){
		return client.isConnected();
	}
	
	public static void main(String[] args) throws MqttException, InterruptedException {
		
		SensorMessageSender burstIN = new SensorMessageSender("tcp://iot.eclipse.org:1883", "eclipseClientINBurst_69178", "iscte_sid_2016_S1", " \"{\"datapassagem\" : \"2017-10-18\", \"horapassagem\" : \"20:12\", \"evento\" : \"Festa ISCTE\" }");
		SensorMessageSender burstOUT = new SensorMessageSender("tcp://iot.eclipse.org:1883", "eclipseClientINBurst_69178", "iscte_sid_2016_S2", " \"{\"datapassagem\" : \"2016-10-18\", \"horapassagem\" : \"20:12\", \"evento\" : \"Festa ISCTE\" }");
		
		System.out.println(burstIN.connected());
		
		burstIN.burstMessage();
		Thread.sleep(10000);
		burstOUT.burstMessage();

	}
}

