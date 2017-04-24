import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.MqttPersistenceException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SensorMessageSender {

	static private int NUMBER_OF_MESSAGES = 10;
	
	private MqttClient client;
	private String topic;
	private int qos = 0;
	private String mqttAdress;
	private String clientId;
	

	public SensorMessageSender(String mqttAddress, String clientId, String topic) { 
		this.mqttAdress=mqttAddress;
		this.clientId=clientId;
		this.topic=topic;

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
	
	void burstMessage() throws MqttPersistenceException, MqttException{
		for(int i = 0; i < NUMBER_OF_MESSAGES; i++){
			Date dNow = new Date( );
		    SimpleDateFormat ft = new SimpleDateFormat ("hh:mm:ss:SSS");
			String messageToSend= "{\"datapassagem\" : \"2017-04-23\", \"horapassagem\" : \""+ft.format(dNow).toString()+"\", \"evento\" : \"Festa ISCTE\" }";
			MqttMessage message = new MqttMessage(messageToSend.getBytes());
			message.setQos(qos);
	    	message.setRetained(false);
			client.publish(topic, message);
		}
	}
	
	public boolean connected(){
		return client.isConnected();
	}
	
	public static void main(String[] args) throws MqttException, InterruptedException, BrokenBarrierException {
		final CyclicBarrier gate = new CyclicBarrier(5);
		for(int i=0;i < 10; i++){
			ThreadTester t = new ThreadTester();
			t.setId(i+100);
			t.start();
			System.out.println(i);
		}
		gate.await();


	}
}

