import java.text.SimpleDateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttException;

public class ThreadTester extends Thread{
	int id;
	
	public void setId(int id){
		this.id=id;
	}
	
	public void run(){
		
		SensorMessageSender burstIN = new SensorMessageSender("tcp://iot.eclipse.org:1883", "eclipseClientINBurst_v_"+id+"", "iscte_sid_2016_S4");
		SensorMessageSender burstOUT = new SensorMessageSender("tcp://iot.eclipse.org:1883", "eclipseClientINBurst_vout_"+id+"", "iscte_sid_2016_S5");
		
		System.out.println(burstIN.connected());
		
		Random rand = new Random();
		int sl = rand.nextInt((2000 - 1000) + 1) + 1000;
		try {
			Thread.sleep(sl);
		} catch (InterruptedException e1) {
			e1.printStackTrace();
		}
		System.out.println("Go: "+sl);
		try {
			burstIN.burstMessage();
			burstOUT.burstMessage();
		} catch (MqttException e) {
			e.printStackTrace();
		}
		id++;
	}

}
