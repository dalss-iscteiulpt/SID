import java.sql.SQLException;
import java.util.HashMap;
import java.util.Scanner;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.eclipse.paho.client.mqttv3.MqttException;

public class SensorTester {

	
	public static void main(String[] args) throws MqttException, InterruptedException, SQLException {
		HashMap<String, String> topics = new HashMap<>();
		topics.put("iscte_sid_2016_S4", "IN");
		topics.put("iscte_sid_2016_S5", "OUT");
		ExportToSybase exporter = new ExportToSybase();
		AppSensor sensor  = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientIN_69178", exporter, topics);	
		sensor.start();
		
		boolean stop = false;
		
		while(!stop){
			 Scanner sc = new Scanner(System.in);
			 System.out.println("Press any key to exit.\n");
		     if(sc.hasNext()){
		    	 stop = true;
		     }
		}
		
		sensor.closeConnections();
		exporter.closeConnections();
		
	}	
	
	//{"datapassagem" : "2013-10-15", "horapassagem" : "10:12", "evento" : "Festa ISCTE" }

}
