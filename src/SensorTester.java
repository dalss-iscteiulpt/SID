import java.sql.SQLException;
import java.util.Scanner;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.eclipse.paho.client.mqttv3.MqttException;

public class SensorTester {

	
	public static void main(String[] args) throws MqttException, InterruptedException, SQLException {
		MongoToSyabaseExporter exporter = new MongoToSyabaseExporter();
		AppSensor senIN  = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientIN_69178", "iscte_sid_2016_S1", "IN", exporter);
		AppSensor senOUT = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientOUT_69178", "iscte_sid_2016_S2", "OUT", exporter);	
		senOUT.start();
		senIN.start();
		
		boolean stop = false;
		
		while(!stop){
			 Scanner sc = new Scanner(System.in);
			 System.out.println("Press any key to exit.\n");
		     if(sc.hasNext()){
		    	 stop = true;
		     }
		}
		
		senOUT.closeConnections();
		senIN.closeConnections();
		exporter.closeConnections();
		
	}	
	
	//{"datapassagem" : "2013-10-15", "horapassagem" : "10:12", "evento" : "Festa ISCTE" }

}
