import java.sql.SQLException;
import java.util.Scanner;

import javax.swing.plaf.synth.SynthSpinnerUI;

import org.eclipse.paho.client.mqttv3.MqttException;

public class SensorTester {

	
	public static void main(String[] args) throws MqttException, InterruptedException, SQLException {
		ExportToSybase exporter = new ExportToSybase();
		SafeThreadList waitingData = new SafeThreadList();
		AppSensor senIN  = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientIN_69178", "iscte_sid_2016_S4", "IN", exporter,waitingData);
		AppSensor senOUT = new AppSensor("tcp://iot.eclipse.org:1883", "eclipseClientOUT_69178", "iscte_sid_2016_S5", "OUT", exporter,waitingData);	
		senOUT.start();
		senIN.start();
		new MondoDBVerifier(waitingData,exporter).start();
		
		boolean stop = false;
		
		while(!stop){
			 Scanner sc = new Scanner(System.in);
			 System.out.println("Press any key to exit.\n");
		     if(sc.hasNext()){
		    	 int size = 0;
		    	 size = waitingData.getSize();
		    	 System.out.println(size);
		    	 System.out.println(waitingData.getData().toString());
		     }
		}
		
		senOUT.closeConnections();
		senIN.closeConnections();
		exporter.closeConnections();
		
	}	
	
	//{"datapassagem" : "2013-10-15", "horapassagem" : "10:12", "evento" : "Festa ISCTE" }

}
