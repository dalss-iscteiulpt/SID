
public class MondoDBVerifier extends Thread{
	private SafeThreadList waitingData;
	private ExportToSybase exportEngine;
	
	public MondoDBVerifier(SafeThreadList waitingData, ExportToSybase exportEngine){
		this.waitingData = waitingData;
		this.exportEngine = exportEngine;
	}
	
	public void run() {
		while(true){
			System.out.println("Go");
			waitingData.sendData();
			exportEngine.executeExport();
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
        
    }

}
