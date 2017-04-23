public class Sensor {
	private int idpassagem;
	private String datapassagem = new String();
	private String horapassagem = new String();
	private String evento = new String();
	public String sensorInOut = new String();
	
	public Sensor(String datapassagem, String horapassagem, String evento, String sensorInOut) {
		this.evento=evento;
		this.datapassagem=datapassagem;
		this.horapassagem=horapassagem;
		this.sensorInOut=sensorInOut;
	};
	
	public int getIdpassagem() {
		return idpassagem;
	}

	public String getDatapassagem() {
		return datapassagem;
	}

	public String getHorapassagem() {
		return horapassagem;
	}

	public String getEvento() {
		return evento;
	}
	
	public String queryToTableSybase(){
		String query = new String("");
		if(sensorInOut != null && sensorInOut.equals("IN")){
			query = "insert into RegistoPassagem (Eve_numeroEvento,hora,sensor) "
				+ "VALUES ((select numeroEvento from Evento where designacaoEvento = '"+evento+"'),"
				+ "'"+datapassagem+"T"+horapassagem+"',"
				+ "'I')";
		} if(sensorInOut != null && sensorInOut.equals("OUT")) {
			query = "insert into RegistoPassagem (Eve_numeroEvento,hora,sensor) "
					+ "VALUES ((select numeroEvento from Evento where designacaoEvento = '"+evento+"'),"
					+ "'"+datapassagem+"T"+horapassagem+"',"
					+ "'O')";

		}
		System.out.println(query);
		return query;
	}
}
