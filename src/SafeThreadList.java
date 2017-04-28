import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.bson.Document;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoSocketWriteException;
import com.mongodb.client.MongoCollection;

public class SafeThreadList {
	
	private  CopyOnWriteArrayList<Document> waitingData = new CopyOnWriteArrayList<Document>();
	private MongoCollection<Document> collection;
	
	public void setCollection(MongoCollection<Document> collection){
		this.collection=collection;
	}
	
	public CopyOnWriteArrayList<Document> getData(){
		return waitingData;
	}
	
	public void add(Document doc){
		waitingData.add(doc);
	}
	
	public int getSize(){
		return waitingData.size();
	}

	public void sendData(){
		if(!waitingData.isEmpty()){
			try{
				for(Iterator<Document> data = waitingData.iterator(); data.hasNext(); ){
					collection.insertOne(data.next());
				}
				waitingData.clear();
			}catch (MongoSocketOpenException | MongoCommandException | MongoSocketWriteException waiting){
				System.out.println("Boop");
			}
		}
	}

}
