package B3gr;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable {
	
	ArrayList<Record>records;
	String primaryKey;
	String path;
	
	
	public Page(String path){
		
	this.records = new ArrayList<>();
	this.path = path;
	}
	
	public void add(Record r){
		
		this.records.add(r);
		if(records.size() == 2)
			this.primaryKey = records.get(0).getPrimaryKey();
		
	}
	
	public ArrayList<Record> getRecords(){
		
		return this.records;
	}
	
	public void write() throws FileNotFoundException, IOException{
		
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(this.path)));
     	oos.writeObject(this);
       	oos.close();	
	}
	
	public static Page read(String path) throws FileNotFoundException, IOException, ClassNotFoundException{
		
    	ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(path)));
        Page currentPage = (Page)ois.readObject();
        ois.close();
        return currentPage;
	}
}
