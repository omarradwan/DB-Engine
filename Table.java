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
import java.util.Hashtable;

public class Table implements Serializable{

	ArrayList<String> pages;
	Hashtable<String,String> htblColNameType;
	String name;
	String path;
	BTree btree;
	ArrayList<SIsPair> SIsPairs ;
	
	public Table(String name ,Hashtable<String,String> htblColNameType){
		
		this.name = name;
		this.path = name+".class";
		this.htblColNameType = htblColNameType;
		pages = new ArrayList<>();
		btree = new BTree<>();
		this.SIsPairs = new ArrayList<>();
	}
	
	
	public ArrayList<SIsPair> getSIsPairs()
	{
		return this.SIsPairs;
	}
	
	
	public BTree getBtree()
	{
		return this.btree;
	}
	
	public void add (String path)
	{
		pages.add(path);
	}

	
	
	
	public void write() throws FileNotFoundException, IOException{
		
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(new File(this.path)));
     	oos.writeObject(this);
       	oos.close();	
	}
	
	public static Table read(String path) throws FileNotFoundException, IOException, ClassNotFoundException{
		
    	ObjectInputStream ois = new ObjectInputStream(new FileInputStream(new File(path)));
        Table currentTable = (Table)ois.readObject();
        ois.close();
        return currentTable;
	}
	
	public ArrayList<String> getPages() {
		return pages;
	}
	
	
	
	
	
	
}


