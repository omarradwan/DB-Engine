package B3gr;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class Record implements Serializable{

	Hashtable<String,Object> htblColNameValue;
	ArrayList<String> colomnName;
	String tableName;
	String [] data;
	String primaryKey;
	Object primaryKeyValue;
	//Hashtable<String , String> colomndatatype;
	final static String path = "tables.csv";
	
	public Hashtable<String, Object> getHtblColNameValue(){
		return this.htblColNameValue;
	}
	
	public Object getPrimaryKeyValue(){
		
		return this.primaryKeyValue;
	}
	
	public Record(String tableName , Hashtable<String, Object> htblColNameValue) throws IOException{
		
		this.tableName = tableName;
		this.htblColNameValue = htblColNameValue;
		this.primaryKey = findPrimaryKey();
		this.primaryKeyValue = htblColNameValue.get(primaryKey);
		//this.colomndatatype=new Hashtable<>();
		//findColomnType();
		//colomnName = new ArrayList<>();
		//int Ncolomns = readFile(this.tableName);

		//data = new String [Ncolomns];
		//add(this.htblColNameValue);
	}
	

	public String getPrimaryKey(){
		return this.primaryKey;
	}
	
	public String findPrimaryKey() throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("tables.csv"));
		String line;
		String result;
		while ((line = br.readLine()) != null) {
			String[] colomn = line.split(",");
			if(colomn[0].equals(this.tableName) && colomn[3].equals("True"))			
				return colomn[1] ;
			
		}
		return null;
	}
	
	public void add (Hashtable<String,Object> htblColNameValue ){
		
		for (int i = 0; i < data.length; i++) 
			data[i] = htblColNameValue.get(colomnName.get(i))+"";
		
	}
	
		
		
		
	
	
	
	public String toString()
	{
		String res = "";
		Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<String,Object> entry = it.next();
			String colName = entry.getKey();
			String colValue = entry.getValue()+"";
			res+=colValue+",";
		}
		res = res.substring(0,res.length()-1);
		res+="\n";
		return res;
	}
	
	public int readFile(String tableName) throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("tables.csv"));
		String line;
		int result = 0;
		while ((line = br.readLine()) != null) {

			String[] colomn = line.split(",");
			if(colomn[0].equals(tableName)){				
				colomnName.add(colomn[1]);
				result++;
			}
		}
		return result;
	}
}
