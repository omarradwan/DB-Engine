package B3gr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.ServiceConfigurationError;


public class DBApp {
	
	FileWriter fw ;
	FileWriter fw2;
	
	public DBApp() throws IOException{
		fw = new FileWriter("tables.csv", true);
		fw2 = new FileWriter("NRecords.csv",true);
	}
	public void init( ) throws IOException{
		
		 fw = new FileWriter("tables.csv");
		 fw2 = new FileWriter("NRecords.csv");
    }

	public boolean checkIfNameExist(String tableName) throws IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("tables.csv"));
		String line;
		while ((line = br.readLine()) != null) {

			String[] colomn = line.split(",");
			if(colomn[0].equals(tableName))				
				return true;
			
		}
		return false;
	}
	
	public int readFromNReacords(String tableName) throws NumberFormatException, IOException{
		
		BufferedReader br = new BufferedReader(new FileReader("NRecords.csv"));
		String line;
		while ((line = br.readLine()) != null) {
			String[] colomn = line.split(",");
			if(colomn[0].equals(tableName))				
				return Integer.parseInt(colomn[1]);
			
		}
		return -1;
		
	}

	public void writeOnTables(String strTableName,    Hashtable<String,String> htblColNameType, 
            Hashtable<String,String> htblColNameRefs, String strKeyColName) throws IOException, ClassNotFoundException, DBAppException{
		
		fw2.append(strTableName+","+0+"\n");
		fw2.flush();
		Iterator<Map.Entry<String,String>> it = htblColNameType.entrySet().iterator();
		while(it.hasNext())
		{
			Map.Entry<String,String> entry = it.next();
			String colName = entry.getKey();
			String colType = entry.getValue();
			fw.append(strTableName+","+colName+","+colType+",");
			if(colName.equals(strKeyColName))
				fw.append("True,");
			else
				fw.append("False,");
			if(htblColNameRefs.containsKey(colName))
				fw.append(htblColNameRefs.get(colName)+"\n");
			else
				fw.append("null"+"\n");

		}
		
		fw.flush();
	}
	
	public boolean checkIfReferenceExist(Hashtable<String, String>htblColNameRefs)
	{
		Iterator<Entry<String, String>> it = htblColNameRefs.entrySet().iterator();
		
		while(it.hasNext())
		{
			Map.Entry<String,String> entry = it.next();
			String value = entry.getValue();  
			String tblName = value.split("\\.")[0];
			File f = new File(tblName+"_1.class");
			if(!f.exists())
				return false;
		}
		return true;
	}
	
    public void createTable(String strTableName,    Hashtable<String,String> htblColNameType, 
                            Hashtable<String,String> htblColNameRefs, String strKeyColName)  throws DBAppException, IOException, ClassNotFoundException{
    
    	if(checkIfNameExist(strTableName))
    		throw new DBAppException("Table already exist!");
    	 
    	if(!checkIfReferenceExist(htblColNameRefs))
    		throw new DBAppException("Referenced table does not exist");
    	
    		writeOnTables(strTableName, htblColNameType, htblColNameRefs, strKeyColName);
    		CreateObjectTable(strTableName , htblColNameType);	
    }

    public void CreateObjectTable(String tableName , Hashtable<String, String> htblColNameType) throws IOException{
    	
    	Hashtable<String, Object> hs = new Hashtable<>();
    	hs.put("mora1", "mora");
    	hs.put("misho1", "misho");
    	hs.put("mora2", "mora");
    	hs.put("misho2", "misho");
    	hs.put("mora3", "mora");
    	hs.put("misho3", "misho");
    	Record r = new Record(tableName, hs);
    	String path = tableName+"_1.class";
    	Page p = new Page(path);
    	p.add(r);
    	Table t = new Table(tableName, htblColNameType);
    	t.add(path);
    	File table = new File(tableName+".class");
    	table.createNewFile();
    	
    	t.write();
    	
       	File page = new File(path);
       	page.createNewFile();
       	
    	p.write();
    }
    
    public void createIndex(String strTableName, String strColName)  throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException{
    	
    	Table currentTable = Table.read(strTableName+".class");
    	SIPair siPair ;
    	ArrayList<String> paths = new ArrayList<>();
    	ArrayList<SIsPair> colomnsLists = currentTable.getSIsPairs();
    	SIsPair pair = new SIsPair(strColName, new SecondaryIndex());
    	for (int i = 0; i < colomnsLists.size(); i++) {
			SIsPair newPair = colomnsLists.get(i);
			if (newPair.colName.equals(strColName)){
				pair = newPair;
				break;
			}
		}
    	
    	SecondaryIndex SI = pair.SI;
    	ArrayList<SIPair> SIPairs = SI.pairs;
    	int counter = 0;
        ArrayList<Record> matches = new ArrayList<>();
        while(true)
        {
        	counter++;
        	String path = strTableName+"_"+counter+".class";
        	File currentFile = new File(path);
        	if(!currentFile.exists())
        		break;
        	
            Page currentPage = Page.read(path);
            for (int i = 0; i < currentPage.getRecords().size(); i++) {
            	if(i == 0 && counter ==1)
            		continue;
            	
				Record currentRecord = currentPage.getRecords().get(i);
				Hashtable<String, Object> recordHt = currentRecord.getHtblColNameValue();
				Object currentValue = recordHt.get(strColName);
			    
				boolean flag = false;
				for (int j = 0; j <SIPairs.size() ; j++) {
					SIPair SIpair = SIPairs.get(j);
					if(SIpair.distinctValue.equals(currentValue))
					{
						SIpair.paths.add(path+","+i);
						flag = true;
						break;
					}
				}
				
				if(!flag)
				{
					ArrayList<String> al = new ArrayList<>();
					al.add(path+","+i);
					SIPair newSIpair = new SIPair(currentValue+"", al);
					SIPairs.add(newSIpair);
				}
            }
            
        }
        //System.out.println(pair.colName);
    	colomnsLists.add(pair);
    	currentTable.write();
    }

    public boolean duplicateID(String strTableName , Hashtable<String,Object> htblColNameValue) throws FileNotFoundException, IOException, ClassNotFoundException{
    	
    	int counter = 0;
    	Object primaryKey;
    	while(true)
    	{
    		++counter;
    		String path = strTableName+"_"+counter+".class";
    		File currentFile = new File(path);
    		if(!currentFile.exists())
    			break;
    			
    		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(currentFile));
            Page currentPage = (Page)ois.readObject();
            ois.close();
            
            for (int i = 0; i < currentPage.getRecords().size(); i++) {
				Record currentRecord = currentPage.getRecords().get(i);
				primaryKey = currentRecord.getPrimaryKey();
				Object primaryKeyValue = htblColNameValue.get(primaryKey);
				Object primaryKeyOfCurrentRecord = currentRecord.htblColNameValue.get(currentRecord.getPrimaryKey());
				if(primaryKeyValue.equals(primaryKeyOfCurrentRecord))
					return true;
				
            }
    	}
    	
    	return false;
    }
    
    
    public boolean InvalidColomnType(String strTableName, Hashtable<String,Object> htblColNameValue) throws FileNotFoundException, IOException, ClassNotFoundException{
    	
    	Hashtable<String,Object>colomndatatype=new Hashtable<>();
    		BufferedReader br = new BufferedReader(new FileReader("tables.csv"));
    		String line;
    		int result = 0;
    		String format="class java.lang.";
    		while ((line = br.readLine()) != null) {

    			String[] colomn = line.split(",");
    			if(colomn[0].equals(strTableName))
    				colomndatatype.put(colomn[1],colomn[2] );
    				
    	}
    		Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
			//boolean found = true;
			
				while(it.hasNext())
				{
					Map.Entry<String,Object> entry = it.next();
					Object colName = entry.getKey();
					Object colValue = entry.getValue();
					String object=(String)colomndatatype.get(colName);
					String o=format+object;
					Object a=o;
					if (!(o.equals(colValue.getClass()+""))) {
						//System.out.println(o+"  "+colValue.getClass());
						return true;
					}
					
				
				}
				return false;
		
    }
    
    
    public void insertIntoTable(String strTableName, Hashtable<String,Object> htblColNameValue)  throws DBAppException, NumberFormatException, IOException, ClassNotFoundException{
    	
    //	if(duplicateID(strTableName , htblColNameValue))
    	//	throw new DBAppException("Duplicate ID!");
    	
    	
    	//if (InvalidColomnType(strTableName, htblColNameValue)) 
    		//throw new DBAppException("Invalid colomn Type!");
		
    	
    	
    	int records = readFromNReacords(strTableName);
    	if(records == -1)
    		throw new DBAppException("Table does not exist!");
 
    	Table currentTable = Table.read(strTableName+".class");
    	ArrayList<String> pages = currentTable.getPages();
    	boolean complete = true;
    	Page currentPage = null;
    	for (int i = 0; i < pages.size(); i++) {
			currentPage = Page.read(pages.get(i));
			if(currentPage.getRecords().size() < 200){
				complete = false;
				break;
			}
		}
    	String path = "";
    	if(complete){ 
    		path = strTableName+"_"+(pages.size()+1)+".class";
    		File f = new File(path);
       		f.createNewFile();
       		Page p  = new Page(path);
       		p.write();
       		currentPage = p;
       		currentTable.add(path);
    	}
    	
    	Record r = new Record(strTableName, htblColNameValue);  
    	Object primarykeyvalue = r.getPrimaryKeyValue(); 
    	currentTable.btree.insert((Comparable) primarykeyvalue, path+","+currentPage.getRecords().size());
    	currentPage.add(r);
    	currentPage.write();
   		currentTable.write();
        updateNRecords(strTableName);
        
    }
    
    public void updateNRecords(String tableName) throws IOException{
    	 
		BufferedReader br = new BufferedReader(new FileReader(new File("NRecords.csv")));
		//
		String line;
		StringBuilder st=new StringBuilder();
		
		
		while ((line = br.readLine()) != null) {
			String [] processedLine = processLine(line , tableName);
			st.append(join(processedLine)+"\n");
		}
		File outputFile = new File("NRecords.csv");
		FileWriter f=new FileWriter(outputFile);
		f.append(st.toString());
		f.flush();
	
    	
    }
    public String join (String[] s){
		
		String result = "";
		for (int i = 0; i < s.length; i++) {
			result+=s[i]+",";
		}
		return result.substring(0, result.length()-1);
	}
	
	public  String[] processLine(String line , String tableName) {
	    String [] cells = line.split(","); // note this is not sufficient for correct csv parsing.
	  if(cells[0].equals(tableName)){
	    int n =	Integer.parseInt(cells[1]) +1;
	    cells[1] = n+"";
	  }
	    return cells;
	 }


    public void updateTable(String strTableName, Object strKey,
                            Hashtable<String,Object> htblColNameValue)  throws DBAppException, FileNotFoundException, ClassNotFoundException, IOException{
    
    	int counter = 0;
    	boolean found = false;
        
        while(true)
        {
        	counter++;
        	String path = strTableName+"_"+counter+".class";
        	File currentFile = new File(path);
        	if(!currentFile.exists())
        		break;
            Page currentPage = Page.read(path);
   
            for (int i = 0; i < currentPage.getRecords().size(); i++) {
            	
            	if(i == 0 && counter ==1)
            		continue;
            	
            	Record currentRecord = currentPage.getRecords().get(i);
				Object primaryKeyValue = currentRecord.getPrimaryKeyValue(); 
				Hashtable<String, Object> RecordHt = currentRecord.getHtblColNameValue();
				Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
				if(primaryKeyValue.equals(strKey)){
					found = true;
					while(it.hasNext())
					{
						Map.Entry<String,Object> entry = it.next();
						String colomn = entry.getKey();
						Object value = entry.getValue();
						RecordHt.put(colomn, value);
					}
				}
				if(found)
					break;
				
            }
            
            if(found){
            	currentPage.write();
				System.out.println("ubdated");
				break;
			}   
        }
        
				
    }


    
    
    public void deleteFromTable(String strTableName, Hashtable<String,Object> htblColNameValue, 
                                String strOperator) throws Exception{
    	
    	File currentFile = null;
        Page currentPage = null;
    	
    	
    	Table currentTable = Table.read(strTableName+".class");
    	ArrayList<ArrayList<Record>> records = new ArrayList<>();
		Iterator<Entry<String, Object>> iterator = htblColNameValue.entrySet().iterator();
		while(iterator.hasNext())
		{
			Map.Entry<String,Object> entry = iterator.next();
			Object colName = entry.getKey();
			Object colValue = entry.getValue();
			createIndex(strTableName, colName+"");
			ArrayList<Record> recordsArraylist = new ArrayList<>();
			ArrayList<SIsPair> SIspairs = currentTable.getSIsPairs();
			SIsPair SIspair = null;
			for (int i = 0; i < SIspairs.size(); i++) {
				SIspair = SIspairs.get(i);
				if(SIspair.colName.equals(colName+""))
					break;
			}
					
			SecondaryIndex secondaryIndex = SIspair.SI;
			SIPair sipair = null ;
			for (int j = 0; j < secondaryIndex.pairs.size(); j++) {
				sipair = secondaryIndex.pairs.get(j);
				if(sipair.distinctValue.equals(colValue+""))
					break;
					}
			if(sipair == null)
				break;
			ArrayList<String> paths = sipair.paths;
			for (int k = 0; k < paths.size(); k++) {
				String path = paths.get(k);
				String [] s = path.split(",");
				String pagePath = s[0];
				int recordIndex = Integer.parseInt(s[1]);
				currentFile = new File(pagePath);
				if(!currentFile.exists())
					break;
					        	
				currentPage = Page.read(pagePath);
				Record currentRecord = currentPage.records.get(recordIndex);
				recordsArraylist.add(currentRecord);				            
			}
				records.add(recordsArraylist);
							
		}
					
			ArrayList<Record> matches = new ArrayList<>();
            for (int j = 0; j < records.size(); j++) {
				ArrayList<Record> colomnRecords = records.get(j);
			
            for (int i = 0; i < colomnRecords.size(); i++) {
            	if(i == 0)
            		continue;
            	
				Record currentRecord = colomnRecords.get(i);
				Hashtable<String, Object> recordHt = currentRecord.getHtblColNameValue();
				Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
				boolean found = true;
				if(strOperator.toUpperCase().equals("AND")){
					while(it.hasNext())
					{
						Map.Entry<String,Object> entry = it.next();
						Object colName = entry.getKey();
						Object colValue = entry.getValue();
						if(!(recordHt.get(colName).equals(colValue))){
							found = false;
							break;
						}
					}
					
				}
				else{
					while(it.hasNext())
					{
						Map.Entry<String,Object> entry = it.next();
						Object colName = entry.getKey();
						Object colValue = entry.getValue();
						if(recordHt.get(colName).equals(colValue)){
						//	System.out.println(colName+" "+colValue);
							found = true;
							break;
						}
						else
							found = false;
					}
				}
				if(found){
					System.out.println("found");
					currentPage.getRecords().remove(currentRecord);
					i--;
				}
			}
            
            currentPage.write();
        }                   
    	
    }
		
    public Iterator selectFromTable(String strTable,  Hashtable<String,Object> htblColNameValue, 
                                    String strOperator) throws Exception{
       
    	
        int counter = 0;
        ArrayList<Record> matches = new ArrayList<>();
        while(true)
        {
        	counter++;
        	String path = strTable+"_"+counter+".class";
        	File currentFile = new File(path);
        	if(!currentFile.exists())
        		break;
        	
            Page currentPage = Page.read(path);
   
            for (int i = 0; i < currentPage.getRecords().size(); i++) {
            	if(i == 0 && counter ==1){
            		continue;
            	}
				Record currentRecord = currentPage.getRecords().get(i);
				Hashtable<String, Object> recordHt = currentRecord.getHtblColNameValue();
				Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
				boolean found = true;
				if(strOperator.toUpperCase().equals("AND")){
					while(it.hasNext())
					{
						Map.Entry<String,Object> entry = it.next();
						Object colName = entry.getKey();
						Object colValue = entry.getValue();
						if(!(recordHt.get(colName).equals(colValue))){
							found = false;
							break;
						}
					}
					
				}
				else{
					while(it.hasNext())
					{
						Map.Entry<String,Object> entry = it.next();
						Object colName = entry.getKey();
						Object colValue = entry.getValue();
						if(recordHt.get(colName).equals(colValue)){
						//	System.out.println(colName+" "+colValue);
							found = true;
							break;
						}
						else
							found = false;
					}
				}
				if(found)
					matches.add(currentRecord);
			}
        }
    	System.out.println(matches.size());
    	return matches.iterator();
    	
    	
    	
    	
//    	Table currentTable = Table.read(strTable+".class");
//    	ArrayList<ArrayList<Record>> records = new ArrayList<>();
//		Iterator<Entry<String, Object>> iterator = htblColNameValue.entrySet().iterator();
//		while(iterator.hasNext())
//		{
//			Map.Entry<String,Object> entry = iterator.next();
//			Object colName = entry.getKey();
//			Object colValue = entry.getValue();
//			createIndex(strTable, colName+"");
//			ArrayList<Record> recordsArraylist = new ArrayList<>();
//			ArrayList<SIsPair> SIspairs = currentTable.getSIsPairs();
//			SIsPair SIspair = null;
//			//System.out.println(SIspairs.size());
//			for (int i = 0; i < SIspairs.size(); i++) {
//				SIspair = SIspairs.get(i);
//				System.out.println(SIspair.colName);
//				if(SIspair.colName.equals(colName+""))
//					break;
//			}
//					
//			SecondaryIndex secondaryIndex = SIspair.SI;
//			SIPair sipair = null ;
//			for (int j = 0; j < secondaryIndex.pairs.size(); j++) {
//				sipair = secondaryIndex.pairs.get(j);
//				if(sipair.distinctValue.equals(colValue+""))
//					break;
//					}
//			if(sipair == null)
//				break;
//			ArrayList<String> paths = sipair.paths;
//			for (int k = 0; k < paths.size(); k++) {
//				String path = paths.get(k);
//				String [] s = path.split(",");
//				String pagePath = s[0];
//				int recordIndex = Integer.parseInt(s[1]);
//				File currentFile = new File(pagePath);
//				if(!currentFile.exists())
//					break;
//					        	
//				Page currentPage = Page.read(pagePath);
//				Record currentRecord = currentPage.records.get(recordIndex);
//				recordsArraylist.add(currentRecord);				            
//			}
//				records.add(recordsArraylist);
//							
//		}
//					
//			ArrayList<Record> matches = new ArrayList<>();
//            for (int j = 0; j < records.size(); j++) {
//				ArrayList<Record> colomnRecords = records.get(j);
//			
//            for (int i = 0; i < colomnRecords.size(); i++) {
//            	if(i == 0)
//            		continue;
//            	
//				Record currentRecord = colomnRecords.get(i);
//				Hashtable<String, Object> recordHt = currentRecord.getHtblColNameValue();
//				Iterator<Entry<String, Object>> it = htblColNameValue.entrySet().iterator();
//				boolean found = true;
//				if(strOperator.toUpperCase().equals("AND")){
//					while(it.hasNext())
//					{
//						Map.Entry<String,Object> entry = it.next();
//						Object colName = entry.getKey();
//						Object colValue = entry.getValue();
//						if(!(recordHt.get(colName).equals(colValue))){
//							found = false;
//							break;
//						}
//					}
//					
//				}
//				else{
//					while(it.hasNext())
//					{
//						Map.Entry<String,Object> entry = it.next();
//						Object colName = entry.getKey();
//						Object colValue = entry.getValue();
//						if(recordHt.get(colName).equals(colValue)){
//						//	System.out.println(colName+" "+colValue);
//							found = true;
//							break;
//						}
//						else
//							found = false;
//					}
//				}
//				if(found)
//					matches.add(currentRecord);
//			}
//           }
//        
//    	System.out.println(matches.size());
//    	return matches.iterator();
    }

	


}
