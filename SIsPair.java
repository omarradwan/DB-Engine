package B3gr;

import java.io.Serializable;
import java.util.ArrayList;

public class SIsPair implements Serializable{
		
		String colName ;
		SecondaryIndex SI ;
		
		public SIsPair(String colName , SecondaryIndex SI)
		{
			this.colName = colName;
			this.SI = SI;
		
	}
	
}