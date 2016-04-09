package B3gr;

import java.io.Serializable;
import java.util.ArrayList;

public class SIPair implements Serializable{
		
		String distinctValue ;
		ArrayList<String> paths ;
		
		public SIPair(String distinctValue , ArrayList<String> paths)
		{
			this.distinctValue = distinctValue;
			this.paths = paths;
		
	}
	
}
