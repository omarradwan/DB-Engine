package B3gr;

import java.io.Serializable;
import java.util.ArrayList;

public class SecondaryIndex implements Serializable{
	
	ArrayList<SIPair> pairs;
	
	public SecondaryIndex()
	{
		pairs = new ArrayList<SIPair>();
	}

}
