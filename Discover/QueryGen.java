
import java.util.*;
import java.io.*;

public class QueryGen extends discover.Global{
	public static void main (String args[]) throws IOException{
	    //String[] tableNames = {"Person", "Series", "Publisher", "Proceeding", "InProceeding"};
	    if (args.length != 2) {
	        System.out.println("QueryGen queryNum keywordNum");
	    }
	    
	    int queryNum = Integer.valueOf(args[0]).intValue();
	    int keywordNum = Integer.valueOf(args[1]).intValue();
//	    int resultSize = Integer.valueOf(args[2]).intValue();
	    
            String record = null; 
            Integer count;
	    String word = null;
	    
//            TreeMap<String, Integer> dict = new TreeMap<String, Integer>();  
//     	       FileReader fr = new FileReader(directory + "AllFullText.index"); 
//                BufferedReader br = new BufferedReader(fr);
// 	    
//                while ( (record=br.readLine()) != null ) { 
//                   String[] ss = record.split(" ");
// 	          for (int i=0; i < ss.length; i++) {
// 	              if (!ss[i].equals("")) {
// 	                 count = Integer.valueOf(ss[i]);
// 	   	         word = ss[ss.length-1];
// 		         if (dict.containsKey(word)) {
// 		             count = count + dict.get(word);
// 		             dict.remove(word);
// 		         }  
// 		         dict.put(word, count);
// 		         //System.out.println(len+" "+ss[ss.length-1]);
// 		         break;
// 		      }
// 	           }      
// 		}   
//             
// 	    long s = 0;
// 	    int n;
// 	    for (String w: dict.keySet()) {
// 	        n = dict.get(w);
// 	        System.out.println(w+" "+n+" "+s);
// 		s = s + n;
// 	    }

	    Random generator = new Random( (int)System.currentTimeMillis() );
	    int[] nset = new int[queryNum * keywordNum];
	    String[] sset = new String[queryNum * keywordNum];
            for (int i = 0; i < queryNum * keywordNum; i++) {
	         nset[i] = generator.nextInt(1576603);
	    }
	    Arrays.sort(nset);
	   
	    FileReader fr = new FileReader("FullDict"); 
            BufferedReader br = new BufferedReader(fr);	    	
	    
            count = -1;
            String[] ss = new String[3];
	    
	    for (int i=0; i<queryNum*keywordNum; i++) {
		while (count <= nset[i]) {
		    word = ss[0];
		    record = br.readLine();
	            ss = record.split(" ");
		    count = Integer.valueOf(ss[2]);
                }
		sset[i] = word;
	    }
	    for (int i = 0; i < queryNum; i++) {
	        for (int j = 0; j < keywordNum; j++) {
		    System.out.print(sset[i*keywordNum+j]+" ");
		}
		System.out.println();
	    }
	    	    
 	}
}  
