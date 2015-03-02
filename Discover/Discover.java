  

//import java.sql.*;
//import java.math.*;
//import java.util.*; 
import java.io.*; 

import discover.SchemaGraph;
import discover.Query;
import discover.Global;

public class Discover{ 
	public static void main (String args[]) throws IOException, Exception{
//		---------------------------------------------------------------------------------------------------

//		boolean checkAllKeywords = false;  //Boolean OR
//		if (args[0].equalsIgnoreCase("True")) {
//		checkAllKeywords = true; //Boolean AND
//		}
		
		String infilename = args[0];
		

//		---------------------------------------------------------------------------------------------------
		System.out.println("==================================================\n");

		//test: output the list of tables' names and matrix of schema graph
		FileReader fr = new FileReader(infilename);
		BufferedReader br = new BufferedReader(fr);

		Global global = new Global();
		global.setAttributes(br);
		SchemaGraph sg = new SchemaGraph(global);		
		sg.setDataSet();

	//	sg.updateDataSet("imdb");
		
		String keywords;
		int queryId; 
		while ( (keywords = br.readLine()) != null) {System.out.println(keywords);
			queryId = Integer.valueOf(keywords.substring(0, keywords.indexOf(" "))).intValue();
			keywords = keywords.substring(keywords.indexOf(" ")+1); 
			Query q = new Query(sg);
			q.queryProcessing(queryId, keywords);
			//ArrayList<StringBuffer> resStrings = sg.allResults.getAllDataStrings();
		}   

/*		queryId = 1;
		keywords = "Japanese";
		Query q = new Query(sg);
		q.queryProcessing(queryId, keywords);
*/		
		
	}


}
