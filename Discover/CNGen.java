import java.io.*; 

import discover.SchemaGraph;
import discover.Global;
import discover.SolutionTree;

class CNGen {	//for CN generation

	public static void main (String args[]) throws IOException, Exception{
//		---------------------------------------------------------------------------------------------------

//		boolean checkAllKeywords = false;  //Boolean OR
//		if (args[0].equalsIgnoreCase("True")) {
//		checkAllKeywords = true; //Boolean AND
//		}
		
		String infilename = args[0];
		String outfilename = args[1];
		int maxi = Integer.valueOf(args[2]).intValue();

//		---------------------------------------------------------------------------------------------------
		System.out.println("==================================================\n");

		//test: output the list of tables' names and matrix of schema graph
		FileReader fr = new FileReader(infilename);
		BufferedReader br = new BufferedReader(fr);

		FileWriter fw= new FileWriter(outfilename);
		BufferedWriter bw = new BufferedWriter(fw);
		
		Global global = new Global();
		global.setAttributes(br);
		global.OFFLINE_CN_SEARCH = false;
		
		SchemaGraph sg = new SchemaGraph(global);		
		sg.setDataSet();

	//	sg.updateDataSet("imdb");
		
		for (int i=3; i<=maxi; i++) {
			global.setAttribute("MAXCN", String.valueOf(i));
			
			SolutionTree stree = new SolutionTree(sg);
			long time = stree.generateCNG();		//generate CNs without duplication
			
			SolutionTree streeND = new SolutionTree(sg);
			long timeND = streeND.generateNoDupCNG();		//generate CNs without duplication
			
			bw.write(String.valueOf(i)+" "+String.valueOf(time)+" "+String.valueOf(timeND));
			bw.newLine();			
		}
		bw.close();
	}

}

