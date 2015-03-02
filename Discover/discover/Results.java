package discover;
import java.sql.*;
//import java.math.*;
import java.util.*;
import java.util.regex.*;
import java.text.DecimalFormat;

class ResultItem implements Comparable {
	public double score;
	public int id;
	public Solution sol;
	public StringBuffer resultData;
	public boolean printed;
	public boolean passFinalCheck;
	public long elapsedTime;
	public int dl;
	public int[] tfs; 
	           
	public ResultItem(int _id, Solution _s) {
		sol = _s;
		Time joinTime = sol.query.allResults.joinTime; 
		joinTime.stop();		
		elapsedTime = joinTime.getTime();
		joinTime.start();
		id = _id;
		dl = 0;
		score = 0;
		tfs = new int[sol.query.getKwNum()];
		for (int i=0; i<tfs.length; i++) {
			tfs[i] = 0;
		}
	}
	public ResultItem(Solution _s) { //only used for pseudo object
		sol = _s;
		tfs = new int[sol.query.getKwNum()];
		for (int i=0; i<tfs.length; i++) {
			tfs[i] = 0;
		}

	}
	public void setTfs(int[] _tfs) {
		tfs = (int[])_tfs.clone();
	}
	public void setDl(int _dl) {
		dl = _dl;
	}
	
	public boolean finalCheckWithTfs() {
		passFinalCheck = true;
//		int k = sol.query.keywords.length; 
		for (int i=0; i<tfs.length; i++) {
			if (tfs[i] == 0 && sol.query.getKeyword(i).getPrefix() == Global.AND_SYNTAX) {
				passFinalCheck = false;
				break;
			}	
		}
		return passFinalCheck;
	}
	
	public boolean finalCheckWithData() {
		passFinalCheck = true;
		String dataS = resultData.toString().toLowerCase();
		dataS = Global.cleanData(dataS);
/*		for (int i=0; i<sol.query.getKwNum(); i++) {
			String w= sol.query.getKeyword(i);
			int available = dataS.indexOf(w.toLowerCase());
			if (available <= 0 && sol.query.getPrefix(i) == Global.AND_SYNTAX) {
				passFinalCheck = false;
				break;
			}
		}
		*/
		for (Keyword notW: sol.query.notWords) {
			int available = dataS.indexOf(notW.getStr().toLowerCase());
			if (available >= 0) {
				passFinalCheck = false;
				break;
			}
		}
//System.out.println(resultData+"  "+passFinalCheck);
		return passFinalCheck;
	}
	
	public void updateTf(String newTfString) {
		String[] tfsList = newTfString.split(" ");
		int listSize = tfsList.length;
		for (int i=0; i<listSize; i++) {
			tfs[i] = tfs[i] + Integer.valueOf(tfsList[i]);
		}
		//dl = dl + Integer.valueOf(tfsList[listSize]);
	}
	
	public int compareTo(Object ri) {
		if (((ResultItem)ri).id == id) {
			return 0;
		}    
		if (((ResultItem)ri).score > score) {
			return 1;
		}    
		if (((ResultItem)ri).score == score) {
			return (id - ((ResultItem)ri).id);
		}
		return -1;
	}
	
	public StringBuffer getPrintData() throws SQLException {
		StringBuffer printData = new StringBuffer();
		StringBuffer originalLink = new StringBuffer("search.jsp?frmAction=SEARCH&frmSearch=");
		for (int j = 0; j < sol.query.notWords.size(); j++) {
			//originalLink.append('-');
			Keyword k = sol.query.notWords.get(j);
			originalLink.append(k.getInputStr());
			//int id = sol.query.getKeywordIndex(Global.NOT_SYNTAX, j);
			//String notWord= sol.query.notWords.get(j).getStr();
			//if (sol.query.isPhrase(id)) originalLink.append("'").append(notWord).append("' ");
			//else originalLink.append(notWord).append(' ');
		}
		int node = 1;
		//resultData.append("");
		int colspan = 6;
		//resultData.append("<tr>");
		String[] nodeStrings = resultData.toString().split("<");
		HashMap<String, Pattern> keyPtns = new HashMap<String, Pattern>();
		for (int i=0; i<sol.query.getKwNum(); i++){
			String k = sol.query.getKeyword(i).getStr();		
			keyPtns.put(k, Pattern.compile(k, Pattern.CASE_INSENSITIVE));
		}
		for (GraphNode n: sol.getNodes()) {
			String s = new String(nodeStrings[node]);
			s = s.split(">")[0];
			int spacePos = s.indexOf(" ");
			s = s.substring(spacePos);
			printData.append("<tr><td width=\"1%\">&nbsp;&nbsp;&nbsp;</td>");
			
			for (int col = 0; col < 5 - colspan; col++) {
				printData.append("<td width=\"1%\" valign=\"bottom\"></td>");
			}
			if (colspan < 6) 
				printData.append("<td width=\"1%\" valign=\"top\"><img src=\"images/arrow_curve.gif\"></td>");
			
			printData.append("<td width=\"1%\" valign=\"top\" style=\"BACKGROUND-IMAGE: url('images/arrow_left.gif'); BACKGROUND-REPEAT: repeat-y; BACKGROUND-POSITION: top left; \"><img src=\"images/arrow_arrow.gif\" hspace=\"0\"></td>");
			printData.append("<td width=\"100%\"  class=\"Content\" valign=\"top\" colspan=\"").append(colspan).append("\">");
			
			printData.append("<a href=\"\" class=\"Table\">").append(n.getTableName()).append("</a>&nbsp;:&nbsp;");
			String[] cols = s.split("=");
			for (int i=0; i<cols.length-1; i++) {
				String colName = cols[i].substring(cols[i].lastIndexOf(" "));
				String data = cols[i+1].substring(0, cols[i+1].lastIndexOf(" "));
				data = data.replaceAll("\"","&quot;");
				StringBuffer link = new StringBuffer();
				link.append(originalLink);
				if (n.isNonFree()) {
					for (int j=0; j < sol.query.getKwNum(); j++) {
						
						Keyword k = sol.query.getKeyword(j);
//						String k = sol.query.getKeyword(j).getStr();
						Pattern kp = keyPtns.get(k.getStr());
						if (kp.matcher(data).find()) {
							link.append(n.getTableName()).append(":");
							link.append(k.getPrefixStr()).append("'").append(data).append("' ");
						} else {
							link.append(k.getInputStr()).append(" ");
						}
						data = kp.matcher(data).replaceAll("<b>"+k.getStr()+"</b>");
//if (kp.matcher(data).find()) System.out.println("match---"+data);
					}

//System.out.println(link);
				}
				printData.append("<a href=\"").append(link).append("\" class=\"BottomNavigation\">").append(colName).append(":</a>&nbsp;");
				printData.append(data);
			
			}
			colspan--;
			node++;
			printData.append("</td><td valign=\"top\" rowspan=\"2\">");
			if (colspan == 5) {
				DecimalFormat myFormatter = new DecimalFormat("0.00");
				String scoreS = myFormatter.format(score);
				int userCN = sol.schema.stree.sols.indexOf(sol);
				printData.append("<table><tr><td>").append(scoreS).
				append("</td><td><a href=\"search.jsp?frmAction=SEARCH").
				append("&frmSearch=@").append(userCN).append(" ").append(sol.query.genKeywordList()).
						append("\"><img src=\"images/zoom.gif\" border=\"0\"></a></td></tr></table>");
			}
			printData.append("</td></tr>");
		}
System.out.println(printData);		
		return printData;
	}
	public StringBuffer setResultData(int rsId, ResultSet rs, String[] colNames) throws SQLException {
		resultData = new StringBuffer();
		int numberOfColumns;
		if (colNames == null) { //default: all columns
			ResultSetMetaData rsmd = rs.getMetaData();
			numberOfColumns = rsmd.getColumnCount();
			colNames = new String[numberOfColumns];
			for (int i=0; i<numberOfColumns; i++) {
				colNames[i] = rsmd.getColumnName(1 + i);
			}
		}
		else numberOfColumns = colNames.length;

		rs.absolute(rsId);
		//int i = 0;
		for (GraphNode n: sol.getNodes()) {
			resultData.append("<").append(n.getTableName()).append(" ");
			String tbl_name = n.getTableName();
			String tbl_name_q = n.getAlignName();
			
			String textCols = sol.schema.textCols.get(tbl_name);		
			//if (n.isNonFree() && textCols.length() > 0) {
			if (textCols != null && textCols.length() > 0) {
				
				int pKey = rs.getInt(tbl_name_q+"_"+n.getPrimaryKey());
				//	to get the text columns from original non-free tuple set
				//int rowN = rs.getInt(tbl_name_q+"_RowN");
				//ResultSet nonFreeSet = sol.query.nonFreeSets.get(tbl_name);
				//nonFreeSet.absolute(rowN);
				StringBuffer getString = new StringBuffer();
				getString.append("SELECT * FROM ").append(n.getTableName())
					.append(" WHERE ").append(n.getPrimaryKey()).append(" = ").append(pKey);
				Statement stmt = sol.schema.dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);

				ResultSet original = stmt.executeQuery(getString.toString());
				for (String col: textCols.split(",")) {						
					//alignCol = tbl_name_q+"_"+col;
					String data = sol.schema.dbconn.getDataString(original, 1, col);

					resultData.append(col).append("=").append(data).append(" ");
				}						
			}
			String pCols = sol.schema.primaryKey.get(tbl_name);
			if (pCols != null) {
				for (String col: pCols.split(",")) {
					resultData.append(col).append("=").append(rs.getString(tbl_name_q+"_"+col)).append(" ");
				}	
			}
			//while (i < numberOfColumns && colNames[i].indexOf(n.getAlignName()) >= 0) {
			//	String colName = colNames[i].split("_")[1];
			//	if (sol.schema.textCols.get(n.getTableName()) != null && sol.schema.textCols.get(n.getTableName()).contains(colName)) {
			//		String data = rs.getString(i+1);
			//		resultData.append(colName).append("=").append(data).append(" ");
			//	}
			//	i++;
			//}
			resultData.append("> ");	
		}
			
		//resultData.append("size=").append(sol.size()).append("> ");
		return resultData;
	}
	
	//public double setDl(int rsId, ResultSet rs) throws SQLException {

	
	public double setRealScore2() {    // for our function, bscore without dl normalization
		//SchemaGraph schema = sol.schema;
		Query query = sol.query;
		//ArrayList<String> keywords = query.keywords;
		double ndtf = 0.0;
		int countk = 0;
		double ic = 0.0;
		int maxtf = 0;
//		System.out.print("calculate rscore on ");
		for (int i = 0; i < query.getKwNum(); i++) {
			maxtf = (maxtf<tfs[i])?tfs[i]:maxtf;
		}
		for (int i = 0; i < query.getKwNum(); i++) {
//			System.out.print(dtfs[i]+" ");
			if (tfs[i] > 0) {
				double idf = (sol.global.trysepidf)?sol.idf.get(query.getKeyword(i).getStr()):query.globalIdf.get(query.getKeyword(i).getStr());
				countk++;
				//ndtf = ndtf + (Math.log(dtfs[i]) + 1) * schema.globalIdf.get(keywords[i]);
				ndtf = ndtf + (Math.log((Math.log(tfs[i]) + 1))+1) * idf;
				if (sol.global.checkScoreb) 
					ic = ic + Math.pow(1.0 - 1.0 * tfs[i] / maxtf * idf / sol.maxidf , sol.global.ALPHA);
			} else {
//				//ic = ic + Math.pow(schema.globalIdf.get(keywords[i]), ALPHA);
				ic = ic + 1;
			}
		}

		// calculate real score
		//rscore = ndtf / sumdtf * U * sol.globalWeight * (1.0 - ALPHA + ALPHA * countk / keywords.length);
		//rscore = ndtf * sol.globalWeight * (1.0 - ALPHA + ALPHA * countk / keywords.length);
		//rscore = ndtf * sol.globalWeight * (1.0 - Math.pow(ic/sol.sumidfpow, 1.0/ALPHA));
//		System.out.println("globalWeight:"+sol.globalWeight+"ic:"+ic+(ndtf * sol.globalWeight * (Math.pow(keywords.length, 1.0/ALPHA) - Math.pow(ic, 1.0/ALPHA))));

//		double dlNorm = (dl>0)?0.8 / (0.8 + 0.2 * dl / sol.avdl):1;
//		double dlNorm = 1;
//		System.err.println("dlnorm="+dlNorm+"  "+dl+" "+sol.avdl);
		score = ndtf * sol.globalWeight * (Math.pow(query.getKwNum(), 1.0/sol.global.ALPHA) - Math.pow(ic, 1.0/sol.global.ALPHA)); 
		//System.err.println(score);
		return score;
	}

	public double dlNormalization() {  //dl normalization
		double dlNorm = (dl>0)?(1-0.0115) / (1.0-0.0115 + 0.0115 * dl / sol.avdl):1; 
		//System.err.println("dlnorm="+dlNorm+"  "+dl+" "+sol.avdl);
		score = score * dlNorm;
		return score;
	}

	public int size() {
		return sol.size();
	}
}	
//--------------------------------------Results--------------------------------------------
public class Results{

	private TreeSet<ResultItem> allResults;
	public double minScore;
	private int id;
//	private boolean checkAllKeywords;
	//public long startTime;
	private Query query;
	private SchemaGraph schema;
	
	public Time joinTime;
	public Time SQLTime;				//part of join Time
	public Time preProcessTime;		//for building non-free tuple sets, etc.
	//private long maxTime;
	public int queryCount;
	public long querySize;
	public int pruned;
	public int printed;
	
	public double idfError;
	public int idfErrorCount;

	public int k() {
		return schema.global.k;
	}
	public long getJoinTime() {
		return joinTime.getTime();
	}
	public long getPreProcessTime() {
		return preProcessTime.getTime();
	}
	public Results(Query q) {
		id = 0;
		query = q;
		schema = q.schema;
		
		idfError = 0.0;
		idfErrorCount = 0;
		minScore = 0.0;
		printed = 0;
		
		allResults = new TreeSet<ResultItem>();
//		checkAllKeywords = _checkAllKeywords;
		
		joinTime = new Time();
		SQLTime = new Time();
		queryCount = 0;
		querySize = 0;
		pruned = 0;
		
		joinTime.start();
	//	maxTime = 0;
	}

	public double getMinScore() {
		return minScore;
	}	
	public int size() {
		return allResults.size();
	}
	public boolean full() {
		// System.out.println("FULL? " + allResults.size()+" "+k);
		return (allResults.size() == k());
	}	

	public boolean insert(ResultItem ri, int rsId, ResultSet rs) throws SQLException{
		boolean added = false;
		//Global global = schema.global;
		if (ri.score > minScore || size() < k()) {
			//id++;
			ri.setResultData(rsId, rs, null);

			//if (global.scoreFunc == 1) {	
			//ri.finalCheckWithData(); //late finalCheck
			ri.finalCheckWithData(); //late finalCheck
			//}

			if (ri.passFinalCheck) { //check again, since allKeywords may be changed
				allResults.add(ri);
				//System.out.println("Added "+ri.resultData+ri.score+"   size="+size());				
				added = true;

				if (size() > k()) {
					allResults.remove(allResults.last());
				}
				minScore = allResults.last().score;
			}
		}    
		//System.out.println(size()+" "+k);

		return added;
	}

	public boolean insert(ResultItem ri) throws SQLException {
		boolean added = false;
		if (ri.score > minScore || size() < k()) {
			ri.finalCheckWithData();
			if (ri.passFinalCheck) {
				id++;
				ri.id = id; 
				allResults.add(ri);
				added = true;
				if (size() > k()) {
					allResults.remove(allResults.last());
				}
				minScore = allResults.last().score;
			}
		}
		return added;
	}
	public void addResultSet(ResultSet rs, TodoItem todo) throws SQLException{
	
		Solution sol = todo.sol;
		Global global = schema.global;
		//ArrayList<String> keywords = sol.query.keywords;

//		int size = Global.resultSetSize(rs);
		if (!sol.inResults) {		//if it is an invalid CN; possible when userCN is given
			return;
		}
		boolean stop = false;
		int rsId = 0;

		ResultSetMetaData rsmd = rs.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		String[] colNames = new String[numberOfColumns];
		for (int i=0; i<numberOfColumns; i++) {
			colNames[i] = rsmd.getColumnName(1 + i);
		}
		double uscore = todo.possibleMax;
		//	double rscore = todo.possibleMax;
		//	boolean allKeywords = todo.containsAllKeywords;

		while (rs.next() && !stop) {
			rsId++;
			id++;
			ResultItem ri = new ResultItem(id, sol);

			switch (global.scoreFunc) {
			case 1:
				for (int j = 0; j < numberOfColumns; j++) {
					String colName = colNames[j];
					if (colName.toLowerCase().indexOf("upperscore") >= 0) {
						uscore = rs.getDouble(1 + j); 
					}
				}
				ri.score = uscore;
				//ri.passFinalCheck = true;	//case 1: final check not yet done!
				break;

			case 2:
				/*
				if (!todo.checkReal()) {
					for (int j = 0; j < numberOfColumns; j++) {
						String colName = colNames[j];
						if (colName.toLowerCase().indexOf("upperscore") >= 0) {
							uscore = rs.getDouble(1 + j); 
						}
						else if (colName.toLowerCase().indexOf("dtfs") >= 0) {
							ri.updateTf(rs.getString(1+j));
						}
						//ri.finalCheckWithTfs();
						ri.setRealScore2();
					}
				} else {	// almost real score!
					ri.score = todo.possibleMax;
					//ri.allKeywords = todo.containsAllKeywords;
				}
				for (int j = 0; j < numberOfColumns; j++) {
					String colName = colNames[j];
					if (colName.toLowerCase().indexOf("dl") >= 0) {
						ri.dl = ri.dl + rs.getInt(1 + j);
					}
				}
				ri.dlNormalization();*/
				for (int i=0; i < sol.query.getKwNum(); i++) {
					StringBuffer s = new StringBuffer("Dtf").append(i);
					ri.tfs[i] = rs.getInt(s.toString());
				}
				ri.score = rs.getDouble("Score");
				break;
				
			case 3:
				double[] dtfsosixsum = new double[query.getKwNum()];
				double[] dtfsosixmax = new double[query.getKwNum()];
				for (int j = 0; j < numberOfColumns; j++) {
					String colName = colNames[j];
					if (colName.toLowerCase().indexOf("dtfsosix") >= 0) {
						String[] sdtfs = rs.getString(1+j).split(" ");
						for (int i = 0; i < query.getKwNum(); i++) {
							dtfsosixsum[i] = dtfsosixsum[i] + Double.valueOf(sdtfs[i]) * sol.globalWeight;
							dtfsosixmax[i] = Math.max(dtfsosixmax[i], Double.valueOf(sdtfs[i]) * sol.globalWeight);
						}                                
					}
				}
				//int countk = 0;
				for (int i = 0; i < query.getKwNum(); i++) {
					if (dtfsosixmax[i] > 0) {
						ri.score = ri.score + dtfsosixmax[i] * (Math.log(Math.log(dtfsosixsum[i]/dtfsosixmax[i])+1)+1);
						//countk++;
					}
				}
				//ri.allKeywords = (countk == keywords.length);
				break;
			}

			insert(ri, rsId, rs);

//			if (rscore > uscore + 0.0001) System.err.println("rscore>uscore!!!!");
			if (global.scoreFunc != 3 && minScore >= uscore - Global.DELTA && full()) {
				stop = true; // stop before checking all results
			}
		}

		//rtime.stop();
		//if (rsId > 0) {
			//System.out.println("# Checked Results: "+count + " RTime: "+relapsedTime);
			//sol.foundRes = true;
		//}
		//System.out.println();

		//if (size0 != size()) dump();
	}


	public boolean dump(double uscore) {
//		System.out.println("Total Number of Found Results: " + id);
//		System.out.println("Top " + k + " Results: ");
       boolean printing = false; 
		int i = 0;
//System.out.println("HERE"+allResults.size());		
		for (ResultItem ri: allResults) {
			i++;
//System.out.println(ri.score+"  "+uscore +"   "+ri.printed);
			if (!ri.printed && ri.score >= uscore) {
				//if (ri.elapsedTime > maxTime) maxTime = ri.elapsedTime;
				//joinTime.stop();
				ri.printed = true;
				printed++;
				String resultData = "";
				if (ri.resultData != null) resultData = ri.resultData.toString() + Double.toString(ri.score);
				System.out.println("Q" + query.queryId+" A"+query.topKAlgorithm+" T"+i+" "+resultData);
				System.out.println(i+"Time: Q" + query.queryId+" A"+query.topKAlgorithm+" "+ri.score+
						" "+ri.size()+" "+joinTime.getTime()+" ("+ri.elapsedTime+")");
				//joinTime.start();
				printing = true;
			}
		}
		return printing;
	}	
	
    public ArrayList<StringBuffer> getAllDataStrings() throws SQLException {
    	//System.out.println(size());
    	ArrayList<StringBuffer> resultStrings = new ArrayList<StringBuffer>(); 
    	for (ResultItem ri: allResults) {
    		resultStrings.add(ri.getPrintData());
//System.out.println(ri.resultData);
    	}
    	return resultStrings;
    }
    public StringBuffer getDataString(int i) throws SQLException {
    	int j = 0;
    	for (ResultItem ri: allResults) {
    		j++;
    		if (i == j) {
    			return ri.getPrintData();
    		}
    	}
    	return null;
    }

	//used in treePipeline
	/** Return true if no duplicate!

        Dup detection method:
	1) Replace T?_xxx to xxx, break down on <> boundary
	2) example input: <NF_INP T1_INPID:xxx T1_Name:yyy ...><...>
	 */
	public boolean checkNoDuplicate(StringBuffer oldData, StringBuffer newData) {
	//	rtime.start();
		
//		String[] oldTuples = oldData.split("<");
		String[] oldTuples = oldData.toString().replaceAll("T\\d_","").split("<");
		String[] newTuples = newData.toString().replaceAll("T\\d_","").split("<");
//		String[] newTuples = newData.split("<");

		for (String newTuple: newTuples) {
			if (newTuple.length() > 0) {
				for (String oldTuple: oldTuples) {

					if (newTuple.length() == oldTuple.length() && newTuple.equals(oldTuple)) {
		//				rtime.stop();
						return false;

					}
				}
			}
		}
//		rtime.stop();
		return true;
	}


}


