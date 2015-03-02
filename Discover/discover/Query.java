package discover;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

class Keyword {
	String word;
	boolean isPhrase;
	boolean isStar;
	Integer[] wordId;
	GraphNode inTable;
	int group;
	protected String getStr() {
		return word;
	}
	protected Keyword(String kw, int _group) {
		word = kw;
		group = _group;
		isPhrase = false;
		isStar = false;
		inTable = null;
	}
	protected int getPrefix() {		
		return group;
	}
	protected String getPrefixStr() {
		switch (group) {
		case Global.OR_SYNTAX:
			return "";
		case Global.AND_SYNTAX:
			return "+";
		case Global.NOT_SYNTAX:
			return "-";
		}
		return "";
	}
	protected StringBuffer getInputStr() {
		StringBuffer ipt = new StringBuffer();
		if (inTable != null) {
			ipt.append(inTable.getTableName()).append(":");
		}
		ipt.append(getPrefixStr());
		if (isPhrase) ipt.append("'").append(getStr()).append("'");
		else ipt.append(getStr());
		return ipt;
	}
}
public class Query {
	// translate table name to node
	public HashMap<String, ResultSet> nonFreeSets;
	public HashMap<String, Integer> scorebound; //<"Person 12", 21>, <"Person 22", 30>
	public HashMap<String, Double> selectivity;
	public HashMap<String, Double> globalIdf;

	// remember which col matches which keyword (varying across queries). i.e., String[0] = col, [1] = keyword
	protected HashMap<String, LinkedList<Keyword>> matchedKeywords;
	protected HashMap<String, Double> maxLocalScore;
	protected HashMap<String, Double> minLocalScore;
	// <"InProceeding", {"0","1"}: NF_Inproceeding matches keywords[0] and keywords[1] 

	public int queryId;
	public String uniqueId;
	protected int todoItemNum;
	
	public ArrayList<Keyword> orWords;	//query
	//public ArrayList<Integer> keywordPrefix; //AND, OR
	public ArrayList<Keyword> notWords; //NOT
	public ArrayList<Keyword> andWords;
	public boolean hasPhrase;
	public int userCN;
	//public HashMap<Integer, Boolean> isPhrase;
	//public HashMap<Integer, int[]> wordId;		//keyword ids as in Dict table
	//public HashMap<Integer, GraphNode> wordTable; 
	
	public SolutionTree stree;
	public PriorityQueue<TodoItem> todoList;
	public Results allResults;	//query results
	public int topKAlgorithm;
	public int nonFreeRange;

	public SchemaGraph schema;
	public DBConnect dbconn;
	public Global global;
	protected ArrayList<String> tmpTableNames;

	public Query(SchemaGraph _schema) {
//System.out.println("Query:"+this);
			
		schema = _schema;
		dbconn = schema.dbconn;
		global = schema.global;

		schema.queryNum++;
		uniqueId = dbconn.uniqueId+"_"+schema.queryNum;
		todoItemNum = 0;
		
		orWords = null;
		notWords = null;
		andWords = null;
		allResults = null;
		topKAlgorithm = -1;
		userCN = -1;
		
		todoList = new PriorityQueue<TodoItem>();		
		nonFreeSets = new HashMap<String, ResultSet>();
		scorebound = new HashMap<String, Integer>();
		selectivity = new HashMap<String, Double>();
		globalIdf= new HashMap<String, Double>();
		matchedKeywords = new HashMap<String, LinkedList<Keyword>>();
		maxLocalScore = new HashMap<String, Double>();
		minLocalScore = new HashMap<String, Double>();
		tmpTableNames = new ArrayList<String>();
	}

	public boolean isTreePipeline() {
		return (topKAlgorithm == Global.TREEPIPELINE_BFS_2LEVEL);
	}
	protected boolean matchKeyword(GraphNode n, Keyword kw) {
		if (matchedKeywords.get(n.getTableName()).contains(kw)) return true;
		return false;
	}
	public LinkedList<Keyword> getMatchedKeywords(String tableName) {
		if (matchedKeywords.containsKey(tableName)) {
			return matchedKeywords.get(tableName);
		}
		return null;
	}
	public int getMatchedKeywordSize(String tableName) {
		if (matchedKeywords.containsKey(tableName)) {
			return matchedKeywords.get(tableName).size();
		}
		return 0;
	}
	public double getMaxLocalScore(GraphNode n) {
		return maxLocalScore.get(n);
	}
	public void addMatch(String tbl_name, Keyword kw) {
		if (matchedKeywords.containsKey(tbl_name)) {
			matchedKeywords.get(tbl_name).add(kw);
		} else {
			LinkedList<Keyword> k = new LinkedList<Keyword>();
			k.add(kw);
			matchedKeywords.put(tbl_name, k);
		}
	}
	public long getNonFreeTupleSize(String tableName) {
		return (long)(schema.originalSize.get(tableName) * selectivity.get(tableName));
	}

	public boolean buildIndex() throws SQLException {
		//dbconn.dropIndexTables();
		if (global.indexType == Global.COLUMN_INDEX_REBUILD || global.indexType == Global.COLUMN_INDEX_NO_REBUILD) {
			buildColIndex();
		} else { 
			return buildTupleIndex();
		}
		return true;
	}

	public void dropIndex1() throws SQLException {
		//dbconn.dropIndexTables();
		for (String tbl: nonFreeSets.keySet()) {
			//System.out.println(tbl);
			nonFreeSets.get(tbl).getStatement().close();
		}
		nonFreeSets.clear();
		scorebound.clear();
		for (GraphNode n: schema.getNodes()) {
			n.reset();
		}
	}  
	// build the index for all terms in the tables
	public void buildColIndex() throws SQLException {
		//backup in 5Sep.tar (or before)
	}
	public int getKeywordIndex(int group, int i) {
		switch (group) {
		case Global.OR_SYNTAX:
			return i;
		case Global.AND_SYNTAX:
			return orWords.size() + i;
		case Global.NOT_SYNTAX:
			return orWords.size() + andWords.size() + i;
		}
		return 0;
	}
	public int getKwNum() {
		return orWords.size() + andWords.size();
	}
	public int getTotalKwNum() {
		return orWords.size() + andWords.size() + notWords.size();
	}
	public Keyword getKeyword(int id) {
		if (id < orWords.size()) return orWords.get(id);
		if (id < getKwNum()) return andWords.get(id - orWords.size());
		return notWords.get(id - getKwNum());
	}
	protected StringBuffer genKeywordList() {
		//String kw = q.
		StringBuffer keywordList = new StringBuffer("");
		StringBuffer punct = global.SysIsOracle()?new StringBuffer(","):new StringBuffer(" ");
		int knum = getTotalKwNum();
		
		if (knum > 0) {
			for (int i=0; i < knum; i++) {
				keywordList.append(getKeyword(i).getInputStr());
				if (i < knum) keywordList.append(punct);
			}
		}
		return keywordList;
	}

	public void removeKeyword(int i) {
		if (getKeyword(i).getPrefix() == Global.OR_SYNTAX) orWords.remove(i);
		else andWords.remove(i - orWords.size());
	}
	public boolean buildTupleIndex() throws SQLException {
		//ResultSet rs;
		//int[][] df = new int[keywords.length][size()];
		int[] globaldf = new int[getKwNum()];
		double[] minSel = new double[getKwNum()];
		for (int i=0; i<getKwNum(); i++) {
			globaldf[i] = 0;
			minSel[i] = 1.0;
		}    		       
		//set selectivity of each keyword in each table
		for (GraphNode n: schema.getNodes()) {
			n.reset();
			String tbl_name = n.getTableName();
			if (schema.textCols.get(tbl_name) != null) {
				for (int i = 0; i < getKwNum(); i++) {
					int count = 0;
					ResultSet rs = null;
					if (getKeyword(i).inTable == null | getKeyword(i).inTable == n) {
	//					global.debugln(countSel.toString());
						//ResultSet rs = dbconn.executeQuery(dbconn.genCountSel(n, keywords[i]));
						if (getKeyword(i).isStar) {
							//addMatch(tbl_name, getKeyword(i));		//add match of star
							//count = schema.originalSize.get(n.getTableName());
						} else {
							rs = dbconn.executeQuery(dbconn.genCountSel(n, this, i));
							rs.next();
							count = rs.getInt(1);
							rs.close();
						}
//					~~~~~~~~~~~~~~~~~~~~~to seperate idf~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
					//df[i][n.getId()-1] = count;

						if (count > 0) {
System.out.println(tbl_name+" "+getKeyword(i).getStr()+" "+count);
							addMatch(tbl_name, getKeyword(i));
							double sel = (double) count / schema.originalSize.get(n.getTableName());
	//						if (sel<0.0002) sel=0.0002; //threshold
							selectivity.put((n.getTableName()+" "+getKeyword(i).getStr()), sel);
							minSel[i] = (minSel[i] > sel)?sel:minSel[i];
							globaldf[i] = globaldf[i] + count;
						}	
						
					}
				}
				//n.buildKeywordList();
			}  
		}
		//set globalIdf for each keyword
		LinkedList<Boolean> valid = new LinkedList<Boolean>();
		for (int i = 0; i < getKwNum(); i++) {
			 if (globaldf[i] == 0) {
				 valid.add(false);
			} else {
				valid.add(true);
				if (global.scoreFunc == 3) {
					globalIdf.put(getKeyword(i).getStr(), Math.log(1.0 * schema.TOTAL_TUPLE_NUMBER / (globaldf[i] + 1)));
				}
				else {
					globalIdf.put(getKeyword(i).getStr(), Global.DELTA * i + Math.log((1.0 * schema.TOTAL_TUPLE_NUMBER - globaldf[i]) / globaldf[i]));
				}
				if (global.trysepidf) {
					globalIdf.put(getKeyword(i).getStr(), Global.DELTA * i + Math.log(1.0 / minSel[i]));
				}
			}	
//			System.err.println(keywords[i]+"globalIdf: "+ schema.globalIdf.get(keywords[i]));
		}
		int i = 0;
		while (i < getKwNum()) {
			if (!valid.poll()) {
				removeKeyword(i);
			} else i++;			
		}
		if (getKwNum() == 0) return false;
		// then, fill in the contents
		//for each table {
		Statement stmt1 = dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
		//ResultSet rs1;
		for (GraphNode n: schema.getNodes()) {
			if (getMatchedKeywordSize(n.getTableName()) > 0) {
				String tbl_name = n.getTableName();
//				String tbl_name_q = n.getTableNameQ();
				/*
				if (!global.OWN_FULLTEXT_INDEX) {
					// step 1
					StringBuffer sqlFindNFT = dbconn.genFindNonFreeTbl(n, this);
	//				System.out.println(sqlFindNFT);
					dbconn.executeUpdates(sqlFindNFT);
					// step 2
					StringBuffer sqlGetNonFreeTbl = dbconn.genGetNonFreeTbl(n);
					rs1 = stmt1.executeQuery(sqlGetNonFreeTbl.toString());
					fillContents(n, rs1);
					rs1.close();
					//step 3
					StringBuffer sqlCreateNFT = dbconn.genCreateNonFreeTbl(n, keywords);
	//				System.out.println(sqlCreateNFT);
					dbconn.executeUpdates(sqlCreateNFT);
				} else { */
				StringBuffer sqlCreateNFT = dbconn.genCreateNonFreeSetWithII(n, this);
				dbconn.executeUpdates(sqlCreateNFT);
				//}
				
				
				//set in-momory non-free sets and related statistics 
				Statement nonfreestmt = dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
				String sqlNonFreeSet = dbconn.genNonFreeSet(n, this);
				ResultSet rs = nonfreestmt.executeQuery(sqlNonFreeSet);

				boolean notEmpty = rs.next();
				if (notEmpty) {
					maxLocalScore.put(tbl_name, rs.getDouble("Score"));		//the largest score
					rs.last();
					minLocalScore.put(tbl_name, rs.getDouble("Score"));			//the minimal score
					//set non-free tupleset size
					//rs.last();
					int size = rs.getRow();
					selectivity.put(tbl_name, new Double((double)size / schema.originalSize.get(tbl_name)));
					nonFreeSets.put(tbl_name, rs);
//					dbconn.findAllCols(tbl_name_q);
global.debugln(n.getTableName() + ": " + size+" ("+maxLocalScore.get(tbl_name) + " ~ " + 
							minLocalScore.get(tbl_name)+")");
				} else {
					selectivity.put(tbl_name, 0.0);
				}
			}
		}
		stmt1.close();
		return true;
	}

	protected void fillContents(GraphNode n, ResultSet rs) throws SQLException {  
		String[] textCol = schema.textCols.get(n.getTableName()).split(",");
		int kwNum = getKwNum();
		while (rs.next()) {
			double score1 = 0.0;
			double score2 = 0.0;
			double score3 = 0.0;
			StringBuffer dtfs =new StringBuffer("");
			StringBuffer dtfsosix = new StringBuffer("");
			StringBuffer colBuffer = new StringBuffer("");
			for (String tc: textCol) {
				colBuffer = colBuffer.append(rs.getString(tc)).append(" ");
			}
			String col = colBuffer.toString();
			col = Global.cleanData(col);

			int totalwordcount = col.split("  ").length;
			for (int i = 0; i < kwNum; i++) {
				String oriKeyword = getKeyword(i).getStr();
				String keyword = " " + oriKeyword.toLowerCase() + " ";
				int length = col.length();
				col = col.replaceAll(keyword, "");
				int wordcount = (length - col.length()) / keyword.length();			//count of keyword i

				double loglogdtf = 0;
				if (wordcount > 0) loglogdtf = Math.log(Math.log(wordcount) + 1) + 1;		//normalized count

				if (global.scoreFunc == 1) {							//vldb 03 function
					//if (schema.avgDl.containsKey(n.getTableName())) {
					if (selectivity.containsKey(n.getTableName() + " " + oriKeyword)) {
						double avgDl = schema.avgDl.get(n.getTableName());
						double sel = selectivity.get(n.getTableName() + " " + oriKeyword);
						score1 = score1 + loglogdtf / (0.8 + 0.2 * totalwordcount / avgDl) * Math.log(1.0 / sel);
					}
				}

				if (global.scoreFunc == 2) {
					score2 = score2 + 1.0 * wordcount * globalIdf.get(oriKeyword);	//watf*sumidf: sum(tf * idf)
					dtfs = dtfs.append(wordcount).append(" ");					//a string of dtfs: "1 1 0"
				}
				if (global.scoreFunc == 3) {
					double avgDl = schema.avgDl.get(n.getTableName());				//sigmod06 function
					double kwscore = loglogdtf * globalIdf.get(oriKeyword) / ((0.8 + 0.2 * totalwordcount / avgDl) * (1.0 + Math.log(avgDl)));
					score3 = score3 + kwscore;							//sum(ndtf * nidf / ndl)
					String kwscorestring = String.valueOf(kwscore);
					if (kwscorestring.length() > 7) kwscorestring = kwscorestring.substring(0,6);
					dtfsosix.append(kwscorestring).append(" ");					//a string of ndtf*nidf/ndl of each keywords
//					if (kwscore>0) {
//					System.err.println(wordcount+"  "+oriKeyword+" inside, loglogdtf:"+loglogdtf+" Idf: "+schema.globalIdf.get(oriKeyword)+" avgDl:"+avgDl+"totalwordcount: "+totalwordcount+" score:"+kwscore);
//					}

				}
			}

			if (global.scoreFunc == 1) {
				rs.updateDouble("Score", score1);
			}
			if (global.scoreFunc == 2) {
				rs.updateDouble("Score", score2);
				//dtfs.append(totalwordcount);
				rs.updateInt("dl", totalwordcount);
				rs.updateString("dtfs",dtfs.toString());

			}
			if (global.scoreFunc == 3) {
				rs.updateDouble("Score", score3);
				rs.updateString("dtfsosix", dtfsosix.toString());
			}
			rs.updateRow();
		}
	}

	private boolean parseKeywords(String in) throws SQLException {
System.out.print("Parsing... keywords \""+in);		
		in = in.replaceAll("\"","'") + " ";
		notWords = new ArrayList<Keyword>();
		orWords = new ArrayList<Keyword>();
		andWords = new ArrayList<Keyword>();
		hasPhrase = false;
		
		int wordLen;
		String word = "";
		int prefix = Global.OR_SYNTAX;
		boolean isPs = false;
		String tableName = null;
		while (in.length() > 0) {
			char c = in.charAt(0);
			wordLen = 0;
			boolean addNewWord = false;
			switch (c) { 
			case '-':			
					prefix = Global.NOT_SYNTAX;
					break;
				case '+':
					prefix = Global.AND_SYNTAX;
					break;
				case ' ':
					prefix = Global.OR_SYNTAX;
					tableName = null;
					break;
				case '\'':
					wordLen = in.indexOf("'", 1);
					if (wordLen <= 0) return false;
					word = in.substring(1, wordLen);
					isPs = true;
					addNewWord = true;
					break;
				case '@':
					wordLen = in.indexOf(" ") - 1;
					userCN = Integer.valueOf(in.substring(1, wordLen + 1));
					
					break;
				default:
					wordLen = in.indexOf(" ") - 1;
					word = in.substring(0, wordLen + 1);
					int sep = word.indexOf(":");
					if (sep >= 0) {
						wordLen = sep;
						tableName = word.substring(0, sep);	
						//word = word.substring(sep + 2, word.length());
					} else {
						if (word.matches(".*\\p{Punct}.*")) isPs = true;
						addNewWord = true;
					}
					
			}
			/*if (addNewWord){
				String cw = Global.cleanData(word).trim();
				if (cw.length() <= 0)
					addNewWord = false;
				else {
					if (isPs) {
						hasPhrase = true;
					}
				}
			}*/
			if (addNewWord) {
				Keyword k = new Keyword(word, prefix);

				k.isPhrase = isPs;
				if (tableName != null) {
					k.inTable = schema.tableNameDict.get(tableName);
				}
				switch (prefix) {
				case Global.NOT_SYNTAX:
					notWords.add(k);
					break;
				case Global.AND_SYNTAX:
					andWords.add(k);
					break;
				case Global.OR_SYNTAX:
					orWords.add(k);
					break;
				}
			}
			in = in.substring(wordLen+1, in.length());
	
			//}
		}
		if (global.OWN_FULLTEXT_INDEX) {
			//wordId = new HashMap<Integer, int[]>();
			for (int i = 0 ; i < getKwNum(); i++) {
				
				if (getKeyword(i).getStr().equals("*")) {
					getKeyword(i).wordId = new Integer[1];
					getKeyword(i).wordId[0] = -1;
					getKeyword(i).isStar = true;
					getKeyword(i).inTable.matchStar = true;
				} else {
					ArrayList<Integer> idsList = new ArrayList<Integer>();
					String phrase;
					phrase = Global.cleanData(getKeyword(i).getStr()).trim();
					if (phrase.length() > 0) {
						String[] kws = phrase.split("  ");
						//int[] ids = new int[kws.length];
						 
						for (int j = 0; j < kws.length; j++) {							
						
							StringBuffer sqlFindWordId = dbconn.genFindWordId(kws[j]);
//System.out.println(sqlFindWordId);						
							ResultSet rs = dbconn.executeQuery(sqlFindWordId.toString());
							if (rs.next() ) {
								idsList.add(rs.getInt("WordId"));
							}
//System.out.println(kws[j]+":"+rs.getInt("WordId"));							
						}
					}				
					if (idsList.size() > 0)						
						getKeyword(i).wordId = idsList.toArray(new Integer[idsList.size()]);
					else phrase = "";
				
					if (phrase.length() == 0) {
						removeKeyword(i);
					}
				}
			}
		
		}
		if (getKwNum() == 0) {
			System.out.println("\" is invalid.");
			return false;
		}
		System.out.println("\" is valid.");
		return true;
	}

	public void queryProcessing(int _queryId, String _keywords) throws SQLException, IOException, Exception{
//for java version: multiple query, multiple algorithms, return all results
		allResults = null;
		if (!parseKeywords(_keywords)) {  //if query is invalid 
			return;
		}
		queryId = _queryId;
		Time preProcessTime = new Time();
		preProcessTime.start();
		System.out.println("==================================================\n");
		System.err.println("Keywords=" + _keywords + "  k="+global.k+" alpha="+global.ALPHA);
		System.out.println("Keywords=" + _keywords + "  k="+global.k+" alpha="+global.ALPHA);

		dbconn.resetConn();		
		//dropIndex();

		if (!buildIndex()) {
			return;
		}
		
		stree = new SolutionTree(this);
		if (!global.OFFLINE_CN_SEARCH) {				
			stree.generateCNG();
		} else {
			stree.scanCNG(nonFreeSets.keySet());
		}
		
		if (!global.ADAPTIVE_EP) {
			stree.initEP();
		}
		preProcessTime.stop();
		System.out.println("PreProcessing Time: "+preProcessTime.getTime());
		boolean inTimeLimit = true;
		for (int curAlg: global.algs) {
			if (curAlg != topKAlgorithm || inTimeLimit) {

				topKAlgorithm = curAlg;
								
				System.err.println("Running "+topKAlgorithm+"...");
				allResults = new Results(this);
				//sg.scorebound.clear();
				//Time time = new Time();
				allResults.preProcessTime = preProcessTime;
				//time.start();
				todoList = stree.getInitTodoList();
				for (int curK = 1; curK <= global.k; curK++) {
					switch (topKAlgorithm) {
	
					case Global.SPARSE:
						inTimeLimit = sparse(curK);
						break;
					case Global.GPIPELINE:
						inTimeLimit = globalPipeline(curK); 
						break;
					case Global.QPIPELINE_SBS:	
						inTimeLimit = queuePipeline(curK);
						break;
					case Global.QPIPELINE_2LEVEL:	
						inTimeLimit = queuePipeline_2level(curK);
						break;
					case Global.TREEPIPELINE_BFS_2LEVEL:
						//if (!global.CREATE_TEMP_TABLES) global.ownJoin = true;
						inTimeLimit = treePipeline_BFS_2Level(curK);
						break;
					}
				}
				allResults.joinTime.stop();
				
				StringBuffer statistics = new StringBuffer();
				statistics.append("STATISTICS Query=").append(queryId)
				.append(" Alg=").append(topKAlgorithm).append(" CNNum=").append(stree.size())
				.append(" JoinTime=").append(allResults.joinTime.getTime())
				.append(" SQLTime=").append(allResults.SQLTime.getTime())
				.append(" PreProcessTime=").append(allResults.preProcessTime.getTime())
				.append(" QNum=").append(allResults.queryCount)
				.append(" QSize=").append(allResults.querySize)
				.append(" Pruned=").append(allResults.pruned)
				.append(" TopKScore=").append(allResults.minScore).append("\n");
				
				System.err.println(statistics);
				System.out.println(statistics);

				closeTodoList(todoList);
//				dbconn.closeTables(tmpTableNames);
//				tmpTableNames.clear();
			}
		}

		stree.closeQuery();
		dbconn.closeTables(tmpTableNames);
		tmpTableNames.clear();
		//System.out.println(allResults.size());
//		if (checkIdfErr) System.out.println("Average IdfError: "+idfError / idfErrorCount);
	}

	public boolean queryProcessing(int _queryId, String _keywords, int curK) throws SQLException, IOException, Exception{
//		for web version: one query, one algorithm, one result
		if (curK == 1) {
			allResults = null;
			if (!parseKeywords(_keywords)) {  //if query is invalid 
				return false;
			}
			queryId = _queryId;
			Time preProcessTime = new Time();
			preProcessTime.start();
			System.out.println("==================================================\n");
			System.err.println("Keywords=" + _keywords + "  k="+global.k+" alpha="+global.ALPHA);
			System.out.println("Keywords=" + _keywords + "  k="+global.k+" alpha="+global.ALPHA);
	
			dbconn.resetConn();		
			//dropIndex();
	
			if (!buildIndex()) {
				return false;
			}
			
			stree = new SolutionTree(this);
			if (!global.OFFLINE_CN_SEARCH) {				
				stree.generateCNG();
			} else {
				stree.scanCNG(nonFreeSets.keySet());
			}
			
			if (!global.ADAPTIVE_EP) {
				stree.initEP();
			}
			preProcessTime.stop();
			System.out.println("PreProcessing Time: "+preProcessTime.getTime());
		
			topKAlgorithm = global.algs[0];

			System.err.println("Running "+topKAlgorithm+"...");
			allResults = new Results(this);
				//sg.scorebound.clear();
				//Time time = new Time();
			allResults.preProcessTime = preProcessTime;
				//time.start();

			todoList = stree.getInitTodoList();
			
		}
		boolean getResult = false;
//System.err.println("HERE"+topKAlgorithm);				
		
		switch (topKAlgorithm) {
			case Global.SPARSE:
				getResult = sparse(curK);
				break;
			case Global.GPIPELINE:
				getResult = globalPipeline(curK); 
				break;
			case Global.QPIPELINE_SBS:	
				getResult = queuePipeline(curK);
				break;
			case Global.QPIPELINE_2LEVEL:	
				getResult = queuePipeline_2level(curK);
				break;
			case Global.TREEPIPELINE_BFS_2LEVEL:
				//if (!global.CREATE_TEMP_TABLES) global.ownJoin = true;
				getResult = treePipeline_BFS_2Level(curK);				
				break;
		}
		if (curK == global.k | !getResult) {
				allResults.joinTime.stop();
				
				StringBuffer statistics = new StringBuffer();
				statistics.append("STATISTICS Query=").append(queryId)
				.append(" Alg=").append(topKAlgorithm).append(" CNNum=").append(stree.size())
				.append(" JoinTime=").append(allResults.joinTime.getTime())
				.append(" SQLTime=").append(allResults.SQLTime.getTime())
				.append(" PreProcessTime=").append(allResults.preProcessTime.getTime())
				.append(" QNum=").append(allResults.queryCount)
				.append(" QSize=").append(allResults.querySize)
				.append(" Pruned=").append(allResults.pruned)
				.append(" TopKScore=").append(allResults.minScore).append("\n");
				
				System.err.println(statistics);
				System.out.println(statistics);

				closeTodoList(todoList);
//				dbconn.closeTables(tmpTableNames);
//				tmpTableNames.clear();
		
				stree.closeQuery();
				dbconn.closeTables(tmpTableNames);
				tmpTableNames.clear();
		}	
		return getResult;
		//System.out.println(allResults.size());
//		if (checkIdfErr) System.out.println("Average IdfError: "+idfError / idfErrorCount);
	}
	
	protected void closeTodoList(PriorityQueue<TodoItem> todoList) throws SQLException {
		while (!todoList.isEmpty()) {
			TodoItem todo = todoList.poll();
			todo.close();
		}
	}
	// return the number of result (i.e., # of instance of join trees)
	public boolean sparse(int curK) throws SQLException {
		ResultSet rs;
		boolean stopSearch = false;
		while (!stopSearch && todoList.size()!=0) {
			if (allResults.printed >= curK) return true;

			allResults.queryCount++;
			TodoItem todo = todoList.poll();
			if (!global.trysepidf) {
//				System.out.println("HERE"+todo.possibleMax+"  "+allResults.minScore+"     "+allResults.size());				
				allResults.dump(todo.possibleMax);
			}	
//			debugln(todo.possibleMax + " " + todo.getSolution().keyString);
			if (!global.trysepidf && todo.possibleMax <= allResults.getMinScore() && allResults.full()) {
				stopSearch = true;
				break;
			}	
			// try to get its result by sending sql to the db

			if (global.checkIdfErr && todo.size() == 3) {
				todo.sol.computeEstErr(allResults);
			}

			StringBuffer sql = dbconn.getPreparedSQLStatement(todo);

			if (sql != null) {
				allResults.SQLTime.start();
//System.out.println("==> SQL is: " + sql.toString() + "\n");				
				//Statement stmt = dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
				//stmt.setMaxRows(global.MAX_ROW);
				rs = schema.dbconn.executeQuery(sql.toString());
				allResults.SQLTime.stop();

				long qs = 1;
				for (GraphNode n: todo.getNodes()) {
					if (n.isNonFree()) 
						qs = qs * (long)(schema.originalSize.get(n.getTableName()) * selectivity.get(n.getTableName()));
				}
				allResults.querySize = allResults.querySize + qs;

				allResults.addResultSet(rs, todo);
				//stmt.close();
			}    
			//} // END: process a valid solution
			if (allResults.SQLTime.getTime() > global.TIMEOUT) {
				System.err.println("TIMEOUT "+global.TIMEOUT);
				System.out.println("TIMEOUT "+global.TIMEOUT);
				return false;
			}
		}
		if (global.trysepidf || todoList.size()==0) allResults.dump(0);
		if (allResults.printed >= curK) return true;

		if (global.checkIdfErr) 
			System.out.println("IdfError: Q"+queryId+" "+allResults.idfError/allResults.idfErrorCount);
		return false;
	}

	public boolean globalPipeline(int curK) throws SQLException {
		ResultSet rs; 
		boolean stopSearch = false;

		while (!stopSearch && todoList.size() != 0) {
			if (allResults.printed >= curK) return true;

			TodoItem todo = todoList.poll();
			allResults.dump(todo.possibleMax);

//			System.out.println(todo.possibleMax + " MinK: "+ allResults.getMinScore() + " SIZE: "+todoList.size());
			if (todo.possibleMax <= allResults.getMinScore() && allResults.full()) {
				stopSearch = true;
				break;
			}
			if (todo.sol.stmt == null) {
				todo.sol.setStatement(dbconn.getPreparedSQLStatement(todo).toString());
			}
			allResults.querySize = allResults.querySize + todo.setPara(topKAlgorithm);
//			System.out.println("==> SQL " + allResults.queryCount + " is: " + todo.sol.stmtStr + "\n");
			allResults.SQLTime.start();
			rs = todo.sol.stmt.executeQuery();
			allResults.SQLTime.stop();

			allResults.queryCount++;
			allResults.addResultSet(rs,todo);
			rs.close();
			GraphNode chosenNode = null;
			for (GraphNode n: todo.curCur.keySet()) {
				if (todo.curCur.get(n) != 0) {
					chosenNode = n;
					boolean ret = todo.update(n);
					if (ret) todoList.add(todo);
					break;
				}
			}	 
			//at the first point   
			if (chosenNode == null) {
				for (GraphNode n: todo.curCur.keySet()) {
					TodoItem newTodo = new TodoItem(todo);
					boolean ret = newTodo.update(n);
					if (ret) todoList.add(newTodo);
				}
			}
			todo.close();
			//System.out.println("QR" + queryCount + " A"+topKAlgorithm + " uTime:" + elapsedTime);
			if (allResults.SQLTime.getTime() > global.TIMEOUT) {
				System.err.println("TIMEOUT "+global.TIMEOUT);
				System.out.println("TIMEOUT "+global.TIMEOUT);
				return false;
			}

		}
		if (todoList.size()==0) allResults.dump(0);
		if (allResults.printed >= curK) return true;

		return false;
	}
	

	public boolean queuePipeline(int curK) throws SQLException {
		ResultSet rs;
		boolean stopSearch = false;

		while (!stopSearch && todoList.size() != 0) {
			if (allResults.printed >= curK) return true;

			TodoItem todo = todoList.poll();
//			System.out.println(todo.sol.keyString + todo.curCur.size());
			allResults.dump(todo.possibleMax);
			//System.out.println(todo.possibleMax + " MinK: "+ allResults.getMinScore() + " SIZE: "+todoList.size());
			if (todo.possibleMax <= allResults.getMinScore() && allResults.full()) {
				stopSearch = true;
				break;
			}

			if (todo.sol.stmt == null) {
				todo.sol.setStatement(dbconn.getPreparedSQLStatement(todo).toString());
			}
			long qSize = todo.setPara(topKAlgorithm);

			allResults.querySize = allResults.querySize + qSize;
//System.out.println("==> SQL " + todo.sol.stmtStr + "\n");
			allResults.SQLTime.start();
			//rs = todo.sol.stmt.executeQuery();
			rs = todo.sol.stmt.executeQuery();
			allResults.SQLTime.stop();
			allResults.queryCount++;
			allResults.addResultSet(rs,todo);
			rs.close();
			for (GraphNode n: todo.getSolution().getNodes()) {
				if (n.isNonFree()) {

					TodoItem newTodo = new TodoItem(todo);
					boolean ret = false;
					ret = newTodo.update(n);
//					System.out.println("QR" + queryCount + " A"+topKAlgorithm + " updateTime:" + elapsedTime);
					if (ret) {
						todoList.add(newTodo);
					} 	
					if (!todo.curCur.get(n).equals(0))   //no duplication!
						break;
				}
			}   
			todo.close();
			if (allResults.SQLTime.getTime() > global.TIMEOUT) {
				System.err.println("TIMEOUT "+global.TIMEOUT);
				System.out.println("TIMEOUT "+global.TIMEOUT);
				return false;
			}
		}	    
		if (todoList.size()==0) allResults.dump(0);
		if (allResults.printed >= curK) return true;

		return false;
	}

//	with two upper bound
	public boolean queuePipeline_2level(int curK) throws SQLException {
		ResultSet rs;
		boolean stopSearch = false;
		
		while (!stopSearch && todoList.size() != 0) {
			if (allResults.printed >= curK) return true;

			TodoItem todo = todoList.poll();
			allResults.dump(todo.possibleMax);
//			System.out.println(todo.possibleMax + " MinK: "+ todo.sol.keyString+" "+todo.getCurCurString());
			if (todo.possibleMax <= allResults.getMinScore() && allResults.full()) {
				stopSearch = true;
				break;
			}

			if (todo.sol.stmt == null) {
				todo.sol.setStatement(dbconn.getPreparedSQLStatement(todo).toString());
System.out.println("==> SQL is: " + dbconn.getPreparedSQLStatement(todo).toString() + "\n");
			}
			//todo.setStatement(null);
			long qSize = todo.setPara(topKAlgorithm);
			if (todo.checkReal()) {
//System.out.println(allResults.queryCount+" "+todo);
				allResults.querySize = allResults.querySize + qSize;
				allResults.SQLTime.start();
				rs = todo.sol.stmt.executeQuery();
//System.out.println(Global.resultSetSize(rs));				
				allResults.SQLTime.stop();
				allResults.queryCount++;
				allResults.addResultSet(rs,todo);
				rs.close();

			} else {

				for (GraphNode n: todo.getSolution().getNodes()) {
					if (n.isNonFree()) {

						TodoItem newTodo = new TodoItem(todo);
						boolean ret = false;
						ret = newTodo.update(n);

//						System.out.println("QR" + queryCount + " A"+topKAlgorithm + " updateTime:" + elapsedTime);
						if (ret) {
							todoList.add(newTodo);
						}  	
						if (!todo.curCur.get(n).equals(0))   //no duplication!
							break;
					}
				}
				todo.setReal(allResults);
//				System.out.println(todo.possibleMax);
				todoList.add(todo);
			}
			todo.close();
			if (allResults.SQLTime.getTime() > global.TIMEOUT) {
				System.err.println("TIMEOUT "+allResults.SQLTime.getTime());
				System.out.println("TIMEOUT "+allResults.SQLTime.getTime());
				return false;
			}

		}	    
		if (todoList.size()==0) allResults.dump(0);
		if (allResults.printed >= curK) return true;
		return false;
	}

	public boolean treePipeline_BFS_2Level(int curK) throws SQLException {
		boolean stopSearch = false;
//System.out.println("call treePipeline:"+todoList.size());
		while (!stopSearch && todoList.size() != 0) {
			TodoItem todo = todoList.peek();
			allResults.dump(todo.possibleMax);
			if (allResults.printed >= curK) return true;
			if (todo.possibleMax < 0 || todo.possibleMax <= allResults.getMinScore() && allResults.full()) {
				stopSearch = true;
				break;
			}
			if (todo.checkReal()) {
				//add all todoItems of this run
				ArrayList<SolutionEdge> partition = new ArrayList<SolutionEdge>();
				Stack<TodoItem> curTodoList = null;
				if (global.ADAPTIVE_EP) {
					partition = todo.sol.getShortestPath(todo.getCurCurList());
				} else {
					partition = todo.sol.getStaticPath(todo.getCurCurList());
				}
				boolean pruned = false;
				if (partition == null) {
					todoList.remove(todo);
					allResults.pruned ++;
					pruned = true;
				} else {
					curTodoList = todo.getTodoItems(partition, todoList);
//System.out.println("a run" + curTodoList.size());
				}
				while (curTodoList != null && !curTodoList.empty()) {
					TodoItem curTodo = curTodoList.pop();
					if (!curTodo.checkReal()) {
						curTodo.addNeighbors(todoList);
					}
					todoList.remove(curTodo);

System.out.println(curTodo);
					curTodo.pResult = new PartialResult(curTodo);		//mark as executed

					if (!pruned) {
						allResults.querySize += curTodo.executeSQL(allResults);
						if (curTodo.pResult.isEmpty()) {
							pruned = true;
						}	
					} else {
						allResults.pruned ++;
					}						

					//clean useless partial results
					//		curTodo.unLinkToEpTodo();

					//		if (curTodo.epTodo != null && curTodo.epTodo.sol.todoMap.size() == curTodo.epTodo.sol.children.size() && curTodo.epTodo.children.isEmpty()) {
//					System.out.println("close "+curTodo.epTodo.sol.keyString+"  "+curTodo.epTodo.getCurCurVal());						
					//			curTodo.epTodo.close();				//after running current todoItem, the epTodo can be closed
					//		}
					//		if (curTodo.children.isEmpty()) {
//					System.out.println("close "+curTodo.sol.keyString+"  "+curTodo.getCurCurVal());						
					//			curTodo.close();
					//		}

				}
//				System.out.println();

			} else {	//checkReal() failed
				todoList.poll();
				todo.addNeighbors(todoList);
				todo.setReal(allResults);
				todoList.add(todo);
				
			}
		}
		allResults.dump(0);
		if (allResults.printed >= curK) return true;
		return false;
	}	
}


