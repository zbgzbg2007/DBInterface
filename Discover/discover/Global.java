package discover;  

import java.io.BufferedReader;
import java.sql.*;
//import java.math.*;
import java.io.*;

class Time{
	long startTime;
	long stopTime;
	long elapsedTime;
	private boolean ticking;

	public Time() {
		elapsedTime = 0;
		ticking = false;
	}

	public void start() throws SecurityException{
		if (ticking) {
			throw new SecurityException("Clock is ticking.");
		}
		startTime = System.currentTimeMillis();
		ticking = true;
	}
	public void stop() {
		if (!ticking) {
			throw new SecurityException("Clocking is not ticking.");
		}
		stopTime = System.currentTimeMillis();
		elapsedTime = elapsedTime + stopTime - startTime;

		ticking = false;
	}
	public void stopNPrint() {
		if (!ticking) {
			throw new SecurityException("Clocking is not ticking.");
		}
		stopTime = System.currentTimeMillis();
		System.out.println(stopTime-startTime);
		elapsedTime = elapsedTime + stopTime - startTime;

		ticking = false;
	}
	public long getTime() {

		return elapsedTime;

	}
}
public class Global {

	final boolean debug = true;
	final static double DELTA = 0.0000000000001; //for comparasion of float numbers
	final int MAX_ROW = 10000; //otherwise, java out of memory sometimes...
	public final static int MIN_WORD_LEN = 2;
	
	public final static String ORACLE = "ORACLE";
	public final static String MYSQL = "MYSQL";
	public final static String DBLP = "DBLP";
	public final static String DBLPF = "dblp_full";
	public final static String MONDIAL = "Mondial";
	public final static String IMDB = "imdb";
	public final static String NORTHWIND = "northwind";

	String RDMS;
	Boolean SCORE_RANGE;
	int SCORE_MAX_OFFSET;
	long TIMEOUT;
	public int k;

	String dataSet;
	String server;
	String username;
	String password;

	int scoreFunc;
	public double ALPHA;    // p in our score, used in incomplete penalty default=1
	double BETA; // 1/7=0.14286 default=0.15
	public boolean OFFLINE_CN_SEARCH;
	boolean OWN_FULLTEXT_INDEX;
	int searchRange;    //default=6
	int INITIAL_STEP;
	public int[] algs;

	boolean checkIdfErr;	//check Idf Estimation error; work on mysql and alg={1}
	boolean checkScoreb;	//true: the scoreb function in the paper; false: without tf and idf normalization
	boolean trysepidf;		//idf: local(true) or global(false)
//	-----for tree pipeline-----------------------------------------------------
	boolean ADAPTIVE_EP;					//EPs are found adaptively (nearest path)
	//public boolean ownJoin;			//always false
	int PARAMETER_NUM;						//number of ? in preparedstatment for tree pipeline
	//public boolean EXPAND_ASS_ARR;		//expand association array so that each tuple correspond to a JTT, or not
	//public boolean CREATE_TEMP_TABLES;	//
	//	---------------------------------------------------------------------------
	public Global() { //all default
		//RDMS = ORACLE;
		SCORE_RANGE = true;
		SCORE_MAX_OFFSET = 1000000;
		TIMEOUT = 60000000;
		k=10;

		/*For mondial data:
	    1. currently works on mysql only, because we need to build fulltext index on multiple columns
	    2. searchRange can only be 5, because schema is too complex and will produce too many CNs
		 */

		dataSet = DBLP;
		//dataSet = MONDIAL;
		RDMS = MYSQL;
		server = null;
		username = null;
		password = null;

		scoreFunc = 2;
		//scoreFunc=1: sum(ndtf*nidf)/size --ndtf=log(log(dtf)+1)+1; nidf=df, algs={1}
		//scoreFunc=2: our function
		//scoreFunc=3: sigmod 2006 function, algs={1}(sparse)
		//scoreFunc=4: AND

		//ALPHA, BETA are only used for function 2
		ALPHA = 2;    // p in our score, used in incomplete penalty default=1
		BETA = 0.15; // 1/7=0.14286 default=0.15

		OFFLINE_CN_SEARCH = true;
		OWN_FULLTEXT_INDEX = true;		//inverted index as extra tables
		//OFFLINE_CN_SEARCH = false;
		searchRange = 4;    //default=6
		INITIAL_STEP = 1;

		/*algs = new int[12];	//all algorithms to run
		algs[0] = 1;
		algs[1] = 1;
		algs[2] = 1;
		algs[3] = 2;
		algs[4] = 2;
		algs[5] = 2;
		algs[6] = 5;
		algs[7] = 5;
		algs[8] = 5;
		algs[9] = 7;
		algs[10] = 7;
		algs[11] = 7;*/

		checkIdfErr = false;   //check Idf Estimation error; work on mysql and alg={1}
		checkScoreb = false;    //true: incompleteness fator as in paper; false: without tf and idf normalization
		trysepidf = false;	    //idf: local(true) or global(false)

//		-----for tree pipeline-----------------------------------------------------
		ADAPTIVE_EP = true;
		//ownJoin = false;					//true: use "?"; false: create temp tables 
		PARAMETER_NUM = 1000;			//number of ? in preparedstatment for tree pipeline
		//EXPAND_ASS_ARR = false;		//expired; expand association array so that each tuple correspond to a JTT, or not
		//CREATE_TEMP_TABLES = true;	//expired; if true, tieNode of a TPSolution is in its _compNodes set; otherwise not.
//		---------------------------------------------------------------------------
	}
	final static int NON_FREE_NODE = -2;
	final static int TIE_NODE = -3;

	final static int COLUMN_INDEX_REBUILD = 0;
	final static int COLUMN_INDEX_NO_REBUILD = 1;
	final static int TUPLE_RS_WITH_TBLS = 2;
	final static int TUPLE_RS = 3;
	final int indexType = TUPLE_RS_WITH_TBLS;

	public boolean SysIsOracle() {
		return RDMS.equals(ORACLE);
	}
	// constant
	final static String COLUMN_INDEX_TABLE_NAME = "KSINDEX";
	final static String NON_FREE_TABLE_NAME = "_NF";
	final static String TMP_TABLE_NAME = "_TMP";
	public final static String INDEX_TABLE_NAME = "_INDX";

	public void setAttributes(BufferedReader br) throws IOException {
		String line;
		while (!(line = br.readLine().trim()).equals("")) {
			String[] s = line.split("=");
			String data = null;
			if (s.length == 2) data = s[1].trim();
			else data = "";
			setAttribute(s[0].toUpperCase().trim(), data);
		}
	}
	public void setAttribute(String par, String data) {
//System.out.println("SETATT"+par+data);		
		if (par.compareTo("K") == 0) {
			k = Integer.parseInt(data);
		} else if (par.compareTo("DATASET") == 0) {
			dataSet = data;
			server = null;
		} else if (par.compareTo("OFFLINE_CN_SEARCH") == 0) {
			if (data.equals("false"))
				OFFLINE_CN_SEARCH = false;
			else
				OFFLINE_CN_SEARCH = true;
		} else if (par.compareTo("SERVER") == 0) {
			server = data;
		} else if (par.compareTo("USERNAME") == 0) {
			username = data;
		} else if (par.compareTo("PASSWORD") == 0) {
			password = data;
		} else if (par.compareTo("RDMS") == 0) {
			RDMS = data;
		} else if (par.compareTo("ALGS") == 0) {
			if (data.length() > 0) {
				String[] algsString = data.split(",");
				algs = new int[algsString.length];
				for (int i=0; i<algs.length; i++) {
					algs[i] = Integer.parseInt(algsString[i]);
				}
			}
		} else if (par.compareTo("ALPHA") == 0) {
			ALPHA = Double.parseDouble(data);
		} else if (par.compareTo("K") == 0) {
			k = Integer.parseInt(data);
		} else if (par.compareTo("MAXCN") == 0) {
			searchRange = Integer.parseInt(data);
		}
	}

	public String getAttribute(String par) {
		if (par.compareTo("K") == 0) {
			System.out.println("K="+k);
			return ""+k;
		}
		if (par.compareTo("DATASET") == 0) {
			return dataSet;
		}
		if (par.compareTo("ALPHA") == 0) {
			return String.valueOf(ALPHA);
		}
		if (par.compareTo("ALG") == 0) {
			return String.valueOf(algs[0]);
		}
		if (par.compareTo("MAXCN") == 0) {
			return String.valueOf(searchRange);
		}
		return "";
	}

	protected void debug(Object msg) {
		if (debug) {
			System.out.print(msg);
		}
	}
	protected void debugln(Object msg) {
		debug(msg + "\n");
	}
	protected static int resultSetSize(ResultSet r) throws SQLException{
		int size = 0;
		r.beforeFirst();
		if (r.next()) {
			r.last();
			size = r.getRow();
			r.beforeFirst();
		}
		return size;
	}
	static final int AND_SYNTAX = 1;
	static final int OR_SYNTAX = 0;
	static final int NOT_SYNTAX = -1;

	static final int SPARSE = 1;			//benchmark
	final static int GPIPELINE = 2;		//benchmark
//	final static int QPIPELINE = 3;		//not used, qpipeline + exp
//	final static int GPIPELINE_EXP = 4;  //not used, gpipeline + exp
	final static int QPIPELINE_SBS = 5;	//qpipeline with 1 upper bound
	final static int QPIPELINE_2LEVEL = 7;	//qpipeline with 2 upper bounds
//	final static int TREEPIPELINE_DFS = 8;		
//	final static int TREEPIPELINE_BFS = 9;	
	final static int TREEPIPELINE_BFS_2LEVEL = 10;

	
//-------------------------------DBLP---------------------------------
	final static String[][] DBLP_AVGDL = {//average length of text attributes in relations
		{"tbl_film", "48.4840"},
		{"tbl_film_genre", "3.2633"}};

	final static String[][] DBLP_REFS = {//foreign key between relations
		{"tbl_film",	"id",			"tbl_film_film_genre",			"film"},
		{"tbl_film_genre",	"id",			"tbl_film_film_genre",			"genre"}};

	final static String[][] DBLP_PKEYS = {//primary keys
		{"tbl_film", "id"},
		{"tbl_film_film_genre", "id"},
		{"tbl_film_genre", "id"}};

	final static String[][] DBLP_TEXTCOLS = {// text columns
		{"tbl_film", "name,description"},
		{"tbl_film_genre", "name,description"} };

	final static String[][] DBLP_KEYS = {	//foreign keys; need to be indexed
		{"tbl_film", ""},
		{"tbl_film_film_genre", "film,genre"},
		{"tbl_film_genre", ""} };

//	-------------------------------DBLP_FULL---------------------------------
	final static String[][] DBLPF_AVGDL = {
		{"Item", "8.53"},
		{"Person", "2.10"},
		{"Publisher", "3.80"},
		{"Series", "4.24"} };

	final static String[][] DBLPF_REFS = {
		{"Publisher",	"PublisherId",	"Item",			"PublisherId"},
		{"Series",	"SeriesId",			"Item",			"SeriesId"},
		{"Item",	"ItemId",			"Item",			"CrossRef"},
		{"Person",	"PersonId",			"RelationPersonItem",	"PersonId"},
		{"Item",	"ItemId",			"RelationPersonItem",	"ItemId"},
		{"Item",	"ItemId",			"Cite",		"ItemId"},
		{"Item",	"ItemId",			"Cite",		"Reference"}};

	final static String[][] DBLPF_PKEYS = {
		{"Item", "ItemId"},
		{"Person", "PersonId"},		
		{"Publisher", "PublisherId"},
		{"Series", "SeriesId"} };

	final static String[][] DBLPF_TEXTCOLS = {
		{"Item", "Title"},
		{"Person", "Name"},
		{"Publisher", "Name"},
		{"Series", "Title"},
		{"RelationPersonItem", ""},
		{"Cite", ""} };

	final static String[][] DBLPF_KEYS = {	//foreign keys; need to be indexed		
		{"Person", ""},
		{"Item", "CrossRef,PublisherId,SeriesId"},
		{"Publisher", ""},
		{"Series", ""},
		{"RelationPersonItem", "ItemId,PersonId"},
		{"Cite", "ItemId,Reference"} };

//-------------------------------------IMDB------------------------------------
	final static String[][] IMDB_AVGDL = {
		{"movies","3.1817"},
		{"actors","1.9086"},
		{"actorplay","1.1845"},
		{"actresses","1.926"},
		{"actressplay","1.2456"},
		{"directors","1.9693"},
		{"genres","1.0077"} };

	final static String[][] IMDB_REFS = {
		{"actors",	"Id",				"actorplay",	"Actor"},
		{"movies",	"Id",				"actorplay",	"Movie"},
		{"actresses",	"Id",				"actressplay",	"Actress"},
		{"movies",	"Id",				"actressplay",	"Movie"},
		{"directors",	"Id",				"direct",	"Director"},
		{"movies",	"Id",				"direct",	"Movie"},
		{"movies",	"Id",				"genres",	"Movie"} };
	
	final static String[][] IMDB_PKEYS = {
		{"actorplay","Id"},
		{"actors","Id"},
		{"actresses","Id"},
		{"actressplay","Id"},
		{"directors","Id"},
		{"genres","Id"},
		{"movies","Id"} };

	final static String[][] IMDB_TEXTCOLS = {
		{"actorplay","Charactor"},
		{"actors","Name"},
		{"actresses","Name"},
		{"actressplay","Charactor"},
		{"directors","Name"},
		{"genres","Name"},
		{"movies","Name"} };

	final static String[][] IMDB_KEYS = {
		{"actorplay","Actor,Movie"},
		{"actors",""},
		{"actresses",""},
		{"actressplay","Actress,Movie"},
		{"direct","Movie"},
		{"directors",""},
		{"genres","Movie"},
		{"movies",""} };
	
//-------------------------------MONDIAL-------------------------------
	final static String[][] MONDIAL_AVGDL = {
		{"city", "1.1156239764166394"},
		{"continent", "1.2"},
		{"country", "1.1794871794871795"},
		{"desert", "1.4333333333333333"},
		{"ethnic_group", "1.1423076923076922"},
		{"island", "2.5925925925925926"},
		{"is_member", "1.101854236415143"},
		{"lake", "1.898876404494382"},
		{"language", "1.1181818181818182"},
		{"mountain", "1.303370786516854"},
		{"organization", "4.130952380952381"},
		{"politics", "2.005128205128205"},
		{"province", "1.2228654124457308"},
		{"religion", "1.3719211822660098"},
		{"river", "1.1231884057971016"},
		{"sea", "1.391304347826087"} };

	final static String[][] MONDIAL_REFS = {
//		cleaned on 1st March, 2007
		{"city",	"ID",			"organization",	"ID_CITY"},
		{"organization","ID",		"is_member",	"ID_ORGANIZATION"},
		{"country",	"ID",			"is_member",	"ID_COUNTRY"},
		{"city",	"ID",			"country",	"ID_CITY"},			//capital city of a country
		{"country",	"ID",			"borders",	"ID_COUNTRY1"},
		{"country",	"ID",			"borders",	"ID_COUNTRY2"},
		{"country",	"ID",			"economy",	"ID_COUNTRY"},
		{"country",	"ID",			"population",	"ID_COUNTRY"},
		{"country",	"ID",			"politics",	"ID_COUNTRY"},
		{"city",	"ID",			"located",	"ID_CITY"},
		{"river",	"ID",			"located",	"ID_RIVER"},
		{"lake",	"ID",			"located",	"ID_LAKE"},
		{"sea",		"ID",			"located",	"ID_SEA"}, 
		{"province",	"ID",		"city",		"ID_PROVINCE"},			//city in a province
		{"country",	"ID",			"province",	"ID_COUNTRY"},			//province in a country
		{"city",	"ID",			"province",	"ID_CITY"},			//capital city of a province
		{"country",	"ID",			"language",	"ID_COUNTRY"},
		{"country",	"ID",			"religion",	"ID_COUNTRY"},
		{"country",	"ID",			"ethnic_group",	"ID_COUNTRY"},
		{"lake",	"ID",			"geo_lake",	"ID_LAKE"},
		{"province",	"ID",		"geo_lake",	"ID_PROVINCE"},
		{"river",	"ID",			"geo_river",	"ID_RIVER"},
		{"province",	"ID",		"geo_river",	"ID_PROVINCE"},
		{"sea",		"ID",			"geo_sea",	"ID_SEA"},
		{"province",	"ID",		"geo_sea",	"ID_PROVINCE"},
		{"island",	"ID",			"geo_island",	"ID_ISLAND"},
		{"province",	"ID",		"geo_island",	"ID_PROVINCE"},
		{"mountain",	"ID",		"geo_mountain",	"ID_MOUNTAIN"},
		{"province",	"ID",		"geo_mountain",	"ID_PROVINCE"},
		{"desert",	"ID",			"geo_desert",	"ID_DESERT"},
		{"province",	"ID",		"geo_desert",	"ID_PROVINCE"},
		{"country",	"ID",			"encompasses",	"ID_COUNTRY"},
		{"continent",	"ID",		"encompasses",	"ID_CONTINENT"},
		{"lake",	"ID",			"river",	"ID_LAKE"},
		{"sea",		"ID",			"river",	"ID_SEA"},
		{"river",	"ID",			"river",	"ID_RIVER"},
		{"sea",		"ID",			"merges_with",	"ID_SEA1"},
		{"sea",		"ID",			"merges_with",	"ID_SEA2"} };

	final static String[][] MONDIAL_KEYS = {
		{"borders",	"ID_COUNTRY1,ID_COUNTRY2"},
		{"city",	"ID,ID_PROVINCE"},
		{"continent",	"Id"},
		{"country",	"Id,ID_CITY"},
		{"desert",	"ID"},
		{"economy",	"ID_COUNTRY"},
		{"encompasses",	"ID_CONTINENT,ID_COUNTRY"},
		{"ethnic_group","Name,ID_COUNTRY"},
		{"geo_desert",	"ID_PROVINCE,ID_DESERT"},
		{"geo_island",	"ID_PROVINCE,ID_ISLAND"},
		{"geo_lake",	"ID_PROVINCE,ID_LAKE"},
		{"geo_mountain","ID_PROVINCE,ID_MOUNTAIN"},
		{"geo_river",	"ID_PROVINCE,ID_RIVER"},
		{"geo_sea",	"ID_PROVINCE,ID_SEA"},
		{"is_member",	"ID_COUNTRY,ID_ORGANIZATION"},
		{"island",	"ID"},
		{"lake",	"ID"},
		{"language",	"Name,ID_COUNTRY"},
		{"located",	"ID_CITY,ID_RIVER,ID_LAKE,ID_SEA"},
		{"merges_with",	"ID_SEA1,ID_SEA2"},
		{"mountain",	"ID"},
		{"organization","ID,ID_CITY"},
		{"politics",	"ID_COUNTRY"},
		{"population",	"ID_COUNTRY"},
		{"province",	"ID,ID_COUNTRY,ID_CITY"},
		{"religion",	"Name,ID_COUNTRY"},
		{"river",	"ID"},
		{"sea",		"ID"} };

	final static String[][] MONDIAL_PKEYS = {
		{"borders",	"ID_COUNTRY1,ID_COUNTRY2"},
		{"city",	"ID"},
		{"continent",	"Id"},
		{"country",	"Id"},
		{"desert",	"ID"},
		{"economy",	"ID_COUNTRY"},
		{"encompasses",	"ID_CONTINENT,ID_COUNTRY"},
		{"ethnic_group","Name,ID_COUNTRY"},
		{"geo_desert",	"ID_PROVINCE,ID_DESERT"},
		{"geo_island",	"ID_PROVINCE,ID_ISLAND"},
		{"geo_lake",	"ID_PROVINCE,ID_LAKE"},
		{"geo_mountain","ID_PROVINCE,ID_MOUNTAIN"},
		{"geo_river",	"ID_PROVINCE,ID_RIVER"},
		{"geo_sea",	"ID_PROVINCE,ID_SEA"},
		{"is_member",	"ID_COUNTRY,ID_ORGANIZATION"},
		{"island",	"ID"},
		{"lake",	"ID"},
		{"language",	"Name,ID_COUNTRY"},
//		{"located",
		{"merges_with",	"ID_SEA1,ID_SEA2"},
		{"mountain",	"ID"},
		{"organization","ID"},
		{"politics",	"ID_COUNTRY"},
		{"population",	"ID_COUNTRY"},
		{"province",	"ID"},
		{"religion",	"Name,ID_COUNTRY"},
		{"river",	"ID"},
		{"sea",		"ID"} };

	final static String[][] MONDIAL_TEXTCOLS = {
		{"city",	"Name"},
		{"continent",	"Name"},
		{"country",	"Code,Name"},
		{"desert",	"Name"},
		{"ethnic_group","Name"},
		{"is_member",	"Type"},
		{"island",	"Name,Islands"},
		{"lake",	"Name"},
		{"language",	"Name"},
		{"mountain",	"Name"},
		{"organization","Abbreviation,Name"},
		{"politics",	"Government"},
		{"province",	"Name"},
		{"religion", 	"Name"},
		{"river",	"Name"},
		{"sea",		"Name"} };

//-----------------------------northwind--------------------------------
	
	public static String cutTail(String s, int c) {
		if (s.length() >= c)
			return s.substring(0, s.length() - c);
		return s;
	}
	public static StringBuffer cutTail(StringBuffer s, int c) {
		if (s.length() >= c)
			return s.delete(s.length() - c, s.length());
		return s;

	}
	public static String cleanData(String col) {
		col = col.toLowerCase();
		col = col.replaceAll("\\p{Punct}|\\p{Blank}"," ");					//clean punctuations
		col = " " + col.replaceAll("\\s+","  ").trim() + " ";				//compact
		col = col.replaceAll("\\s\\p{Alnum}{1,"+(MIN_WORD_LEN-1)+"}?\\s", "");	//delete all words with length<=3
		return col;
	}
	public static String cleanDataCompact(String col) {
		col = col.toLowerCase();
		col = col.replaceAll("\\p{Punct}|\\p{Blank}"," ");					//clean punctuations
		col = col.replaceAll("\\s+"," ").trim();
		return col;

	}
	public static StringBuffer replaceString(final String aInput, final String aOldPattern, final String aNewPattern){
		if ( aOldPattern.equals("") ) {
			throw new IllegalArgumentException("Old pattern must have content.");
		}

		final StringBuffer result = new StringBuffer();
		//startIdx and idxOld delimit various chunks of aInput; these
		//chunks always end where aOldPattern begins
		int startIdx = 0;
		int idxOld = 0;
		while ((idxOld = aInput.indexOf(aOldPattern, startIdx)) >= 0) {
			//grab a part of aInput which does not include aOldPattern
			result.append( aInput.substring(startIdx, idxOld) );
			//add aNewPattern to take place of aOldPattern
			result.append( aNewPattern );

			//reset the startIdx to just after the current match, to see
			//if there are any further matches
			startIdx = idxOld + aOldPattern.length();
		}
		//the final chunk will go to the end of aInput
		result.append( aInput.substring(startIdx) );
		return result;
	}
}
