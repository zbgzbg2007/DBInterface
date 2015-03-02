import java.io.*;
import java.sql.*;
import java.util.*;
import discover.*;

class InvertedIndex {
	SchemaGraph sg;
	HashSet<String> stopWords;
	//TreeSet<String> dict;
//	HashMap<String, Integer> dict;
	Statement stmtUnlimit;
	
	protected String[] readLine(String[] textCol, ResultSet rs) throws SQLException{
		StringBuffer colBuffer = new StringBuffer();
		for (String tc: textCol) {
			colBuffer = colBuffer.append(rs.getString(tc)).append(" ");
		}
		String col = colBuffer.toString();
		col = Global.cleanData(col);
		col = col.replaceAll("  ", ",").trim();
		return col.split(",");	
	}
	
	public InvertedIndex(SchemaGraph _sg) throws SQLException {
		sg = _sg;
		stopWords = new HashSet<String>();
		String[] sws = {"a", "a's", "able", "about", "above", "according", "accordingly", "across", "actually", "after", "afterwards", "again", "against", "ain't", "all", "allow", "allows", "almost", "alone", "along", "already", "also", "although", "always", "am", "among", "amongst", "an", "and", "another",	"any", "anybody", "anyhow", "anyone", "anything", "anyway", "anyways", "anywhere", "apart", "appear", "appreciate", "appropriate", "are", "aren't", "around", "as", "aside", "ask", "asking", "associated", "at", "available", "away", "awfully", 
				"b", "be", "became", "because", "become", "becomes", "becoming", "been", "before", "beforehand", "behind", "being", "believe", "below", "beside", "besides", "best", "better", "between", "beyond", "both", "brief", "but", "by", 
				"c", "c'mon", "c's", "came", "can", "can't", "cannot", "cant", "cause", "causes", "certain", "certainly", "changes", "clearly", "co", "com", "come", "comes", "concerning", "consequently", "consider", "considering", "contain", "containing", "contains", "corresponding", "could", "couldn't", "course", "currently", 
				"d", "definitely", "described", "despite", "did", "didn't", "different", "do", "does", "doesn't", "doing", "don't", "done", "down", "downwards", "during", 
				"e", "each", "edu", "eg", "eight", "either", "else", "elsewhere", "enough", "entirely", "especially", "et", "etc", "even", "ever", "every", "everybody", "everyone", "everything", "everywhere", "ex", "exactly", "example", "except", 
				"f", "far", "few", "fifth", "first", "five", "followed", "following", "follows", "for", "former", "formerly", "forth", "four", "from", "further", "furthermore", "g", "get", "gets", "getting", "given", "gives", "go", "goes", "going", "gone", "got", "gotten", "greetings", 
				"h", "had", "hadn't", "happens", "hardly", "has", "hasn't", "have", "haven't", "having", "he", "he's", "hello", "help", "hence", "her", "here", "here's", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "hi", "him", "himself", "his", "hither", "hopefully", "how", "howbeit", "however", 
				"i", "i'd", "i'll", "i'm", "i've", "ie", "if", "ignored", "immediate", "in", "inasmuch", "inc", "indeed", "indicate", "indicated", "indicates", "inner", "insofar", "instead", "into", "inward", "is", "isn't", "it", "it'd", "it'll", "it's", "its", "itself", "j", "just", 
				"k", "keep", "keeps", "kept", "know", "knows", "known", "l", "last", "lately", "later", "latter", "latterly", "least", "less", "lest", "let", "let's", "like", "liked", "likely", "little", "look", "looking", "looks", "ltd", 
				"m", "mainly", "many", "may", "maybe", "me", "mean", "meanwhile", "merely", "might", "more", "moreover", "most", "mostly", "much", "must", "my", "myself", "n", "name", "namely", "nd", "near", "nearly", "necessary", "need", "needs", "neither", "never", "nevertheless", "new", "next", "nine", "no", "nobody", "non", "none", "noone", "nor", "normally", "not", "nothing", "novel", "now", "nowhere", 
				"o", "obviously", "of", "off", "often", "oh", "ok", "okay", "old", "on", "once", "one", "ones", "only", "onto", "or", "other", "others", "otherwise", "ought", "our", "ours", "ourselves", "out", "outside", "over", "overall", "own", "p", "particular", "particularly", "per", "perhaps", "placed", "please", "plus", "possible", "presumably", "probably", "provides", 
				"q", "que", "quite", "qv", "r", "rather", "rd", "re", "really", "reasonably", "regarding", "regardless", "regards", "relatively", "respectively", "right", 
				"s", "said", "same", "saw", "say", "saying", "says", "second", "secondly", "see", "seeing", "seem", "seemed", "seeming", "seems", "seen", "self", "selves", "sensible", "sent", "serious", "seriously", "seven", "several", "shall", "she", "should", "shouldn't", "since", "six", "so", "some", "somebody", "somehow", "someone", "something", "sometime", "sometimes", "somewhat", "somewhere", "soon", "sorry", "specified", "specify", "specifying", "still", "sub", "such", "sup", "sure", 
				"t", "t's", "take", "taken", "tell", "tends", "th", "than", "thank", "thanks", "thanx", "that", "that's", "thats", "the", "their", "theirs", "them", "themselves", "then", "thence", "there", "there's", "thereafter", "thereby", "therefore", "therein", "theres", "thereupon", "these", "they", "they'd", "they'll", "they're", "they've", "think", "third", "this", "thorough", "thoroughly", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "took", "toward", "towards", "tried", "tries", "truly", "try", "trying", "twice", "two", 
				"u", "un", "under", "unfortunately", "unless", "unlikely", "until", "unto", "up", "upon", "us", "use", "used", "useful", "uses", "using", "usually", "v", "value", "various", "very", "via", "viz", "vs", 
				"w", "want", "wants", "was", "wasn't", "way", "we", "we'd", "we'll", "we're", "we've", "welcome", "well", "went", "were", "weren't", "what", "what's", "whatever", "when", "whence", "whenever", "where", "where's", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "who's", "whoever", "whole", "whom", "whose", "why", "will", "willing", "wish", "with", "within", "without", "won't", "wonder", "would", "would", "wouldn't", 
				"x", "y", "yes", "yet", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves", "z", "zero"};
		for (String sw: sws) stopWords.add(sw);
	
//		dict = new HashMap<String, Integer>();
		//TreeSet<String> tmpDict = new TreeSet<String>();
		stmtUnlimit = sg.dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
	}	
//useless
/*		sg.dbconn.executeUpdates(new StringBuffer("DROP TABLE IF EXISTS Dict"));
		
		StringBuffer sqlCreateDict = new StringBuffer();
		sqlCreateDict.append("CREATE TABLE Dict (WordId INT, Word VARCHAR(50) Key)");
		sg.dbconn.executeUpdates(sqlCreateDict);
		int id = 0;
		for (GraphNode n: sg.getNodes()) {
			if (n.getTextCols() != null) {
				String[] textCol = n.getTextCols().split(",");
				StringBuffer sqlGetText = new StringBuffer();
				sqlGetText.append("SELECT ").append(n.getTextCols()).append(" FROM ").append(n.getTableName());
	//System.out.println(sqlGetText);			
				ResultSet rs = stmtUnlimit.executeQuery(sqlGetText.toString());
				
				while (rs.next()) {					
					for (String kw: readLine(textCol, rs)) {
//						if (!tmpDict.contains(kw) && kw.length() > 0) tmpDict.add(kw);
						if (kw.length() > 0) {
							StringBuffer sqlAdd = new StringBuffer();
							sqlAdd.append("INSERT INTO Dict VALUES (").append(id++).append(", \"").append(kw).append("\")");
							try {
								sg.dbconn.executeUpdates(sqlAdd);
							} catch (SQLException e) {
								id--;
							}
						}
					}
				}	
			}
		}
		int i = 1;
		for (String kw: tmpDict) {
//System.out.println(kw);			
			dict.put(kw, i);
			i++;
		}
	}

	public void createDict() throws SQLException, IOException, Exception{
		sg.dbconn.executeUpdates(new StringBuffer("DROP TABLE IF EXISTS Dict"));
		
		StringBuffer sqlCreateDict = new StringBuffer();
		sqlCreateDict.append("CREATE TABLE Dict (WordId INT, Word VARCHAR(50))");
		sg.dbconn.executeUpdates(sqlCreateDict);
		for (String kw: dict.keySet()) {
			StringBuffer sqlInsertWord = new StringBuffer();
			sqlInsertWord.append("INSERT INTO Dict VALUES (" + dict.get(kw) + ",'" + kw + "')");
//System.out.println(sqlInsertWord);			
			sg.dbconn.executeUpdates(sqlInsertWord);	
		}
		sg.dbconn.executeUpdates(new StringBuffer("CREATE INDEX DictIndex USING HASH ON Dict (Word)"));
	}*/	
	
	public void createInvertList() throws SQLException, IOException {
		stmtUnlimit = sg.dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		
		sg.dbconn.executeUpdates(new StringBuffer("DROP TABLE IF EXISTS Dict"));
		
		StringBuffer sqlCreateDict = new StringBuffer();
		sqlCreateDict.append("CREATE TABLE Dict (WordId INT, Word VARCHAR(50) Primary Key)");

		sg.dbconn.executeUpdates(sqlCreateDict);
//		sg.dbconn.executeUpdates(new StringBuffer("CREATE INDEX DictIndex USING HASH ON Dict (Word)"));

		int id = 0;
//		GraphNode n = sg.tableNameDict.get("movies");
		for (GraphNode n: sg.getNodes()) {
			if (n.getTextCols() != null) {
System.out.println("processing "+n.getTableName()+"...");				
				String indexTableName = n.getTableName() + Global.INDEX_TABLE_NAME;
				sg.dbconn.executeUpdates(new StringBuffer("DROP TABLE IF EXISTS "+indexTableName));
				StringBuffer sqlCreateII = new StringBuffer("CREATE TABLE ");
				sqlCreateII.append(indexTableName).append("(WordId INT NOT NULL, PKey INT NOT NULL, Count INT)");
				sg.dbconn.executeUpdates(sqlCreateII);
				
				String[] textCol = n.getTextCols().split(",");
				String tblName = n.getTableName();
				String pkey = sg.primaryKey.get(n.getTableName());
				
				int i = 1;
				while (i <= sg.originalSize.get(tblName)) {

					StringBuffer sqlGetText = new StringBuffer();
					sqlGetText.append("SELECT ").append(n.getPrimaryKey()).append(",")
					.append(n.getTextCols()).append(" FROM ").append(n.getTableName())
					.append(" WHERE ").append(pkey).append(">=").append(i)
					.append(" AND ").append(pkey).append("<").append(i+1000);
	
					ResultSet rs = stmtUnlimit.executeQuery(sqlGetText.toString());
					Statement stmtUpdate = sg.dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
	
					while (rs.next()) {

						TreeMap<String, Integer> wordList = new TreeMap<String, Integer>();
						for (String kw: readLine(textCol, rs)) {
							if (kw.length() > 0 && !stopWords.contains(kw)) {
								if (!wordList.containsKey(kw)) {						
									wordList.put(kw, 1);
								} else {
									int prevCount = wordList.get(kw);
									wordList.put(kw, prevCount + 1);
								}	
							}
						}
	//System.out.println(sqlGetText+n.getPrimaryKey());
						//int PKey = rs.getInt(n.getPrimaryKey());
						int PKey = i;
if (PKey % 1000 == 0) System.out.println(PKey);		
						for (String kw: wordList.keySet()) {
							StringBuffer sqlAdd = new StringBuffer();
							//int wordId = id;
							sqlAdd.append("INSERT INTO Dict VALUES (").append(id++).append(", \"").append(kw).append("\")");						
							try {
								sg.dbconn.executeUpdates(sqlAdd);							
							} catch (SQLException e) {
								id--;			
							
							}					
							StringBuffer sqlInsertRow = new StringBuffer("INSERT INTO ");
							sqlInsertRow.append(indexTableName).append(" VALUES (")
								//.append(wordId).append(",")
								.append("(SELECT WordId FROM Dict WHERE Word=\""+kw+"\")").append(",")
								.append(PKey).append(",")
								.append(wordList.get(kw)).append(")");
							try {
								stmtUpdate.executeUpdate(sqlInsertRow.toString());
							} catch (SQLException e) {
								System.out.println("invalid error: "+i+"  "+sqlInsertRow);
							}
						}
						i++;
					}
				}
				
				StringBuffer sqlAddIdx = new StringBuffer();

				sqlAddIdx.append("ALTER TABLE ").append(indexTableName).append(" ADD CONSTRAINT ")
					.append(indexTableName).append("_PKEY PRIMARY KEY(WordId, PKey);");
				
				//pkey.append("analyze index ").append(tbl_name_q).append("_PKEY compute statistics;");
				
			//	sg.dbconn.executeUpdates(sqlAddIdx);
			}
		}
		StringBuffer sqlAddIdx = new StringBuffer();
		sqlAddIdx.append("alter table Dict drop primary key;");
		sqlAddIdx.append("create index wordHash using HASH on Dict(Word);");
		sg.dbconn.executeUpdates(sqlAddIdx);
		
	}
	
	public void updateDl() throws SQLException {
		for (GraphNode n: sg.getNodes()) {
			if (n.getTextCols() != null) {

System.out.println("processing "+n.getTableName()+"...");
				int i = 1;
				String[] textCol = n.getTextCols().split(",");
				String tblName = n.getTableName();
				String pkey = sg.primaryKey.get(n.getTableName());
				//int[] dl = new int[sg.originalSize.get(n.getTableName())];
				
				while (i <= sg.originalSize.get(tblName)) {
				
				StringBuffer sqlGetText = new StringBuffer();
				sqlGetText.append("SELECT ").append(n.getTextCols()).append(" FROM ").append(tblName)
					.append(" WHERE ").append(pkey).append(">=").append(i)
					.append(" AND ").append(pkey).append("<").append(i+1000);
				ResultSet rs = stmtUnlimit.executeQuery(sqlGetText.toString());			
				
				while (rs.next()) {		
if (i % 1000 == 0) System.out.println(i);	
					int len = 0;
					for (String kw: readLine(textCol, rs)) {
						if (kw.length() > 0) len++;
					}					
					sg.dbconn.executeUpdates(new StringBuffer("UPDATE "+tblName+" SET dl = "+len+" WHERE "+pkey+" = "+i));
					
	//				dl[i] = len;
					i++;
				}
				rs.close();
				}

				
		/*		Statement stmtUpdatable = sg.dbconn.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
				ResultSet rsUpdatable = stmtUpdatable.executeQuery("SELECT dl FROM "+n.getTableName());
				
				i = 0;
System.out.println("updating "+n.getTableName()+"...");
				while (rsUpdatable.next()) {
if (i % 1000 == 0) System.out.println(i);						
					rsUpdatable.updateInt("dl", dl[i]);
					rsUpdatable.updateRow();
					i++;
				}
				stmtUpdatable.close();*/
			}
		}
	}
}

public class TableGen {
	public static void main (String args[]) throws SQLException, IOException, Exception{
		Global global = new Global();
		if (args.length < 1) {
			System.out.println("Create full text index for a given dataset.");
			System.out.println("Each textual relation should contain a \"dl\"(document length) column.");
			System.out.println("USAGE: java TableGen dataset");			
		} else {
			global.setAttribute("DATASET", args[0]);
			SchemaGraph sg = new SchemaGraph(global);
			sg.setDataSet();		
			
			InvertedIndex iindex = new InvertedIndex(sg);
			
			//System.err.println("create dict table...");
			//iindex.createDict();  
			System.err.println("update document lengths...");
			iindex.updateDl();
			System.err.println("create inverted lists...");
			iindex.createInvertList();
		}
	}

}

