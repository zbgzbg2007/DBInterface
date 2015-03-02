package discover;  


import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;

public class DBConnect{
	Global global;
	public Connection conn;
	Statement stmt;
	SchemaGraph schema;
	String uniqueId;
	
	public DBConnect(SchemaGraph _schema) throws SQLException{
		schema = _schema;
		global = schema.global;
		conn = null;
	}
	public DatabaseMetaData getMetaData() throws SQLException {
		return conn.getMetaData();
	}
	public void resetConn() throws Exception {
		if (conn != null) conn.close();
		initJDBC();
	}
	public void initJDBC() throws Exception{
//		String server;
		if (global.RDMS.equals(Global.MYSQL)) {
			if (global.server == null) {
				global.username = "root";
				global.password = "";
				global.server = "jdbc:mysql://localhost/"+global.dataSet;
			}	
			Class.forName("com.mysql.jdbc.Driver").newInstance();
		//	System.out.println(global.server);
			conn = DriverManager.getConnection(global.server, global.username, global.password);
		} else {
			if (global.server == null) {
				global.server = "jdbc:oracle:thin:@localhost:1520";//try 1520 or 1521
				global.username = global.dataSet;
				global.password = global.username;
			}	
//			DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
//			conn = DriverManager.getConnection(global.server, global.username, global.password);			
		}	
		stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		stmt.setMaxRows(global.MAX_ROW);
		ResultSet rs = stmt.executeQuery("SELECT CONNECTION_ID();");
		rs.next();
		uniqueId = rs.getString(1);
	}

	public ResultSet executeQuery(StringBuffer sql) throws SQLException{		//execute a sql query
//System.out.println(sql.toString());
		return stmt.executeQuery(sql.toString());
	}
	public ResultSet executeQuery(String sql) throws SQLException {
//System.out.println(sql);
		return stmt.executeQuery(sql);
	}
	public void executeUpdates(StringBuffer sqlBuffer) throws SQLException{		//execute a set of update queries
		String[] sqls = sqlBuffer.toString().split(";");
		for (String sql: sqls)  {  
//System.out.println(sql);
			stmt.executeUpdate(sql);
		}
		
	}
	
	
	/*protected StringBuffer genCountSel(GraphNode n, String keyword) { 
		StringBuffer countSel = new StringBuffer("");
		
		countSel.append("SELECT COUNT(*) FROM ").append(n.getTableName()).append(" ").append(n.getAlignName());
		countSel.append(" WHERE ").append(genSqlMatchKeyword(n, keyword));
		//MATCH(").append(schema.textCols.get(tbl_name)).append(") AGAINST (\""+keywords[i]+"\")");
		 */
	
	protected StringBuffer genCountSel(GraphNode n, Query q, int i) {
		StringBuffer countSel = new StringBuffer("");
		if (!q.getKeyword(i).isPhrase) {
			countSel.append("SELECT COUNT(*) FROM ").append(n.getTableName()).append(Global.INDEX_TABLE_NAME)
				.append(" WHERE WordId=").append(q.getKeyword(i).wordId[0]);
		} else {
			countSel.append("SELECT COUNT(*) FROM ").append(n.getTableName()).append(" ")
				.append(n.getAlignName()).append(" WHERE MATCH ").append(genTextColList(n))
				.append(" AGAINST ('\"").append(q.getKeyword(i).getStr()).append("\"' IN BOOLEAN MODE)");
		}
		return countSel;		
	}
	
	protected StringBuffer genGetNonFreeTbl(GraphNode n, Query q) {
		
//		String tbl_name = n.getTableName();
//		String tbl_name_q = n.getTableNameQ();
		String tbl_name_tmp = n.getTableNameTmp(q);
		
		StringBuffer nonFreeTbl = new StringBuffer("SELECT ");
		nonFreeTbl.append(tbl_name_tmp).append(".* FROM ").append(tbl_name_tmp);
		return nonFreeTbl;
	}
	
	protected String findAllCols(String tblName) throws SQLException {
		String colStat = "SELECT * FROM " + tblName + (global.SysIsOracle()?" WHERE ROWNUM=1":" LIMIT 1");
		ResultSet oneRow = stmt.executeQuery(colStat);
		ResultSetMetaData rsmd = oneRow.getMetaData();
		int numberOfColumns = rsmd.getColumnCount();
		StringBuffer colNames = new StringBuffer();
		for (int i = 1; i <= numberOfColumns; i++) {
			colNames.append(rsmd.getColumnName(i));
			if (i < numberOfColumns) colNames.append(",");
		}
		return colNames.toString();
		//cols.close();
	}


	protected void dropIndexTables1() throws SQLException {
		final int TABLE_NAME = 3;
		DatabaseMetaData dbmd = conn.getMetaData();
		String dbUserName = dbmd.getUserName();
		ResultSet rset = dbmd.getTables(null, dbUserName, null, null);
		String sqlTemplate = "DROP TABLE @@@";

		while (rset.next()) {
			String tblName = rset.getString(TABLE_NAME);
			if (tblName.indexOf(Global.NON_FREE_TABLE_NAME) != -1 || tblName.indexOf(Global.COLUMN_INDEX_TABLE_NAME) != -1) {
				String sql = sqlTemplate.replace("@@@", tblName);
				stmt.executeUpdate(sql);
			}
		}
		rset.close();    	
	}

//	----------------------------------------------------------------------------------------------------------------

	protected String genGetTblStat() {
		return global.SysIsOracle()?"SELECT TABLE_NAME, NUM_ROWS FROM USER_TABLES WHERE SECONDARY='N'":"SHOW TABLE STATUS";
	}
	protected StringBuffer genFindWordId(String kw) {
		StringBuffer sqlMatchKeyword = new StringBuffer("SELECT WordId FROM Dict WHERE Word = '"); 
//		if (global.SysIsOracle()) {
//			sqlMatchKeyword.append("CONTAINS(Word, '").append(kw).append("',1) > 0");
//		}
//		else {
//			sqlMatchKeyword.append("MATCH (Word) AGAINST ('").append(kw).append("') ");
//		}
		sqlMatchKeyword.append(kw).append("'");
		return sqlMatchKeyword;
	}

	private StringBuffer genDtfExp(Query q, GraphNode n, int i, boolean inIndex) {
		StringBuffer dtf = new StringBuffer();
		Keyword k = q.getKeyword(i);

		if (!q.matchKeyword(n, k)) return dtf.append("0");
		if (inIndex) {
			if (!k.isPhrase) {
				int id = q.getKeyword(i).wordId[0];
				dtf.append("MAX(CASE WHEN WordId=").append(id).append(" THEN Count ELSE 0 END)");
			} else {			
				int phraseLen = k.wordId.length;
				dtf.append("(");
				for (int j = 0; j< phraseLen; j++) {
					int id = k.wordId[j];
					dtf.append("MAX(CASE WHEN WordId=").append(id).append(" THEN 1 ELSE 0 END)");
					if (j < phraseLen - 1) dtf.append("+"); 
				}
				dtf.append(")/").append(phraseLen);
			}
		} else {
			
			if (!k.isPhrase) {
				dtf.append("Dtf").append(i);
			} else {
				StringBuffer textColList = genTextColList(n);
//			    dtf.append("MATCH (").append(textColList).append(") AGAINST ('\"")
//			    	.append(q.getKwStr(i)).append("\"' IN BOOLEAN MODE)");
				//dtf.append(textColList).append(" REGEXP '[[:<:]]").append(q.getKwStr(i)).append("[[:>:]]'");
				dtf.append(textColList).append(" LIKE '%").append(k.getStr()).append("%'");
			}
		}
		return dtf;
	}
	
	/*
	drop table InProceeding_NF;set @r:=0;CREATE TABLE InProceeding_NF AS SELECT @r:=@r+1 RowN,  Dtf0, @Dtf1:=match(title) against('"Data mining"' in boolean mode) as Dtf1, IFNULL((log(log(Dtf0)+1)+1),0)*9.0028 + IFNULL((log(log(@Dtf1)+1)+1),0)*8.9228 Score,ProceedingId,InProceedingId,dl FROM 

	(SELECT PKey, MAX(CASE WHEN WordId = 3579 THEN Count ELSE 0 END) as Dtf0 FROM InProceeding_INDX WHERE WordId in (3579,28619,85190) GROUP BY PKey Having (Dtf0 >0 or (MAX(CASE WHEN WordId = 28619 THEN 1 ELSE 0 END) + MAX(CASE WHEN WordId = 85190 THEN 1 ELSE 0 END))/2 = 1)  ) Cnt,

	 InProceeding WHERE Cnt.PKey = InProceedingId and (Dtf0>0 or match(title) against('"Data mining"' in boolean mode)=1) and match(title) against("approach" in boolean mode) = 0 ORDER BY Score DESC;
*/

//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
	protected StringBuffer genCreateNonFreeSetWithII(GraphNode n, Query q) {
		String tbl_name = n.getTableName();
		String tbl_name_q = n.getTableNameQ(q);
		q.tmpTableNames.add(tbl_name_q);
		
		StringBuffer createTbl = new StringBuffer();
		int kwNum = q.getKwNum();
		if (!global.SysIsOracle()) {		//mysql
			createTbl.append("SET @r:=0;");
			createTbl.append("CREATE TABLE ").append(tbl_name_q)
				.append(" AS SELECT @r:=@r+1 RowN, ");
			for (int i = 0; i < kwNum; i++) {
				createTbl.append("@Dtf").append(i).append(":=");
				createTbl.append(genDtfExp(q, n, i, false)).append(" Dtf").append(i).append(", ");
			}
			createTbl.append(genScore(q)).append(" Score, ");
			//createTbl.append(n.getTableName()).append(".*");
			if (schema.allKeys.get(n.getTableName()).length() > 0) 
				createTbl.append(schema.allKeys.get(n.getTableName())).append(", ");
			createTbl.append(n.getPrimaryKey()).append(", ").append("dl");
			
			createTbl.append(" FROM (SELECT PKey");	
			for (int i = 0; i < kwNum; i++) {
				if (!q.getKeyword(i).isPhrase) {
					createTbl.append(", ");
					createTbl.append(genDtfExp(q, n, i, true));
					if (!q.getKeyword(i).isPhrase) {
						createTbl.append(" AS Dtf").append(i);
					}	
				}	
			}	
			createTbl.append(" FROM ").append(tbl_name)
				.append(Global.INDEX_TABLE_NAME).append(" WHERE WordId in (");
			
			createTbl.append(getWordIdList(q, n));
				
			createTbl.append(") GROUP BY PKey HAVING ");
			for (int i=0; i<q.getKwNum(); i++) { 
				createTbl.append(genDtfExp(q, n, i, q.getKeyword(i).isPhrase));
				createTbl.append(q.getKeyword(i).isPhrase?"=1":">0");
				if (i < q.getKwNum() - 1) createTbl.append(" OR ");
			}
			createTbl.append(") Cnt, ").append(n.getTableName()).append(" ").append(n.getAlignName())
			.append(" WHERE Cnt.PKey = ").append(n.getPrimaryKey());
			if (q.hasPhrase) {
				createTbl.append(" AND (");			
				for (int i=0; i<q.getKwNum(); i++) { 
					createTbl.append(genDtfExp(q, n, i, false));
					createTbl.append(q.getKeyword(i).isPhrase?"=1":">0");
					if (i < q.getKwNum() - 1) createTbl.append(" OR ");
				}
				createTbl.append(")");
			}	
			if (q.notWords.size() > 0) {				
				for (int i=0; i < q.notWords.size(); i++) {
					createTbl.append(" AND ");
					StringBuffer textColList = genTextColList(n);					
					//createTbl.append("MATCH (").append(textColList).append(") AGAINST ('\"")
			    	//	.append(q.notWords.get(i)).append("\"'");
					//if (q.notWords.get(i).getStr().indexOf(" ") >= 0) 
					//	createTbl.append(" IN BOOLEAN MODE");
			    	//createTbl.append(")=0");
					createTbl.append(textColList).append(" NOT LIKE '%").append(q.notWords.get(i)).append("%'");
				}
			}
			createTbl.append(" ORDER BY Score DESC;");			
			
		}
System.out.println(createTbl);
		return createTbl;
		
	}
	
	private StringBuffer getWordIdList(Query q, GraphNode n) {
		ArrayList<Integer> idList = new ArrayList<Integer>();
	
		for (int i = 0; i < q.getKwNum(); i++) {
		
			if (q.matchKeyword(n, q.getKeyword(i))) {		
				Integer[] ids = q.getKeyword(i).wordId;
				for (int j=0; j<ids.length; j++) {
					idList.add(ids[j]);
				}
			}
		}
		StringBuffer ids = new StringBuffer();
		for (int i=0; i < idList.size(); i++) {
			ids.append(idList.get(i));
			if (i < idList.size() - 1) ids.append(',');
		}
		return ids;
	}
	
	protected String getDataString(ResultSet rs, int rowN, String col) throws SQLException {
		rs.absolute(rowN);
		return rs.getString(col);	
	}

	protected StringBuffer genScore(Query q) {
		StringBuffer sqlScore = new StringBuffer();
		for (int i = 0; i < q.getKwNum(); i++) {
			sqlScore.append("IFNULL((log(log(@Dtf").append(i).append(")+1)+1),0)*")
				.append(q.globalIdf.get(q.getKeyword(i).getStr()));
			if (i < q.getKwNum() - 1) sqlScore.append(" + ");
		}
		return sqlScore;
	}
/*	protected StringBuffer genFindNonFreeTbl(GraphNode n, Query q) {		//create temp non-free tables (before fill contents)
		String tbl_name = n.getTableName();
		String tbl_name_q = n.getTableNameQ();
		String tbl_name_tmp = n.getTableNameTmp();
		StringBuffer createTbl = new StringBuffer("");

		if (global.SysIsOracle()) {
			createTbl.append("CREATE TABLE ").append(tbl_name_tmp).append(" AS ").append(genSqlSelection(tbl_name, n, q)).append(";");
			createTbl.append(genSqlExtraCols(n));
			createTbl.append(genPrimaryKeyList(n).toString().replaceAll(tbl_name_q, tbl_name_tmp));
		}
		else {
			createTbl.append("CREATE TABLE ").append(tbl_name_tmp);
			createTbl.append(" (").append(genPrimaryKeyList(n)).append(genSqlExtraCols(n)).append(") ");
			createTbl.append("AS ").append(genSqlSelection(tbl_name, n, q));
		}
//		global.debugln(createTbl.toString());
		return createTbl;

	}*/
	protected String genNonFreeSet(GraphNode n, Query q) {
		String tbl_name_q = n.getTableNameQ(q);
		//String selection = "Score";
		//if (global.scoreFunc == 2) selection = "Score, dtfs";
		//return ("SELECT " + selection + " FROM " + tbl_name_q + " ORDER BY Score DESC");
		return ("SELECT * FROM " + tbl_name_q + " ORDER BY Score DESC");
	}
/*
	protected StringBuffer genCreateNonFreeTbl(GraphNode n, ArrayList<String> keywords) {	//create real non-free tables
//		String tbl_name = n.getTableName();
		String tbl_name_q = n.getTableNameQ();
		String tbl_name_tmp = n.getTableNameTmp();
		StringBuffer createTbl = new StringBuffer("");

		if (global.SysIsOracle()) {
			createTbl.append("CREATE TABLE ").append(tbl_name_q).append(" AS SELECT rownum RowN, a.* FROM (SELECT * FROM TMP_").append(tbl_name_q).append(" WHERE SCORE > 0 ORDER BY SCORE DESC) a;");
			createTbl.append(genPrimaryKeyList(n));
			createTbl.append(genKeyList(n));
			//createTbl.append("analyze table ").append(tbl_name_q).append(" compute statistics;");
			createTbl.append("analyze table ").append(tbl_name_q).append(" compute statistics for table for all indexes for all indexed columns;");
		}
		else {
			createTbl.append("CREATE TABLE ").append(tbl_name_q);
			createTbl.append(" (").append(genKeyList(n)).append(genPrimaryKeyList(n)).append("RowN Int AUTO_INCREMENT) ");
			createTbl.append("AS SELECT * FROM ").append(tbl_name_tmp).append(" WHERE Score > 0 ORDER BY Score DESC;");
		}
		createTbl.append("DROP TABLE ").append(tbl_name_tmp).append(";");
		//global.debugln(createTbl.toString());
		return createTbl;
	}
*/
	public StringBuffer genPrimaryKeyList(GraphNode n, Query q) {
		StringBuffer pkey = new StringBuffer("");
		String tbl_name = n.getTableName();
		String tbl_name_q = n.getTableNameQ(q);
		if (global.SysIsOracle()) {
			pkey.append("ALTER TABLE ").append(tbl_name_q).append(" ADD CONSTRAINT ").append(tbl_name_q).append("_PKEY PRIMARY KEY(").append(schema.primaryKey.get(tbl_name)).append(");");
			//pkey.append("analyze index ").append(tbl_name_q).append("_PKEY compute statistics;");
		}
		else {
			pkey.append("PRIMARY KEY(").append(schema.primaryKey.get(tbl_name)).append("), ");
		}
		return pkey; //impossible to be empty
	}
	protected StringBuffer genKeyList(GraphNode n, Query q) {
		String tbl_name = n.getTableName();
		String tbl_name_q = n.getTableNameQ(q);
		StringBuffer klist = new StringBuffer("");
		String[] keyCols = ("Score,RowN,"+schema.allKeys.get(tbl_name)).split(",");
		for (String keyCol: keyCols) {
			if (!keyCol.equals("")) {
				if (global.SysIsOracle()) {
					klist.append("CREATE INDEX ").append(tbl_name).append("_").append(keyCol).append(" ON ").append(tbl_name_q).append("(").append(keyCol).append(");");
					//klist.append("analyze index ").append(tbl_name).append("_").append(keyCol).append(" compute statistics;");
				}
				else {
					klist.append("KEY(").append(keyCol).append("), ");
				}	
			}
		}
		return klist;
	}
/*
	protected StringBuffer genSqlExtraCols(GraphNode n) {
		String tbl_name_tmp = n.getTableNameTmp();
		StringBuffer extraCol = new StringBuffer("");
		if (global.SysIsOracle()) {
			if (global.scoreFunc == 1) {
				extraCol.append("ALTER TABLE ").append(tbl_name_tmp).append(" ADD Score FLOAT;");
			}
			if (global.scoreFunc == 2) {
				extraCol.append("ALTER TABLE ").append(tbl_name_tmp).append(" ADD Score FLOAT ADD dtfs VARCHAR2(20) ADD dl INT;");
			}
			if (global.scoreFunc == 3) {
				extraCol.append("ALTER TABLE ").append(tbl_name_tmp).append(" ADD Score FLOAT ADD dtfsosix VARCHAR2(30);");
			}
		}
		else {
			if (global.scoreFunc == 1) extraCol.append(" Score Double");
			if (global.scoreFunc == 2) extraCol.append(" Score Double, dtfs VARCHAR(10), dl INT");
			if (global.scoreFunc == 3) extraCol.append(" Score Double, dtfsosix VARCHAR(30)");
		}
		return extraCol; 
	}
*/


	public StringBuffer genTextColList(GraphNode n) {
//		System.out.println(n.getTableName());
//		System.out.println(n.getTextCols());
		String txcols = n.getTextCols();
		if (txcols != null && txcols.length() != 0) {

			return new StringBuffer(n.getAlignName()).append(".").append(txcols.replaceAll(",", "," + n.getAlignName() + "."));
		}
		return null;
	}
/*
	protected StringBuffer genSqlMatchKeywordOld(GraphNode n, String keyword) {
		StringBuffer sqlMatchKeyword = new StringBuffer("");
		if (global.SysIsOracle()) {
			sqlMatchKeyword.append("CONTAINS(").append(genTextColList(n)).append(", '").append(keyword).append("',1) > 0");
		}
		else {
			sqlMatchKeyword.append("MATCH (").append(genTextColList(n)).append(") AGAINST ('").append(keyword).append("') ");
		}
		return sqlMatchKeyword;
	}

	protected StringBuffer genSqlMatchKeywords(GraphNode n, Query q) {
		StringBuffer sqlMatchKeywords = new StringBuffer("");
		StringBuffer textColList = genTextColList(n);
	    if (global.SysIsOracle()) {
	    	sqlMatchKeywords.append("CONTAINS(").append(textColList).append(", '").append(genKeywordList(q.keywords)).append("',1) > 0");
    	}
    	else {//MYSQL
    		sqlMatchKeywords.append("MATCH (").append(textColList).append(") AGAINST ('").append(genKeywordList(q.keywords)).append("') ");
	    }
	    if (q.notWords.length > 0) {
		    sqlMatchKeywords.append(" AND ");
		    if (global.SysIsOracle()) {
		    	sqlMatchKeywords.append("CONTAINS(").append(textColList).append(", '").append(genKeywordList(q.notWords)).append("') = 0");
		    }
		    else {
	//System.out.println(q.notWords.get(0));	    	
	//	    	Object[] s = new String[q.notWords.size()];
	//	    	s = q.notWords.toArray();
	//System.out.println(s.length);
		    	sqlMatchKeywords.append("MATCH (").append(textColList).append(") AGAINST ('").append(genKeywordList(q.notWords)).append("') = 0 ");
			}
	    }    
		return sqlMatchKeywords;
	}


	protected StringBuffer genSqlSelection(String tableName, GraphNode n, Query q) {
		//String tbl_name = n.getTableName();
		String tbl_align_name = n.getAlignName();
		StringBuffer sqlMatchKeywords = genSqlMatchKeywords(n, q);

		//StringBuffer sqlSelection = new StringBuffer("SELECT * FROM (SELECT *, ");
		//sqlSelection.append(sqlMatchKeywords).append("AS Score FROM ").append(tbl_name).append(" AS ").
		//append(tbl_align_name).append(") AS X WHERE score > 0").append(" ORDER BY Score DESC");

		StringBuffer sqlSelection = new StringBuffer("SELECT " + tbl_align_name + ".* ");

		//select movies.* , score(1) score from movies where contains(Name,'Kill,Easter',1)>0 order by score DESC;   
		sqlSelection.append("FROM ").append(tableName).append(" ").
		append(tbl_align_name).append(" WHERE ").append(sqlMatchKeywords);
		if (global.SysIsOracle()) sqlSelection.append(" ORDER BY Score(1) DESC");

		return sqlSelection;
	}  */

//-------------------------------for query processing----------------------------------
	
	protected StringBuffer getNoDupStatement(TPSolution sol) {
		ArrayList<GraphNode> nodes = sol.getNodes();
		StringBuffer sql_nodup_cond = new StringBuffer();
		for (GraphNode sn1: nodes) {
			for (GraphNode sn2: nodes) {
				if (sn1.getId() < sn2.getId() & sn1.getTableName().equals(sn2.getTableName())) {
					String col = sn1.getPrimaryKey();
					if (col!=null) {
						String col1 = "("+sn1.getAlignName()+"."+col.replaceAll(",", "," + sn1.getAlignName() + ".")+")";
						String col2 = "("+sn2.getAlignName()+"."+col.replaceAll(",", "," + sn2.getAlignName() + ".")+")";
						sql_nodup_cond.append(col1 + " != " + col2 + " AND ");
					}	
				}
			}
		}
		
		return sql_nodup_cond;
	}
	
	protected StringBuffer getJoinStatement(ArrayList<GraphNode> nodes, GraphNode knotNode) {
		if (knotNode != null) nodes.add(knotNode);
		//GraphEdge allEdges = schema.allEdges;
		StringBuffer sql_join_cond = new StringBuffer();
		if (nodes.size() > 1) {
			//i = s.allEdges.size();
			//sql_join_cond.append(" AND ");
			for (GraphNode sn: nodes) {
				for (GraphEdge e : sn.getEdges()) {
				    GraphNode neighbor = e.getNeighbor(sn);
//System.out.println(sn.getTableName()+"("+sn.getId()+")-"+neighbor.getTableName()+"("+neighbor.getId()+")  "+nodes.contains(neighbor));
					if (nodes.contains(neighbor) && sn.getId() >= neighbor.getId()) {					
						String sql_edge = e.genSQLCondition();
						sql_join_cond.append(sql_edge).append(" AND ");
					}
				}	    
			}
			if (sql_join_cond.length() > 0) {
				sql_join_cond.delete(sql_join_cond.length() - 4, sql_join_cond.length());
			}
		}
		
		if (knotNode != null) nodes.remove(knotNode);
		return sql_join_cond;
	}
	
	/*protected StringBuffer getJoinNoDupStatement1(ArrayList<GraphNode> nodes) {
		StringBuffer sql_join_nodup = getNoDupStatement(nodes);
		sql_join_nodup.append(getJoinStatement(nodes, null));
		
		//System.out.println(sql_join_cond);
		return sql_join_nodup;
	}
	*/
	protected StringBuffer getSumScoreStatement(Solution sol) {
		StringBuffer sumtfStatement = new StringBuffer("");
		for (GraphNode sn: sol.getNodes()) {
			if (sn.isNonFree() && global.indexType == Global.TUPLE_RS_WITH_TBLS) {
				sumtfStatement.append(sn.getAlignName()).append(".Score+");
			}
		}    
		sumtfStatement.delete(sumtfStatement.length() - 1, sumtfStatement.length());    
		return sumtfStatement;
	}

	protected StringBuffer getUpperScoreStatement(Solution sol) {
		StringBuffer upperScore = new StringBuffer();
		if (global.scoreFunc == 1) {
			upperScore.append("(").append(getSumScoreStatement(sol));
			upperScore.append(") / ").append(sol.size());//--------------------score function--------------------------------
		}
		if (global.scoreFunc == 2) { //only consider TUPLE_RS_WITH_TBLS
			//System.out.println("HERE!");
//			upperScore.append("LEAST(").append(getSumtfStatement(indexType)).append(", ").append(sol.sumidf).append(") * (log(GREATEST((");
//			upperScore.append(getSumtfStatement(indexType)).append(")/").append(sol.sumidf).append(", 1)) + 1) * ");
			StringBuffer sumtf = getSumScoreStatement(sol);
			upperScore.append("LEAST(").append(sumtf).append(", ").append(sol.sumidf).append(") * ");
			upperScore.append("(ln(ln(GREATEST((").append(sumtf).append(")/").append(sol.sumidf).append(", 1)) + 1)+1) * ");
			upperScore.append(sol.globalWeight).append(" * ").append(sol.upperic);
		}
		if (global.scoreFunc == 3) {
			upperScore.append("0");
		}
		return upperScore;
	}
	protected StringBuffer getSumDtfStatement(int i, TPSolution sol, boolean partial) {
		StringBuffer sumDtf = new StringBuffer();
		if (!partial | sol.ksize <= 2) {
			for (GraphNode sn: sol.getNodes()) {
				if (sn.isNonFree()) {
					sumDtf.append(sn.getAlignName()).append(".Dtf").append(i).append("+"); 
				}
			}
		} else {
//			for (byte j=1; j<sol.epEdge.linkNodes.length; j++) {
//				if (sol.epEdge.linkNodes[j] > 0 & sol.getNode(j).isNonFree()) {
//					sumDtf.append(sol.getNode(j).getAlignName()).append(".Dtf").append(i).append("+");
//				}
//			}
			for (Partition p: sol.epEdge.pList) {
				if (p.pSol.isHead()) {
					byte index = (byte)p.inverseMapNodeIndex(p.pSol.getNodes().get(0));
					sumDtf.append(sol.getNode(index).getAlignName())
						.append(".Dtf").append(i).append("+");
				} else {
					sumDtf.append(p.getAlignName()).append(".Dtf").append(i).append("+");
				}	
			}
		}	
		sumDtf = Global.cutTail(sumDtf, 1);
//System.out.println(sumDtf);		
		return sumDtf;
	}
	protected StringBuffer getSumDlStatement(TPSolution sol, boolean partial) {
		StringBuffer sumDl = new StringBuffer();
		if (!partial | sol.ksize() <= 2) {
			for (GraphNode sn: sol.getNodes()) {
				if (sn.isNonFree()) {
					sumDl.append(sn.getAlignName()).append(".dl+");
				}
			}
		} else {
			//for (byte j=1; j<sol.epEdge.linkNodes.length; j++) {
			//	if (sol.epEdge.linkNodes[j] > 0 & sol.getNode(j).isNonFree()) {
			//		sumDl.append(sol.getNode(j).getAlignName()).append(".dl").append("+");
			//	}
			//}
			for (Partition p: sol.epEdge.pList) {
				if (p.pSol.isHead()) {
					byte index = (byte)p.inverseMapNodeIndex(p.pSol.getNodes().get(0));
					sumDl.append(sol.getNode(index).getAlignName())
						.append(".dl").append("+");
				} else {	
					sumDl.append(p.getAlignName()).append(".dl").append("+");
				} 
					
			}
		}
		sumDl = Global.cutTail(sumDl, 1);
		return sumDl;
	}
	protected StringBuffer genWSumDtfStatement(TPSolution sol, boolean partial) {
		StringBuffer sqlScore = new StringBuffer();
		for (int i = 0; i < sol.query.getKwNum(); i++) {
			sqlScore.append("IFNULL((log(log(").append(getSumDtfStatement(i, sol, partial)).append(")+1)+1),0)*")
				.append(sol.query.globalIdf.get(sol.query.getKeyword(i).getStr()));
			if (i < sol.query.getKwNum() - 1) sqlScore.append(" + ");
		}
		return sqlScore;
	}
	protected StringBuffer getRealScoreStatement(TPSolution sol, boolean partial) {
		StringBuffer rscore = new StringBuffer();
		if (global.scoreFunc == 2) {
			rscore.append("(").append(genWSumDtfStatement(sol, partial)).append(")*").append(sol.globalWeight)
				.append("*(POW(").append(sol.query.getKwNum()).append(",").append(1.0/sol.global.ALPHA)
				.append(") - POW(");
			for (int i = 0; i < sol.query.getKwNum(); i++) {
				rscore.append("(").append(getSumDtfStatement(i, sol, partial)).append("=0)");
				if (i < sol.query.getKwNum() - 1) rscore.append("+");
			}
			rscore.append(",").append(1.0/sol.global.ALPHA).append("))");
			rscore.append("*(1.0-0.0115)/(1.0-0.0115+0.0115*(").append(getSumDlStatement(sol, partial))
				.append(")/").append(sol.avdl).append(")");	
		}
		return rscore;
	}
/*
	protected StringBuffer getRealScoreStatement(Solution sol) { //normally not used, unless try seperate idf
		StringBuffer realScore = new StringBuffer("getRealScore(CONCAT(");
		StringBuffer sumdtf = new StringBuffer("");
		StringBuffer U = new StringBuffer("");
		for (GraphNode sn: sol.getNodes()) {
			if (sn.isNonFree()) {
				realScore.append(sn.getAlignName()).append(".dtfs, ' ', ");
				sumdtf.append(sn.getAlignName()).append(".sumdtf+");
				U.append(sn.getAlignName()).append(".U+");
			}
		}
		sumdtf.delete(sumdtf.length() - 1, sumdtf.length());
		U.delete(U.length() - 1, U.length());
		realScore.delete(realScore.length() - 7, realScore.length());

		realScore.append("), \"");
		for (String kw: sol.getQuery().keywords) {
			if (sol.idf.get(kw) == null) {
				realScore.append("0 ");
			}	
			else {
				realScore.append(sol.idf.get(kw)).append(" ");
			}	
		}
		realScore.delete(realScore.length() - 1, realScore.length());
		realScore.append("\") / (").append(sumdtf).append(") * (").append(U).append(") * ");
		realScore.append(sol.globalWeight).append(" * ").append(sol.upperic);
		return realScore;
	}
*/	
	protected StringBuffer getSelectColumnsString(TodoItem todo, boolean partial) throws SQLException {
		TPSolution sol = todo.sol;
		StringBuffer select_tables = new StringBuffer("");
		//String hint = "";
		ArrayList<GraphNode> nodeSet = null;
		nodeSet = sol.getNodes();
		for (GraphNode sn: nodeSet) {
			//String cols = sol.schema.textCols.get(sn.getTableName());
			//String cols = sol.schema.allCols.get(sn.getTableName());
			String cols = sol.schema.allKeys.get(sn.getTableName());
			//cols = cols.replaceAll("Score","");
			//cols = cols.replaceAll("RowN","");
			if (sol.schema.primaryKey.containsKey(sn.getTableName())) {
				cols = cols + "," + sol.schema.primaryKey.get(sn.getTableName());
			}
			if (sn.isNonFree()) {
				switch (global.scoreFunc) {
				case 1: {cols = "RowN,Score,"+cols; break;}
				//case 2: {cols = "RowN,Score,dtfs,dl,"+cols; break;}
				case 2: {cols = "RowN,"+cols; break;}
				case 3: {cols = "RowN,Score,dtfsosix,"+cols;}				
				}	    
			}
//System.out.println(sn.getTableName()+":"+cols);	
			for (String col: cols.split(",")) {
				if (col.length() > 0) {
				    select_tables.append(sn.getAlignName()).append(".").append(col).append(" ");
				    select_tables.append(sn.getAlignName()).append("_").append(col).append(",");
				}    
			}
		}
		for (int i = 0; i < sol.query.getKwNum(); i++) {
			select_tables.append(getSumDtfStatement(i, sol, partial)).append(" Dtf").append(i).append(",");
		}

		select_tables.append(getSumDlStatement(sol, partial)).append(" dl,");
		//select_tables.append(" (").append(getUpperScoreStatement(sol)).append(") UpperScore");
		select_tables.append(getRealScoreStatement(sol, partial)).append(" Score");
		
		return select_tables;
	}
	
	protected StringBuffer getPreparedSQLStatement(TodoItem todo) throws SQLException {
		Query q = todo.sol.query;
		Solution sol = todo.sol;
		HashMap<String, ResultSet> nonFreeSets = sol.getQuery().nonFreeSets;
		//String[] keywords = sol.getQuery().keywords;
//		System.out.println("call getPreparedSQLStatement"+curCur.size()+(nonFreeSets==null));
		StringBuffer sql = new StringBuffer("");

		StringBuffer from_tables = new StringBuffer("");
		//String hint = "";

		for (GraphNode sn: sol.getNodes()) {
			if (global.indexType == Global.TUPLE_RS_WITH_TBLS) {
				from_tables.append(sn.getNonFreeTableName(q)).append(" ").append(sn.getAlignName()).append(",");
			}
			if (global.indexType == Global.TUPLE_RS) {
				from_tables.append(sn.getTableName()).append(" ").append(sn.getAlignName()).append(",");
			}
		}
		from_tables = Global.cutTail(from_tables, 1);
		StringBuffer sql_tables = new StringBuffer("");
		//StringBuffer sql_tables = new StringBuffer("SELECT ");
		//if (sol.size()>1) sql_tables.append("/*+ USE_NL(T1 T2) */");
		sql_tables.append(getSelectColumnsString(todo, false)).append(" FROM ").append(from_tables);

		StringBuffer sql_where_keyword = new StringBuffer("");

		if (sol.query.topKAlgorithm != Global.SPARSE && nonFreeSets != null 
				&& (global.indexType == Global.TUPLE_RS || global.indexType == Global.TUPLE_RS_WITH_TBLS)) {
			//ResultSet nfs;
			//int length = 0;
			for (byte i=1; i<=todo.size();i++) {
				GraphNode sn = sol.tableIdDict.get(i);
				if (todo.curCur.containsKey(sn)) {
			//for (GraphNode sn: todo.curCur.keySet()) { //in case scoreFunc=1, curCur is null
				//for id range
					StringBuffer range = new StringBuffer("");
					range.append(sn.getAlignName()).append(".RowN >= ? AND ").append(sn.getAlignName()).append(".RowN <= ?");
					sql_where_keyword.append("(").append(range).append(") AND ");			    //sql_where_keyword.append("(");
				}
			}
			//System.out.println();
		}
		if (sql_where_keyword.length()>0) {
			sql_where_keyword.delete(sql_where_keyword.length() - 4, sql_where_keyword.length());
		}

		// finally, THE SQL
		sql.append("SELECT ");
		//if (hint.length() > 0) sql.append("/*+ ").append(hint).append("*/ ");
		sql.append(sql_tables);
		
		StringBuffer sql_join_cond = getNoDupStatement((TPSolution)sol);
		sql_join_cond.append(getJoinStatement(sol.getNodes(), null));		
//		StringBuffer sql_join_cond = getJoinNoDupStatement(sol.getNodes());
		if (sql_where_keyword.length() != 0 || sql_join_cond.length() != 0) {
			sql.append(" WHERE ").append(sql_join_cond);
		}
		if (sql_where_keyword.length() > 0 && sql_join_cond.length() != 0) {
			sql.append(" AND ");
		}
		sql.append(sql_where_keyword).append(" ORDER BY Score DESC LIMIT ").append(global.k);
//System.out.println(sql);
		//if (!trysepidf) sql.append(" LIMIT ").append(MAX_ROW);
		//else sql.append(k);

		//limit k*100, otherwise jdbc for mysql may out of memory. but oracle has no problem

		return sql;
	}

	public StringBuffer buildTPSQLString(TodoItem todo) throws SQLException {	//SQL for _compnodes of sol with "?"
		TPSolution sol = (TPSolution)(todo.sol);
		Query q = sol.getQuery();
		StringBuffer sql = new StringBuffer("");

		//StringBuffer select_tables = new StringBuffer("");
		StringBuffer from_tables = new StringBuffer();
		StringBuffer join_nodup_statement = new StringBuffer();
		//ArrayList<GraphNode> allNodes = new ArrayList<GraphNode>();
		if (!todo.sol.isHead()) {		
			for (Partition p: todo.epTodos.keySet()) {
				if (p.pSol.isHead()) {
					from_tables.append(p.getNodes().get(0).getNonFreeTableName(q))
					.append(" ").append(p.getNodes().get(0).getAlignName()).append(",");
				} else {
					from_tables.append(todo.epTodos.get(p).getTempTableName()).append(" ").append(p.getAlignName()).append(", ");
				}	
			}
			for (GraphNode node: todo.sol.epEdge.getLinkNodesWithoutKnots()) {
				if (!node.isNonFree()) {
					from_tables.append(node.getNonFreeTableName(q)).append(" ").
					append(node.getAlignName()).append(",");
				}	
			}		
			join_nodup_statement = getNoDupStatement(sol);
			join_nodup_statement.append(getJoinStatement(sol.epEdge.getLinkNodes(), null));
			
		} else {				
		//select_tables = Global.cutTail(select_tables, 2);
			//from_tables.append(todo.epTodo.getTempTableName());
			for (GraphNode n: todo.getNodes()) {			
				from_tables.append(n.getNonFreeTableName(q)).append(" ")
				.append(n.getAlignName()).append(",");				
			}
			join_nodup_statement = getNoDupStatement(sol);
			join_nodup_statement.append(getJoinStatement(sol.getNodes(), null));
		}	
		from_tables = Global.cutTail(from_tables, 1);

		// finally, THE SQL
		sql.append("SELECT ");

		//if (hint.length() > 0) sql.append("/*+ ").append(hint).append("*/ ");
		
		sql.append(getSelectColumnsString(todo, true)).append(" FROM ").append(from_tables)
		.append(" WHERE ").append(join_nodup_statement);
		if (join_nodup_statement.length() > 0) sql.append(" AND ");
		//create temp table
		//	for (GraphNode sn: todo.curCur.keySet()) { //in case scoreFunc=1, curCur is null
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		if (!todo.sol.isHead()) {
			for (Partition p: todo.epTodos.keySet()) {
				if (p.pSol.isHead()) nodes.add(p.getNodes().get(0));
			}
		} else {
			for (GraphNode n: todo.getNodes()) {
				if (n.isNonFree()) nodes.add(n);
			}
		}
		for (GraphNode n: nodes) {
			StringBuffer range = new StringBuffer("");
			range.append(n.getAlignName()).append(".RowN >= ? AND ").append(n.getAlignName()).append(".RowN <= ?");
			sql.append("(").append(range).append(") AND ");			    //sql_where_keyword.append("(")
		}
		
		if (!todo.sol.isHead()) {
			int i = 0;
			while (i < todo.sol.epEdge.symmetry.size()) {
				sql.append(todo.sol.epEdge.symmetry.get(i).getAlignName()).append(".RowN < ")
				.append(todo.sol.epEdge.symmetry.get(i+1).getAlignName()).append(".RowN AND ");
				i = i + 2;
			}
		}
		sql = Global.cutTail(sql, 4);
			//	sql = withTmpTable(todo, sql);
		

		//? + order by _joinCol
		//sql.append(" ORDER BY ").append(tieCol);
//System.out.println(sql);
		return sql;
	}
	
	//temptables for intermediate results
	protected StringBuffer genCreateTmpTableString(TodoItem todo, StringBuffer _sql) throws SQLException {

		todo.sol.query.tmpTableNames.add(todo.getTempTableName());

		String sql = _sql.toString();
//		GraphNode n = todo.sol.nonFreeNode;
//		for (GraphNode n: todo.getNodes()) {
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		if (!todo.sol.isHead()){
			for (Partition p: todo.epTodos.keySet()) {
//			if (todo.sol.epEdge._compNodes.contains(n) && n.isNonFree()) {
				if (p.pSol.isHead()) {
					nodes.add(p.getNodes().get(0));
				}
			}
		} else 
			for (GraphNode n: todo.getNodes()) {
				if (n.isNonFree()) nodes.add(n);
			}
//			nodes = todo.getNodes();
		
		for (GraphNode n: nodes) {
			Integer curCur = todo.curCur.get(n).intValue() + 1;
//		    Pattern.compile("?").matcher(sql).replaceFirst(curCur.toString());
			sql = sql.replaceFirst("\\?", curCur.toString());
			sql = sql.replaceFirst("\\?", (new Integer(curCur + todo.length.get(n) - 1)).toString());
		}
		StringBuffer createTmpTbl = new StringBuffer();
		createTmpTbl.append("CREATE ").append(global.SysIsOracle()?"TABLE ":"TEMPORARY TABLE ")
			.append(todo.getTempTableName()).append(" AS ").append(sql);
//System.out.println(createTmpTbl);
		return createTmpTbl;		
	}

	protected StringBuffer genDropTmpTableString(String tableName) {
		
		//if (global.SysIsOracle()) {
		//	return new StringBuffer("DROP MATERIALIZED VIEW ").append(tableName);	//table is faster than materialized view
		//} else {
		
			return new StringBuffer("DROP TABLE ").append(tableName);
		//}
	}
	
	protected void closeTables(ArrayList<String> tbls) throws SQLException {	//intermedate result tables
		for (String tmpTableName: tbls) {
//System.out.println("Drop table "+tmpTableName);			
			executeUpdates(genDropTmpTableString(tmpTableName));
		}
		//select 'DROP TABLE '||table_name||';' from tabs where substr(table_name,1,4)='TMP_';
	}

	public StringBuffer withTmpTable(TodoItem todo, String s) {
		//if (todo.size() > 1) {
//System.out.println(s+  " "+todo.getCurCurVal()+ "  "+((todo.epTodo!=null)?todo.getEpCurCurVal(todo.sol.nonFreeNode):"null"));
		if (!todo.sol.isHead()) {
			for (Partition p: todo.sol.epEdge.pList) {
				if (!p.pSol.isHead()) {
					for (GraphNode sn: p.getNodes()) {
					//if (!todo.sol.epEdge.linkNodes.contains(sn)) {
						//TodoItem epTodo = todo.epTodos.get(p);
					
	//System.out.println(sn.getTableName()+" "+todo.sol.epSol.keyString);					
						s = s.replaceAll(sn.getAlignName()+"\\.", p.getAlignName()+"."+p.mapNode(sn).getAlignName()+"_");
						//s = s.replaceAll(sn.getAlignName()+" ", epTodo.getTempTableName()+" "+p.getAlignName()); //; FROM T2 --> FROM TMP_12345
					}	
				}	//T2.score --> TMP_12345.T2_score
			}	
		}
//System.out.println(s);		
		return new StringBuffer(s);
	}

/*	private void getAvgDl() throws SQLException {
		for (GraphNode n: schema.getNodes()) {
			String tbl_name = n.getTableName();
			ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl_name);
			if (schema.textCols.containsKey(tbl_name)) {
//				System.err.println(tbl_name+" "+schema.textCols.get(tbl_name));
				String[] textCol = schema.textCols.get(tbl_name).split(",");
				long totalwordcount = 0;
				int s = 0;
				while (rs.next()) {
					s++;
					StringBuffer colBuffer = new StringBuffer("");
					for (String tc: textCol) {
						colBuffer = colBuffer.append(rs.getString(tc)).append(" ");
					}
					String col = colBuffer.toString();
					col = Global.cleanData(col);
//					System.err.println(col + col.split("  ").length);

					totalwordcount = totalwordcount + col.split("  ").length;
				}
				System.err.println("{\""+tbl_name+"\", \""+1.0*totalwordcount/s+"\"},");
			}
		}
	}*/
	
//	protected StringBuffer genIdfList(GraphNode n, String[] keywords) {
//	StringBuffer idfList = new StringBuffer("");
//	for (String keyword: keywords) {
//	idfList.append(String.valueOf(n.getIdf(keyword))+" ");
//	}
//	idfList.delete(idfList.length() - 1, idfList.length());
//	return idfList;
//	}
	
}
