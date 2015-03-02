package discover;

import java.sql.*;
import java.util.*;

public class Solution extends Graph { 		//Solution = CN
	public Query query;		//if null, invalid CN
	
	public SchemaGraph schema;
	public Global global;
	public String stmtStr;
	public PreparedStatement stmt;
	
	public HashMap<GraphNode, Integer> curCur;
	//public HashMap<GraphNode, GraphNode> sysmetry; //not implemented
	public double globalWeight;
	protected int ksize;
	protected boolean inResults;
	
	protected HashMap<String, Double> idf;	//idf for each keyword in the solution, only used for seperate idf
	public double sumidf;
	protected double maxidf;
	protected double upperic;					//upper bound of incompleteness factor
	protected double avdl;
	long sumNonFreeTuple;						//solutions are ordered by sum number of non-free tuples, for building CN tree

	public boolean idfErrorComputed;			//normally not used, unless compute idf error
	
	protected boolean isValid() {
		return (query != null);
	}
	protected boolean isHead() {
		return (ksize()==1);
	}
	public Solution(SchemaGraph _schema) {
		super();
		schema = _schema;
		init();	
	}
	public void setQuery(Query q) {
		if (q != null) {
			query = q;
			setGlobalWeight();
		} else {
			query = null;
		}
	}

	public void setStatement(String sql) throws SQLException {
		Connection conn = schema.dbconn.conn;

//System.out.println(sol.stmtStr);		
		stmt = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		stmt.setMaxRows(global.MAX_ROW);
			
	}
	
	private void init() {
		global = schema.global;
		
		upperic = 0.0;
		globalWeight = 0.0;
		sumNonFreeTuple = 0;
		sumidf = 0.0;
		ksize = 0;
		if (root != null) root.DFCS = "";
		//curCur = new HashMap<GraphNode, Integer>();
		idf = new HashMap<String, Double>();
		stmt = null;
		stmtStr = null;

		idfErrorComputed = false;
	}

	public Solution(Query _query) {
		super();
		query = _query;
		schema = query.schema;
		init();
	}

	public Solution(Solution X) {  
		super(X);
		query = X.query;
		global = X.global;
		schema = X.schema;
		upperic = X.upperic;
		root.DFCS = new String(X.getKeyString());
		globalWeight = X.globalWeight;
		sumNonFreeTuple = X.sumNonFreeTuple;
		sumidf = X.sumidf;
		curCur = new HashMap<GraphNode, Integer>();
		idf = new HashMap<String, Double>();
		stmtStr = null;
		stmt = null;
		idfErrorComputed = false;
		// new
	}
	
	protected void reset() throws SQLException{
		stmtStr = null;
		if (stmt != null) {
			stmt.close();
			stmt = null;
		}
		curCur = null;
	}
	
	public Query getQuery() {
		return query;
	}
	
	public void initCur() throws SQLException{
		for (GraphNode n: getNodes()) {
			if (n.isNonFree()) {
				curCur.put(n, 0);
			}
		}    
	}
	public void setKSize() {	//the number of non-free node
		ksize = 0;
		for (GraphNode n: getNodes()) {
			if (n.isNonFree()) {
				ksize++;
			}
		}
	}
	public void setGlobalWeight() {
		setKSize();
		if (query != null) {
			//ArrayList<String> keywords = query.keywords;
			if (!inResults) {
				globalWeight = 0.0;
				return;
			}
			avdl = 0.0;
			sumNonFreeTuple = 0;
			for (GraphNode n: getNodes()) {
				//if (n.getMatchedSize() > 0) dl++;
				if (n.isNonFree()) {
					String tableName = n.getTableName();
					if (schema.avgDl.containsKey(tableName)) {
						avdl = avdl + schema.avgDl.get(tableName);
					}
					for (Keyword kw: query.getMatchedKeywords(tableName)) {
						double i;
						String k = kw.getStr();
						i = 1.0 - query.selectivity.get(tableName+" "+k);
						if (idf.containsKey(k)) {
							i = idf.get(k) * i; 
						}	
						idf.put(k, i);
					}
					sumNonFreeTuple += query.getNonFreeTupleSize(tableName);
				}
			}     
			sumidf = 0.0;
			upperic = 0.0;
			maxidf = 0.0;
	
			int countk = 0;
			//	for (String k: idf.keySet()) {
			for (int j=0; j < query.getKwNum(); j++) {
				String k = query.getKeyword(j).getStr();
				double i = 0.0;
				if (idf.containsKey(k)) {
					if (global.trysepidf) {
						i = idf.get(k);
						i = Math.log(1.0/(1.0 - i));
						idf.put(k, i);
					}
					else {
						i = query.globalIdf.get(k);
						idf.put(k, i);
					}
	//				System.err.println("idf("+k+")="+i);
					sumidf = sumidf + i;
					maxidf = (maxidf<i)?i:maxidf;
					countk++;
				}
				//sumidf = sumidf + schema.globalIdf.get(k); 
			}
			for (int j=0; j < query.getKwNum(); j++) {
				String k = query.getKeyword(j).getStr();
				double i = 0.0;
				if (idf.containsKey(k)) {
					if (global.checkScoreb) {
						i = idf.get(k); //normalize on idf
					} else {
						i = maxidf;
					}
				}
				upperic = upperic + Math.pow(1.0 - i / maxidf, global.ALPHA);
			}
			if (global.scoreFunc == 2) {
				//globalWeight = globalWeight / (1.0 - ALPHA + ALPHA * (Math.abs(s-1) + 1)); // s=1 is the maximal point
				globalWeight = (1.0 + global.BETA - global.BETA * size()) * (1.0 + 1.0/(1.0+query.getKwNum()) -1.0/(1.0+query.getKwNum())*ksize());
				//upperic = 1.0 - ALPHA + ALPHA * countk / keywords.length; //upperic always <=1
				//upperic = 1.0 - Math.pow(upperic/sumidfpow, 1.0/ALPHA); //upperic always <=1
				upperic = Math.pow(query.getKwNum(), 1.0/global.ALPHA) - Math.pow(upperic, 1.0/global.ALPHA); //upper bound of incomplete factor always <=1
	
				// System.out.println(keyString()+" upperic: "+upperic);
	
			}
			if (global.scoreFunc ==3) {
				globalWeight = 1.0 / (0.8 + 0.2 * (1.0 * size() / schema.AVG_CN_SIZE)); //a=0.2
			}
		}
//		System.err.println(keyString+" "+globalWeight);
	}

	public boolean convertToMinimalSolution() {
		// need to find out all the leaf nodes in the sol, and make
		// sure they all match at least one keyword.
		// also set global weight
		boolean minimal = true;

		for (GraphNode n: getNodes()) {
			int cnt = n.getLinkedNodesSize();
			//debugln(n.getTableName()+ "  " + cnt+ " "+ n.getMatchedKeywordSize());
			if (1 == cnt) {
				// leaf node ==> need to check if it is non-free
				if (!n.isNonFree()) {
					minimal = false;
					break;
				}
			}
		}
		if (minimal) {
			ksize = 0;
			for (GraphNode n: getNodes()) {
				if (n.isNonFree()) {
					ksize++;
				}
			}
		//	setKeyString();
		}
		//powksize = (int)Math.pow(10000,1.0/ksize);
		return minimal; // not implemented yet
	}

	//ksize: number of non-free tuple sets
	public double ksize() {
		//System.out.println("size: "+size);
		return ksize;
	}
	
	protected double uscore(double prevSumtf) {
		return Math.min(sumidf, prevSumtf) * (1 + Math.log(1 + Math.log(prevSumtf / Math.min(sumidf, prevSumtf)))) * globalWeight * upperic;
	}
	


	    



	protected void computeEstErr(Results allResults) throws SQLException{  //only on mysql; normally not used, unless checkIdfErr
		//ArrayList<String> keywords = query.keywords;
		DBConnect dbconn = schema.dbconn;
		double est = 1.0;
		double real = 1.0;
//		double err = 0.0;
//		int errCount = 0;

		StringBuffer select_tables = new StringBuffer("");
		ArrayList<StringBuffer> text_cols = new ArrayList<StringBuffer>();
		StringBuffer table_name = new StringBuffer("");
		for (GraphNode sn: getNodes()) {
			table_name.append(sn.getTableName()).append("_");
			select_tables.append(sn.getTableName()).append(" ").append(sn.getAlignName()).append(",");
			if (dbconn.genTextColList(sn) != null) {
				text_cols.add(dbconn.genTextColList(sn));
			}
//			from_tables.append(sn.getTableName()).append(" ").append(sn.getAlignName()).append(",");
		}
		select_tables = Global.cutTail(select_tables, 1);
		table_name = Global.cutTail(table_name, 1);
//		System.out.println(table_name+"   "+table_name.equals("TMP_InProceeding_Proceeding_InProceeding"));
//		if (!table_name.toString().equals("TMP_InProceeding_Proceeding_InProceeding")) {
		
		//StringBuffer sql_join_cond = dbconn.getJoinNoDupStatement(nodes);
		StringBuffer sql_join_cond = dbconn.getJoinStatement(nodes, null);
		
		StringBuffer sql = new StringBuffer("SELECT COUNT(*) FROM ");
		sql.append(select_tables);
//		.append(" WHERE ").append(select_tables).append(") T FROM ").append(from_tables);
		if (sql_join_cond.length() != 0) {
			sql.append(" WHERE ").append(sql_join_cond);
		}
//		sql.append(";");//ALTER TABLE ").append(table_name).append(" ADD FULLTEXT INDEX(TEXT)");
//		try{ //in case the  table is not yet created
//		dbconn.executeUpdates(sql);
//		}catch (SQLException e) {};
//		StringBuffer sql2 = new StringBuffer();
//		sql2.append("SELECT COUNT(*) FROM ").append(table_name);
		ResultSet rs = dbconn.executeQuery(sql);
		rs.next();
		long countAll = rs.getInt(1);
		for (int i=0; i<query.getKwNum(); i++) {
			String keyword = query.getKeyword(i).getStr();
			StringBuffer sql2 = new StringBuffer(sql);
			sql2.append(" AND (");
			for (StringBuffer textCol: text_cols) {
				sql2.append(" MATCH(").append(textCol).append(") AGAINST ('").append(keyword).append("') OR");
//				System.out.println(sql3);
			}
			sql2 = Global.cutTail(sql2, 3);
			sql2.append(")");
			rs = dbconn.executeQuery(sql2);
			rs.next();
			long countKey = rs.getInt(1);
			est = 1.0;
			for (GraphNode sn: getNodes()) {
				String sk = sn.getTableName()+" "+keyword;
				if (query.selectivity.containsKey(sk)) {
					est = est * (1.0 - query.selectivity.get(sk));
				}
			}
			if (real > 0 && est > 0) {
				est = Math.log(1.0 / (1.0 - est));
				real = Math.log(1.0 * countAll / countKey);
				allResults.idfError = allResults.idfError + Math.abs((est - real) / real);
				allResults.idfErrorCount++;
				System.out.println("Local idf Estimation Err: "+table_name+" "+keyword+" "+real+" "+est+" "+Math.abs((est - real) / real));
			}

		}
//		}
//		return ;
	}

}
