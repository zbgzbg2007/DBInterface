/*
  Several hard-coded things: 

  - The schema of the index table is (term, table_name, col_name);

  Limitations:
  - Does not handle a FK->PK constraint where more than 1 attrib is involved on either side
  - Does not treat differently the values in the tables wrt their datatypes. (e.g., special handling of numeric values. 
  - Does not support R-S-R yet (need not only find the sol, but also gen correct sql)
 */
package discover;  

import java.sql.*;
import java.util.*;

public class SchemaGraph extends Graph{
	int TOTAL_TUPLE_NUMBER;
	double AVG_CN_SIZE;
	public HashMap<String, Double> avgDl;
	public HashMap<String, GraphNode> tableNameDict;
	public HashMap<String, String> primaryKey;
	public HashMap<String, String> textCols;
	public HashMap<String, String> allKeys;
	public HashMap<String, String> allCols;
	public HashMap<String, Integer> originalSize;


	//public GraphEdge allEdges;
	public SolutionTree stree;
	public ArrayList<SolutionTree> strees; // Precomputed CN
	public ArrayList<ArrayList<String> > nonfreesets; // Corresponding non-free sets

	public DBConnect dbconn;
	public Global global;
	public int queryNum;

	public SchemaGraph(Global _global) throws SQLException, Exception{
		global = _global;
		reset();
	}

	public void reset() throws SQLException, Exception{
		resetGraph();
		dbconn = new DBConnect(this);
		dbconn.initJDBC();
		queryNum = 0;
		TOTAL_TUPLE_NUMBER = 0;
		AVG_CN_SIZE = 0;
		tableNameDict = new HashMap<String, GraphNode>();

		originalSize = new HashMap<String, Integer>();

		avgDl = new HashMap<String, Double>();

		primaryKey = new HashMap<String, String>();
		allKeys = new HashMap<String, String>();
		textCols = new HashMap<String, String>();
		allCols = new HashMap<String, String>();

		//allEdges = new GraphEdge();
	}


	public void updateDataSet(String dataset) throws SQLException, Exception{
		global.setAttribute("DATASET", dataset);
		reset();
		setDataSet();
	}

	public String getAttribute(String par) {
		return global.getAttribute(par);
	}

	public void setAttribute(String par, String data) {
		global.setAttribute(par, data);
	}

	public void addNode(GraphNode newNode) {
		//super.addNode(newNode, this);
		super.addNode(newNode);System.out.println(newNode.getTableName());
		tableNameDict.put(newNode.getTableName(), newNode);
	}

	public void addEdge(String fktable, String pktable, String fkcol, String pkcol) {
		// NOTE: no dup checking is needed, as we assume that each FK->PK constraint is processed only once
		// first, maintain the node
//System.out.println(fktable+"->"+pktable);
//System.out.println(tableNameDict.size());
		GraphNode pNode = tableNameDict.get(pktable);
		GraphNode fNode = tableNameDict.get(fktable);
		GraphEdge eIn = new GraphEdge(pNode, fNode, true, fkcol);
		pNode.addEdge(eIn);
		GraphEdge eOut = new GraphEdge(pNode, fNode, false, fkcol);
		fNode.addEdge(eOut);
		
		//tableNameDict.get(fktable).addLinkedNode(tableNameDict.get(pktable), fkcol);
		
		// next, update the edge
		//allEdges.addEdge(fktable, pktable, fkcol, pkcol);
	}


	private void addAllTables(DatabaseMetaData dbmd) throws SQLException {
		// final int TABLE_NAME = 3;
		//String dbUserName = dbmd.getUserName();
		//ResultSet rset = dbmd.getTables(null, dbUserName, null, null);
		long totalSize = 0;
		int tablesize;
		ArrayList<String> toCloseTbls = new ArrayList<String>();
		
		String tabStat = dbconn.genGetTblStat();
		int TABLE_NAME = 1;
		int TABLE_SIZE = global.SysIsOracle()?2:5;		
		 
		ResultSet rset = dbconn.executeQuery(tabStat);
		byte id = 1;
		while(rset.next()) {
			String tblName = rset.getString(TABLE_NAME);
			
			if (!tblName.equalsIgnoreCase("Dict") && 
					tblName.indexOf(Global.INDEX_TABLE_NAME) == -1) {
				if (tblName.indexOf(Global.NON_FREE_TABLE_NAME) == -1 && 
					tblName.indexOf(Global.TMP_TABLE_NAME) == -1) {
//			if (tblName.indexOf("_") < 0 && !tblName.equalsIgnoreCase("Dict")) {
					GraphNode sn = new GraphNode(id, tblName, this);
					id++;
					addNode(sn);
					if (textCols.get(tblName) != null) {
						tablesize = rset.getInt(TABLE_SIZE);
						originalSize.put(tblName, tablesize);
						System.out.println(tblName+" "+tablesize);
						totalSize = totalSize + tablesize;
					}
				}
				if (tblName.indexOf(Global.NON_FREE_TABLE_NAME+dbconn.uniqueId) > 0) {
					toCloseTbls.add(tblName);
				}
			}
		}
		dbconn.closeTables(toCloseTbls);
		
		System.out.println("Total:"+totalSize);
		for (GraphNode node: getNodes()) {
			String tblName = node.getTableName();
			allCols.put(tblName, dbconn.findAllCols(tblName));
		}
		rset.close();
	}


	public void setDataSet() throws SQLException{
		//schema = _schema;
		String pk_tbl, pk_col, fk_tbl, fk_col;

//		if (global.RDMS.equals(global.ORACLE)) {
//		backup in 13Sep.tar (or before)
		// MYSQL MyISAM tables do not understand foreign key
//		if (RDMS.equals(MYSQL)) {
		String[][] refs = null;
		String[][] avgDls = null;

		//find keys, primary keys, text columns in each table
		//backup before 17Dec, as its too slow on large tables
		String[][] pkeys = null;
		String[][] tcols = null;
		String[][] keys = null;

		if (global.dataSet.equals(Global.DBLP)) {
			AVG_CN_SIZE = 5.653846;//used in Solution.setGlobalWeight. avg of CN size all possible CNs with size <= 6 5.653846
			//schema.TOTAL_TUPLE_NUMBER = 390099;
			refs = Global.DBLP_REFS;
			avgDls = Global.DBLP_AVGDL;
			pkeys = Global.DBLP_PKEYS;
			tcols = Global.DBLP_TEXTCOLS;
			keys = Global.DBLP_KEYS;
		}
		if (global.dataSet.equals(Global.IMDB)) {
			AVG_CN_SIZE = 5.523472099202834;
			refs = Global.IMDB_REFS;
			avgDls = Global.IMDB_AVGDL;
			pkeys = Global.IMDB_PKEYS;
			tcols = Global.IMDB_TEXTCOLS;
			keys = Global.IMDB_KEYS;
		}
		if (global.dataSet.equals(Global.MONDIAL)) {
			AVG_CN_SIZE = 5.7;  //the number can not correctly computed
			refs = Global.MONDIAL_REFS;
			avgDls = Global.MONDIAL_AVGDL;
			// keys = new String[schema.size()][2];
			keys = Global.MONDIAL_KEYS;
			pkeys = Global.MONDIAL_PKEYS;
			tcols = Global.MONDIAL_TEXTCOLS;
		}
		if (global.dataSet.equals(Global.DBLPF)) {
			AVG_CN_SIZE = 5.7;  //the number can not correctly computed
			refs = Global.DBLPF_REFS;
			avgDls = Global.DBLPF_AVGDL;
			// keys = new String[schema.size()][2];
			keys = Global.DBLPF_KEYS;
			pkeys = Global.DBLPF_PKEYS;
			tcols = Global.DBLPF_TEXTCOLS;
		}

		for (String[] adl: avgDls) {
			avgDl.put(global.SysIsOracle()?adl[0].toUpperCase():adl[0], Double.valueOf(adl[1]).doubleValue());
		}
		for (String[] pkey: pkeys) {
//			System.out.println(pkey[0]+" pkey:"+pkey[1]);
			if (pkey[1].length() > 0) { 
				primaryKey.put(global.SysIsOracle()?pkey[0].toUpperCase():pkey[0], pkey[1]);
			}	
		}
		for (String[] tcol: tcols) {
//			System.out.println(tcol[0]+" tcol:"+tcol[1]);
			if (tcol[1].length() > 0) {
				textCols.put(global.SysIsOracle()?tcol[0].toUpperCase():tcol[0], tcol[1]);
			}
		}
		for (String[] key: keys) {
			allKeys.put(global.SysIsOracle()?key[0].toUpperCase():key[0], key[1]);
		}
		
		DatabaseMetaData dbmd = dbconn.getMetaData();
		//String dbUserName = dbmd.getUserName();
		//ResultSet rset;
		// first, add all tables to the graph
		addAllTables(dbmd);

		for (GraphNode n: getNodes()) {
			String tbl_name = n.getTableName();
			if (textCols.containsKey(tbl_name)) {
				TOTAL_TUPLE_NUMBER = originalSize.get(tbl_name) + TOTAL_TUPLE_NUMBER;
			}
		}
		for (String[] ref: refs) {
			pk_tbl = global.RDMS.equals(Global.ORACLE)?ref[0].toUpperCase():ref[0];
			pk_col = global.RDMS.equals(Global.ORACLE)?ref[1].toUpperCase():ref[1];
			fk_tbl = global.RDMS.equals(Global.ORACLE)?ref[2].toUpperCase():ref[2];
			fk_col = global.RDMS.equals(Global.ORACLE)?ref[3].toUpperCase():ref[3];

			// global.debugln("  " + pk_tbl + ":" + pk_col + " <-- " + fk_tbl + ":" + fk_col);
			addEdge(fk_tbl, pk_tbl, fk_col, pk_col);

			//as in MONDIAL, we didn't build index on foreign key columns, they have to be inserted manually from ref table
			//if (global.DATASET.equals(global.MONDIAL)) {
			//     String k = schema.allKeys.get(fk_tbl);
			//    k = k + "," + fk_col;
			//    schema.allKeys.put(fk_tbl, k);
			// }
		}
		if (global.OFFLINE_CN_SEARCH) {
			//SolutionTree stree1 = new SolutionTree(this);
			//stree1.generateCNG();			//previous CN Graph generator
			
			//stree = new SolutionTree(this);
			//stree.generateValidCNG();		//generate valid CNs only
			//stree = new SolutionTree(this);
			//stree.generateNoDupCNG();		//generate CNs without duplication
			
			//Hard code the non-free tuple sets and generate CNs 
			ArrayList<String> tuplesets = new ArrayList<String>();
			tuplesets.add("tbl_film");
			SolutionTree stree1 = new SolutionTree(this);
			stree1.generateAllCNG(tuplesets);
			strees = new ArrayList<SolutionTree>();
			nonfreesets = new ArrayList<ArrayList<String>>();
			strees.add(stree1);
			nonfreesets.add(tuplesets);
			ArrayList<String> tuplesetss = new ArrayList<String>();
			tuplesetss.add("tbl_film");
			tuplesetss.add("tbl_film_genre");
			SolutionTree stree2 = new SolutionTree(this);
			stree2.generateAllCNG(tuplesetss);
			strees.add(stree2);
			nonfreesets.add(tuplesetss);
			ArrayList<String> tuplesetsss = new ArrayList<String>();
			tuplesetsss.add("tbl_film_genre");
			SolutionTree stree3 = new SolutionTree(this);
			stree3.generateAllCNG(tuplesetsss);
			strees.add(stree3);
			nonfreesets.add(tuplesetsss);
			//stree = new SolutionTree(this);
			//stree.generateNoDupCNG();
		}
	}

}
