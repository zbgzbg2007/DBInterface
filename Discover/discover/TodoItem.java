package discover;

import java.sql.*;
import java.util.*;


public class TodoItem implements Comparable {

	public TreeMap<GraphNode, Integer> curCur;
	public HashMap<GraphNode, Integer> length;

	public TPSolution sol;
	public double possibleMax;
	private double prevSumtf;
	public boolean isRealScore;

	Global global;
	DBConnect dbconn;
	public Statement stmt;

	//tree pipeline
	TreeMap<Partition, TodoItem> epTodos;	//execution parent TodoItem
	PartialResult pResult;						//corresponding Partial Result; null if not yet executed
	private String tempTableName;

	public String getTempTableName() {
		return tempTableName;
	}

	public void close() throws SQLException {
		if (pResult != null) {
			if (pResult.resultset != null) {
				pResult.resultset.close();
			}
			if (pResult.indexes != null) pResult.indexes.clear();
		}
	}

	public StringBuffer getCurCurString() {
		StringBuffer curCurVal = new StringBuffer();
		int[] c = getCurCurList();
		for (int i=1; i<=size(); i++) {
			if (c[i] >= 0) curCurVal.append(c[i]).append(" ");
		}

		return curCurVal;
	}

	public int[] getCurCurList() {
		int[] c = new int[size()+1];
		for (int i=0; i<c.length; i++) c[i] = -1;
		for (GraphNode node: curCur.keySet()) {
			c[node.getId()] = curCur.get(node);
		}
		return c;
	}

	public void setCurCur(int[] c) {
		for (byte i=1; i<= size(); i++) {
			if (c[i] >= 0) {
				curCur.put(sol.tableIdDict.get(i), c[i]);
			}
		}
	}

	protected void addNeighbors(PriorityQueue<TodoItem> todoList) throws SQLException{
		for (GraphNode n: curCur.keySet()) {		//add new todoItems
			TodoItem newTodo = new TodoItem(this);
			if (newTodo.update(n)) {
				String curCurString = newTodo.getCurCurString().toString();
				//if (!sol.todoMap.containsKey(curCurString) && sol.hasVilidTodoItem(newTodo)) {
				if (!sol.todoMap.containsKey(curCurString)) {
					sol.todoMap.put(newTodo.getCurCurString().toString(), newTodo);
					todoList.add(newTodo);
				}			
			}
			if (!curCur.get(n).equals(0))   //no duplication!
				break;
		}	
	}
	public TodoItem linkToEpTodo(Partition p, PriorityQueue<TodoItem> todoList) throws SQLException{
//		System.out.println("link to"+sol.keyString+" "+ep.keyString);		   
		if (p.edge != sol.epEdge) {
			p.edge.setEpEdge();
			sol.stmtStr = null;
		}
		if (epTodos == null) 
			epTodos = new TreeMap<Partition, TodoItem>();
//		System.out.print("ADD LINK " + getCurCurVal() + " TO "+prevcc);  
//		if (!e.isHead()) {
		int[] cc = getCurCurList();

		TPSolution epSol = p.pSol;
		int[] epCurCurList = p.getPPCurCurVal(cc);
		String epCurCurString = p.getPPCurCurStr(cc);
		TodoItem epTodo = epSol.todoMap.get(epCurCurString);
		if (epTodo == null) {
			epTodo = new TodoItem(epSol);
			epTodo.setCurCur(epCurCurList);
			epTodo.possibleMax = epTodo.getUpperScore(sol.query.nonFreeSets);
			epSol.todoMap.put(epCurCurString, epTodo);
			todoList.add(epTodo);
		}
		//		System.out.println("link to" + getCurCurString()+" "+cc[1]+" "+cc[2]+"   "+epCurCurList[1]);
		epTodo = epSol.todoMap.get(epCurCurString);
		epTodos.put(p, epTodo);
		return epTodo;
//		System.out.println(" y");    		
//		} else {
//		epTodo = null;
//		System.out.println(" n");    		
//		}

	}

	public TodoItem(TPSolution _sol) throws SQLException {
		sol = _sol;
		curCur = new TreeMap<GraphNode, Integer>();
		length = new HashMap<GraphNode, Integer>();
		initCur();
		possibleMax = -1.0;
		isRealScore = false;
		global = sol.schema.global;
		dbconn = sol.schema.dbconn;
		stmt = null;
		epTodos = null;
		pResult = null;
		sol.query.todoItemNum++;
		tempTableName = "TMP_" + sol.query.uniqueId+"_"+sol.query.todoItemNum;
	}

	public TodoItem(TodoItem t) {
		curCur = new TreeMap<GraphNode, Integer>(t.curCur);
		length = new HashMap<GraphNode, Integer>();
		sol = t.sol;
		possibleMax = t.possibleMax;
		prevSumtf = t.prevSumtf;
		isRealScore = false;
		global = t.global;
		dbconn = t.dbconn;
		stmt = null;
		epTodos = null;
		pResult = null;
		sol.query.todoItemNum++;
		tempTableName = "TMP_" + sol.query.uniqueId+"_"+sol.query.todoItemNum;
	}

	protected Stack<TodoItem> getTodoItems(ArrayList<SolutionEdge> edges, PriorityQueue<TodoItem> todoList) throws SQLException {
		Stack<TodoItem> curTodoList = new Stack<TodoItem>();
		HashMap<TPSolution, TodoItem> projections = new HashMap<TPSolution, TodoItem>();
		projections.put(sol, this);
		for (SolutionEdge e: edges) {
			TodoItem todo = projections.get(e.sol);
			curTodoList.add(todo);
			for (Partition p: e.pList) {
				TodoItem pTodo = todo.linkToEpTodo(p, todoList);
				projections.put(p.pSol, pTodo);
			}	
		}
		if (sol.isHead()) curTodoList.add(this);
		//curTodoList.push(todo);
		return curTodoList;
	}

		public String toString() {
		return " Todo: " + sol.getKeyString()+ " ("+getCurCurString()+") "+possibleMax;
	}

	public void setStatement(String sql) throws SQLException {
		Connection conn = sol.schema.dbconn.conn;
//		System.out.println(sol.stmtStr);
		if (sql != null) {
			stmt = conn.prepareStatement(sql,ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		} else {
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY,ResultSet.CLOSE_CURSORS_AT_COMMIT);
		}
		stmt.setMaxRows(global.MAX_ROW);
	}

	/*
      build up the results (which is a resultSet of Solution class)
	 */
	protected long executeSQL(Results allResults) throws SQLException {
		// send query to db
//System.out.println(allResults.queryCount+" "+toString());

		long querySize = -1;

		if (sol.stmtStr == null) {
			StringBuffer sql = sol.schema.dbconn.buildTPSQLString(this);
			sol.stmtStr = sql.toString();
		}
		
		querySize = setPara(sol.query.topKAlgorithm);
		allResults.querySize += querySize;

		allResults.SQLTime.start();
		allResults.queryCount++;
		
		StringBuffer createTmpTableString = sol.schema.dbconn.withTmpTable(this, sol.stmtStr);
		createTmpTableString = sol.schema.dbconn.genCreateTmpTableString(this, createTmpTableString);
global.debugln(createTmpTableString);			
		sol.schema.dbconn.stmt.executeUpdate(createTmpTableString.toString());
		pResult.resultset = sol.schema.dbconn.stmt.executeQuery("SELECT * FROM "+getTempTableName()+" ORDER BY Score DESC LIMIT "+global.k);
		
		allResults.SQLTime.stop();

		allResults.addResultSet(pResult.resultset, this);

		pResult.size = Global.resultSetSize(pResult.resultset);
//if (pResult.size > 0) System.out.println(pResult.size);		
		pResult.resultset.close();
		pResult.resultset = null;
		return querySize;
	}



	public Solution getSolution() {
		return sol;
	}
	public int size() {
		return sol.size();
	}
	public double getUpperScore(HashMap<String, ResultSet> nonFreeSets) throws SQLException{
		double upscore = 0.0;
		if (!sol.inResults) return upscore;
		for (GraphNode n: sol.getNodes()) {
			if (n.isNonFree()) { 
				double nodescore = 0.0;
				if (!curCur.containsKey(n) || curCur.get(n) == 0) {
					nodescore = sol.query.maxLocalScore.get(n.getTableName());
				}
				else {
					ResultSet rs = nonFreeSets.get(n.getTableName());
					if (rs.absolute(curCur.get(n) + 1)) {
						nodescore = rs.getDouble("Score");		//HARD CODED!!!!!!!!!: the score column
					}
					else {return -1.0;}
				}
				upscore = upscore + nodescore;
			}	   
		}
//		System.out.println("score"+upscore+"globalWeight"+sol.globalWeight+" sumidf"+sol.sumidf);
		if (global.scoreFunc == 1) {
			upscore = upscore / size();//--------------------score function--------------------------------
		}
		if (global.scoreFunc == 2) {
			prevSumtf = upscore;
			//upscore = Math.min(sol.sumidf, prevSumtf) * (1 + Math.log(1 + Math.log(prevSumtf / Math.min(sol.sumidf, prevSumtf)))) * sol.globalWeight * sol.upperic;
			upscore = sol.uscore(prevSumtf);
		}
		if (global.scoreFunc == 3) {
			upscore = upscore * sol.globalWeight;
		}
		return upscore;
	}

	public int updateUpperScore(GraphNode chosenNode, int offset, HashMap<String, ResultSet> nonFreeSets) throws SQLException{
		double upscore = 0;
		if (!sol.inResults) return 0;

		ResultSet rs = nonFreeSets.get(chosenNode.getTableName());
		Integer privCur = curCur.get(chosenNode) + 1;

		Double privMaxSco = possibleMax;
		rs.absolute(privCur);
		Double privSco = rs.getDouble("Score");						//HARD CODED!!!!!!!!!: the score column
		Double nextSco = -1.0;
		//-------------------for id_range and score_range---------------
		if (rs.absolute(privCur + offset)) {
			nextSco = rs.getDouble("Score");							//HARD CODED!!!!!!!!!: the score column
		}
		else {
			possibleMax = -1.0;
			return offset; 
		}
		//-----------------------------------------------------
		if (global.scoreFunc == 1) {
			upscore = privMaxSco + (nextSco - privSco) / sol.size();   //--------------------score function--------------------------------
		}
		if (global.scoreFunc == 2) {
			prevSumtf = prevSumtf + nextSco - privSco;
			//upscore = Math.min(sol.sumidf, prevSumtf) * (1 + Math.log(1 + Math.log(prevSumtf / Math.min(sol.sumidf, prevSumtf)))) * sol.globalWeight * sol.upperic;
			upscore = sol.uscore(prevSumtf);
		}
		if (global.scoreFunc == 3) {
			upscore = upscore * sol.globalWeight;
		}
		possibleMax = upscore;	
		return offset;
	}

	public boolean checkReal() {
		return isRealScore;
	}

	public void setReal(Results allResults) throws SQLException {
		//int keywordNum = sol.schema.keywords.length;
		ResultItem ri = new ResultItem(sol);
		for (GraphNode n: curCur.keySet()) {
			ResultSet nfSet = sol.getQuery().nonFreeSets.get(n.getTableName());
			nfSet.absolute(curCur.get(n)+1);
			for (int i=0; i < sol.query.getKwNum(); i++) {				
				ri.tfs[i] = ri.tfs[i] + nfSet.getInt("Dtf"+i);
//System.out.print(ri.tfs[i]+" ");
			}
//System.out.println();			
			//String newTfString = nfSet.getString("dtfs"); //second col is always dtf
			//ri.updateTf(newTfString);
		}

//		System.out.println("dtf:"+dtf[0]+" "+dtf[1]);
		possibleMax = ri.setRealScore2();
		//ri.setTfs(dtf);
		//containsAllKeywords = (ri.finalCheckWithTfs());
		isRealScore = true;
	}

	public void initCur() throws SQLException{
		for (GraphNode n: sol.getNodes()) {
			if (n.isNonFree()) {
				curCur.put(n, 0);
			}
		}
		//System.out.println(keyString()+ " : " + curCur.get(chosenNode)+ " "+curMaxScore.get(chosenNode)+" "+possibleMax);
	}

	public void updateCur(GraphNode chosenNode, int offset) throws SQLException {
		Integer privCur = curCur.get(chosenNode);
		//curCur.remove(chosenNode);   
		if (possibleMax > 0) {
			curCur.put(chosenNode, privCur + offset);
		}    
		if (sol.curCur != null) {
			sol.curCur.put(chosenNode, privCur + offset);
			//System.out.println("update sol."+chosenNode.getTableName()+":"+privCur+"+"+offset);
		}
	}	
	public boolean update(GraphNode chosenNode) throws SQLException {
		HashMap<String, ResultSet> nonFreeSets = sol.getQuery().nonFreeSets;
		//System.out.print(offset+" ");
		//int offset = global.TREE_INITIAL_STEP;
		int offset = 0;
		if (length.containsKey(chosenNode)) {
			offset = length.get(chosenNode);
		} else {
			offset = getStep(chosenNode, sol.getQuery().topKAlgorithm, curCur.get(chosenNode)+1, nonFreeSets);
		}

		updateUpperScore(chosenNode, offset, nonFreeSets);
		//System.out.println(offset);
		updateCur(chosenNode, offset);
		//System.out.println("update " + chosenNode.getTableName() + " " + offset + possibleMax);
		if (possibleMax > 0) return true;
		return false;
	}
	public boolean update(GraphNode chosenNode, int length) throws SQLException {
		HashMap<String, ResultSet> nonFreeSets = sol.getQuery().nonFreeSets;
		//System.out.print(offset+" ");
		int offset = length;
		updateUpperScore(chosenNode, offset, nonFreeSets);
		//System.out.println(offset);
		updateCur(chosenNode, offset);
		//System.out.println("update " + chosenNode.getTableName() + " " + offset + possibleMax);
		if (possibleMax > 0) return true;
		return false;
	}

	public int compareTo(Object t) {
		if (((TodoItem)t).possibleMax > possibleMax) return 1;
		if (((TodoItem)t).possibleMax < possibleMax) return -1;
		return hashCode() - t.hashCode();
		//return 0;
	}

	public ArrayList<GraphNode> getNodes() {
		return sol.getNodes();
	}

	public long setPara(int topKAlgorithm) throws SQLException {
//		System.out.println(sol.keyString);
		HashMap<String, ResultSet> nonFreeSets = sol.getQuery().nonFreeSets;
		long qsize = 1;
		int step = 0;
		if (topKAlgorithm != Global.SPARSE) {
			ResultSet nfs;
			int paraNum = 1;
			for (byte i = 1; i <= size(); i++) {
				GraphNode sn = sol.tableIdDict.get(i);
				if (sn.isNonFree()) {
					nfs = nonFreeSets.get(sn.getTableName());
					//System.out.println(sn.getTableName() + ": "+ nfs.getString("Score"));
					int curPos = curCur.get(sn) + 1;
					step = getStep(sn, topKAlgorithm, curPos, nonFreeSets);
					if (!sol.query.isTreePipeline() || sol.isHead()) {
						nfs.absolute(curPos);
						int lb = 0;
						int ub = 5000000;
						lb = curPos;
						ub = curPos + step - 1;
						if (stmt != null) {
							((PreparedStatement)stmt).setInt(paraNum, lb);
							((PreparedStatement)stmt).setInt(paraNum+1, ub);
						} else if (sol.stmt != null) {
							sol.stmt.setInt(paraNum, lb);
							sol.stmt.setInt(paraNum+1, ub);
						}
						paraNum = paraNum + 2;
						//System.out.println(sn.getTableName()+": "+lb+"-"+ub+"  ");

						qsize = qsize * step;
					}
				}	

			}
			//System.out.println();
		}
		return qsize;
//		return step;
//		System.out.println();		
	}

	private int getStep(GraphNode sn, int topKAlgorithm, int curPos, HashMap<String, ResultSet> nonFreeSets) throws SQLException {
//		curPos start from 1
		int offset = 0;
		switch (topKAlgorithm) {
		case Global.QPIPELINE_SBS:
			offset = global.INITIAL_STEP;
			break;

		case Global.GPIPELINE:
			if (curPos == 1) {
				offset = sol.curCur.get(sn) + 1;
				if (offset == 1) offset = global.INITIAL_STEP;
			}	    
			else offset = global.INITIAL_STEP;
			break;

		case Global.QPIPELINE_2LEVEL:

		case Global.TREEPIPELINE_BFS_2LEVEL:
			offset = 1;		//must bound by score
			break;

		}
		//  if (offset > 500) offset = 500;

		if (global.SCORE_RANGE) {		//score boundary (blocks)
			int offset2 = offset;	//>= 1
			ResultSet rs = nonFreeSets.get(sn.getTableName());
			Double neSco = 0.0;
			Double nextSco = -1.0;
			int privCur = curPos;	//>= 1
			boolean stop = false;
			if (rs.absolute(privCur + offset2 -1)) {
				neSco = rs.getDouble("Score");					//hard coded!!!!!!!!!
				String scoreboundKey = sn.getTableName()+Double.toString(neSco);

				if (sol.query.scorebound.containsKey(scoreboundKey)) {
					offset2 = sol.query.scorebound.get(scoreboundKey) - privCur + 1;
				}
				else {
					while ((!stop) && rs.next()) {
						nextSco = rs.getDouble("Score");			//hard coded!!!!!!!!!
						if (nextSco.compareTo(neSco)!=0) {
							stop = true;
						}    
						else {
							//System.out.print("+");
							//neSco = nextSco;
							offset2++;
						}    
					}

					sol.query.scorebound.put(scoreboundKey, privCur+offset2 - 1);
				}
			}
			else {
				rs.last();
				//neSco = rs.getDouble("Score");
				offset2 = rs.getRow() - privCur + 1;
			}

			if (offset2 - offset > global.SCORE_MAX_OFFSET) {
				offset = offset + global.SCORE_MAX_OFFSET;
			}
			else {
				offset = offset2;
			}

		}
		else {		//not score bound
			ResultSet rs = nonFreeSets.get(sn.getTableName());
			int privCur = curPos;
			if (!rs.absolute(privCur + offset - 1)) {
				rs.last();
				//neSco = rs.getDouble("Score");
				offset = rs.getRow() - privCur + 1;
			}
		}    
		length.put(sn, offset);
//		System.out.println(sn.getNonFreeTableName()+":"+offset);
		return offset;
	}	

}

