package discover;

import java.util.*;
import java.sql.*;

public class PartialResult {
	TodoItem todo;
	TPSolution sol;

	public HashMap<String, HashMap<Integer, ArrayList<Integer> > > indexes;

	public ResultSet resultset;            	//!! this is the resultSet that contains "new attributes" and their values. 
	public int prevRsId;		 				//point to the previous results id, for the recursive getNextJtt!
	public int size;		//when resultset is deleted,size can be used for checking

	public PartialResult(TodoItem _todo) {
		todo = _todo;
		sol = todo.sol;
		indexes = new HashMap<String, HashMap<Integer, ArrayList<Integer> > >();
		size = -1;
	}


	public boolean isEmpty() {
		boolean empty = false;
		try {
			if (resultset == null) {
				if (size <= 0) empty = true;
			} else if (Global.resultSetSize(resultset) == 0) {
				empty = true;
			}
		} catch (SQLException e) 
		{empty = true;}
		return empty;
	}


	public boolean containsAllKeys(int[] tfsList) {
		for (int tf: tfsList) {
			if (tf==0) return false;
		}
		return true;
	}

	public StringBuffer getResultData(Integer rsId, Results allResults) throws SQLException {
		StringBuffer res = getLinkNodesString(rsId, allResults);
		return res;
	}

	public void print(ArrayList<Integer> jtt) {
		System.out.print("jtt:");
		for (Integer rsId: jtt) {
			System.out.print(rsId+" ");
		}
		System.out.println();
	}

	/*
	public StringBuffer getResultData(ArrayList<Integer> jtt, Results allResults) throws SQLException {
//		print(jtt);
		StringBuffer res = new StringBuffer("");
		
		LinkedList<TodoItem> q = new LinkedList<TodoItem>();
		q.add(todo);
		while (!q.isEmpty()) {
			TodoItem curTodo = q.poll();
			Integer rsId = jtt.get(i);
			//curTodo.pResult.resultset.absolute(rsId.intValue());
			res.append(curTodo.pResult.getLinkNodesString(rsId.intValue(), allResults));
			for (TodoItem todo: curTodo.epTodos) {
				q.add(todo);
			}	
			
		}
		return res;
	}
*/
	private StringBuffer getLinkNodesString(int rsId, Results allResults) throws SQLException {
		//allResults.uTime.start();

		StringBuffer res = new StringBuffer();
//		System.out.println("getCompNodesString "+rsId);
		//resultset.absolute(rsId);
		for (GraphNode n: sol.epEdge.getLinkNodes()) {
			//res.append("<").append(n.getTableName()).append(" ");
			res.append("<");
			//String[] allCols = schema.allCols.get(n.getNonFreeTableName()).split(" ");
			String tbl_name = n.getTableName();
			String tbl_name_q = n.getAlignName();
			String alignCol;
			String cols = "";
			if (sol.schema.primaryKey.containsKey(tbl_name)) {
				cols = sol.schema.primaryKey.get(tbl_name);
			}
			//String[] allCols = sol.schema.allKeys.get(tbl_name).split(",");
			String fkeys = sol.schema.allKeys.get(tbl_name);
			if (!fkeys.equals("")) {
				cols = cols + ","+fkeys;
			}	
//			System.out.println(cols);	
			for (String col: cols.split(",")) {
				alignCol = tbl_name_q+"_"+col;
//				System.out.println(res+" + "+alignCol);
				res.append(alignCol).append("=").append(resultset.getString(alignCol)).append(" ");
				//res.append(results.getString(alignCol)).append(" ");
			}
			if (n.isNonFree()) {
				int rowN = resultset.getInt(tbl_name_q+"_RowN");
				//	to get the text columns from original non-free tuple set
				ResultSet nonFreeSet = sol.query.nonFreeSets.get(n.getTableName());
				nonFreeSet.absolute(rowN);
				cols = sol.schema.textCols.get(tbl_name);
				for (String col: cols.split(",")) {						
					//alignCol = tbl_name_q+"_"+col;
					res.append(col).append("=").append(nonFreeSet.getString(col)).append(" ");
				}						
			}

			res.append(">");
		}

		//allResults.uTime.stop();		                      
		return res;
	}

}
