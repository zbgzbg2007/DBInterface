package discover;  

//import java.util.*;


public class GraphEdge {
//	final String SEPARATOR = ":";

	// help to record join conditions of FK->PKs
	protected GraphNode pNode;
	protected GraphNode fNode;
	protected boolean in;
	private String fCol;
	
	public GraphEdge(GraphNode _pNode, GraphNode _fNode, boolean _in, String _fCol) {	
		pNode = _pNode;
		fNode = _fNode;
		in = _in;
		fCol = _fCol;
	}
	public GraphEdge(GraphEdge X) 
	{
		pNode = X.pNode;
		fNode = X.fNode;
		in = X.in;
		fCol = X.fCol;		
		
	}
	protected StringBuffer getLabel() {
		StringBuffer DFCStmp = new StringBuffer();
		if (in) 
			DFCStmp.append("$");
		else DFCStmp.append("#");
		DFCStmp.append(getFCol());
		return DFCStmp; 
	}
	protected GraphNode getNeighbor(GraphNode n) {
		if (n == pNode) {
			return fNode;
		}
		return pNode;
	}
	public String getFCol() {
		return fCol;
	}
	public String toString() {
		return pNode.id+":"+pNode.getTableName()+"<--"+fNode.id+":"+fNode.getTableName();
	}
	// generate a unique reprsentation of two strings: we put the smaller one first
//	private String genUniqKey(String fromTable, String toTable) 
//	{
//		return fromTable + SEPARATOR + toTable;
//	}

	// generate a unique reprsentation of two strings: we put the smaller one first
/*	private String genUniqVal(String fromTable, String toTable, String fromCol, String toCol) 
	{
		return fromCol + SEPARATOR + toCol;
	}*/

	// fromXXX is the foreign key table; 
	/*public void addEdge(String fromTable, String toTable, String fromCol, String toCol) 
	{
		String key = genUniqKey(fromTable, toTable);
		String val = genUniqVal(fromTable, toTable, fromCol, toCol);
		allEdges.put(key, val);
	}*/

/*	public String[] getEdgeData(String fromTable, String toTable) 
	{
		String key1 = genUniqKey(fromTable, toTable);
		String key2 = genUniqKey(toTable, fromTable);
		if (allEdges.containsKey(key1) || allEdges.containsKey(key2)) {
			String[] ret = new String[4];
			String[] vals = new String[2];
			ret[0] = fromTable;
			ret[1] = toTable;
			if (allEdges.containsKey(key1)) {
				vals = allEdges.get(key1).split(SEPARATOR);
				ret[2] = vals[0];
				ret[3] = vals[1];
			}
			else {
				vals = allEdges.get(key2).split(SEPARATOR);
				ret[2] = vals[1];
				ret[3] = vals[0];
			}
			//System.out.println(ret[0] + ret[2]);
			return ret;
		} else {
			return null;
		}
	}
	
	public boolean containFP(String fromTable, String toTable) {
		String key = genUniqKey(fromTable, toTable);
		return allEdges.containsKey(key);
	}

	public String getTieCol(String fromTable, String toTable, String fromTableAlign) {
		String[] data = getEdgeData(fromTable, toTable);
		return (fromTableAlign+"_"+data[2]);
	}
	public String getKnotCol(String fromTable, String toTable, String toTableAlign) {
		String[] data = getEdgeData(fromTable, toTable);
		return (toTableAlign+"_"+data[3]);
	}*/
	public String genSQLCondition() {
		StringBuffer sql = new StringBuffer();
		sql.append(pNode.getAlignName()).append(".").append(pNode.getPrimaryKey()).append("=") 
			.append(fNode.getAlignName()).append(".").append(fCol);
		return sql.toString();
	}
	/*
	public String genSQLCondition(String fromTable, String toTable, String fromTableAlign, String toTableAlign) {
		String[] data = getEdgeData(fromTable, toTable);
		String[] colsFromTable = data[2].split(", ");
		String[] colsToTable = data[3].split(", ");
		StringBuffer sql = new StringBuffer("");
		for (int i = 0; i < colsFromTable.length; i++) {
			sql.append(fromTableAlign + "." + colsFromTable[i] + "=" + toTableAlign + "." + colsToTable[i] + " AND ");
		}
		return sql.substring(0, sql.length() - 5);
//		String sql = fromTableAlign + "." + data[2] + "=" + toTableAlign + "." + data[3];
//		return sql;
	}*/

}
