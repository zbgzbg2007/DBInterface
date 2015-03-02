package discover;
//import java.sql.*;
import java.util.*;



public class GraphNode implements Comparable {
	private String tableName;
	//private LinkedList<GraphNode> otherNodes; // adjacent list
	
	//private TreeMap<GraphNode, String> otherNodes; //adjacent nodes(directed), with foreign key column
	private LinkedList<GraphEdge> edges;	
	protected GraphNode parent;			//if it is a tree
	protected GraphEdge parentEdge;
	
	protected byte[] childrenId;
	protected int subtreeHeight;
	
	private boolean nonFree;
	protected boolean matchStar;
	
	protected String DFCS;
	protected Byte id;

	public SchemaGraph schema;
	public Graph graph;		//not null if the node belongs to a graph
	
	protected void setParent(GraphNode n) {
		parent = n;
		parentEdge = getEdge(parent);
	}
	public void reset() {
		nonFree = false;
	}
	public GraphNode(byte _id, String name, SchemaGraph _schema) {
		id = _id;
		setTableName(name);
		edges = new LinkedList<GraphEdge>();
		DFCS = null;
		childrenId = null;
		matchStar = false;
		nonFree = false;
		schema = _schema;
		graph = null;
	}
	public GraphNode(byte _id, GraphNode n, Graph s) {
		id = _id;
		setTableName(n.getTableName());
		edges = new LinkedList<GraphEdge>();		//all links are discarded
		DFCS = n.DFCS;
		
		matchStar = n.matchStar;
		nonFree =  n.nonFree;
		//setId(n.getId());
		
		childrenId = null;
		if (n.childrenId != null) 
			childrenId = (byte[])n.childrenId.clone();
		subtreeHeight = n.subtreeHeight;
		
		schema = n.schema;
		if (s != null) graph = s;
		else graph = n.graph;
	}  
	
	public int compareTo(Object ri) {
		//return ((GraphNode)ri).id.intValue() - id.intValue();
		int comp = (DFCS.compareTo(((GraphNode)ri).DFCS));
		if (comp != 0) return comp;
		return id.intValue() - ((GraphNode)ri).id.intValue();
	}
	public String getTextCols() {
		return schema.textCols.get(tableName);
	}
	public String getPrimaryKey() {
		return schema.primaryKey.get(tableName);
	}
 
	public void setSchemaGraph(SchemaGraph _schema) {
		schema = _schema;
	}        
	public Byte getId() {
		return id;
	}    
	public void setId(byte i) {		
		/*if (parent != null && parent.childrenId != null) {
			for (int j = 0; j < parent.childrenId.length; j++) {
				if (parent.childrenId[j] == id) parent.childrenId[j] = i;
			}
		}*/
		id = new Byte(i);
	}

	public boolean isNonFree() {
		return nonFree;
	}
	public void convertToNonFree() {
		nonFree = true;
	}
	public String getTableName() {
		return tableName;
	}   
	public String getTableNameQ(Query q) {
		return tableName + Global.NON_FREE_TABLE_NAME + q.uniqueId;
	}
	public String getTableNameTmp(Query q) {
		return getTableNameQ(q) + Global.TMP_TABLE_NAME;
	}
	public String getNonFreeTableName(Query q) {
		if (!nonFree) {
			return tableName;
		}	
		else {
			if (q != null)	return tableName+Global.NON_FREE_TABLE_NAME + q.uniqueId;
			return tableName+Global.NON_FREE_TABLE_NAME;
		}	
	}
	public String getAlignName() {
		return "T"+id.toString();
	}
	public void setTableName(String name) {
		tableName = new String(name);
	}
	public double getAvgDl() {
		return schema.avgDl.get(tableName);
	}

	public LinkedList<GraphEdge> getEdges() {
		return edges;
	}
	public LinkedList<GraphNode> getLinkedNodes() {
		LinkedList<GraphNode> n = new LinkedList<GraphNode>(); 
	
		for (GraphEdge e: edges) {
			n.add(e.getNeighbor(this));
		}
		return n;
	}
	public GraphNode[] getChildren() {
		LinkedList<GraphNode> n = new LinkedList<GraphNode>();
		
		for (GraphEdge e: edges) {
			GraphNode neighbor = e.getNeighbor(this);
			if (parent == null || (parent != null && neighbor != parent)) {
				n.add(neighbor);
			}
		}
		return n.toArray(new GraphNode[n.size()]);
	}
	public GraphNode getFirstLinkedNode() {
		if (edges.size() > 0)
			return edges.get(0).getNeighbor(this);
		return null;
	}
	public GraphEdge getFirstEdge() {
		if (edges.size() > 0)
			return edges.get(0);
		return null;
	}
	public void mergeTo(GraphNode n) {		//before deleting this node but keeping n
		for (GraphEdge e: edges) {
			GraphNode neighbor = e.getNeighbor(this);
			GraphEdge revEdge = neighbor.getEdge(this);
			if (e.pNode == this) {
				e.pNode = n;
				revEdge.pNode = n;
			} else {
				e.fNode = n;
				revEdge.fNode = n;
			}
			n.edges.add(e);
//			if (neighbor.parent == this) {
//				neighbor.setParent(n);
//			}
		}
		//n.edges.addAll(edges);
	}
	public int getLinkedNodesSize() {
		return edges.size();
	}	
	public boolean isLeaf() {
		return (edges.size() == 1);
	}
	public void setEdges(LinkedList<GraphEdge> _edges) {
		edges = _edges;
	}	
	public void addEdge(GraphEdge e) { 
		edges.add(e);
	}
	public GraphEdge getEdge(GraphNode n) {
		for (GraphEdge e: edges) {
			if (e.pNode == n | e.fNode == n) return e;
		}
		return null;
	}
	public GraphEdge getParentEdge() {
		return parentEdge;
	}
	public int getEdgeNum() {
		return edges.size();
	}
	public void deleteEdge(GraphEdge e) {
		edges.remove(e);
	}

	public void convertToFree() {
		nonFree = false;
	}
	protected GraphNode getPrevLegRoot() {	
		GraphNode n = this;
		if (graph.size() > 1) {
			while (n.getEdgeNum() <= 2 && n.parent != null)
				n = n.parent;
			n = graph.tableIdDict.get(n.childrenId[n.childrenId.length - 1]);	//ith last child
		}
		return n;
	}


	protected Graph getLastShortestLeg2() {
		LinkedList<GraphNode> q = new LinkedList<GraphNode>();
		HashMap<GraphNode, Integer> height = new HashMap<GraphNode, Integer>();
		q.add(this);
		height.put(this, 0);
		GraphNode lastLeaf = null;
		int shortestLegLength = -1;
		
		while (!q.isEmpty()) {
			GraphNode n = q.poll();
			if (shortestLegLength > 0 && height.get(n) > shortestLegLength) 
				break;
			if (n.isLeaf()) {
				lastLeaf = n;
				shortestLegLength = height.get(n);
			}
			if (lastLeaf == null) {
				for (byte cid: n.childrenId) {
					GraphNode c = graph.tableIdDict.get(cid);
//System.out.println(c.id+" "+c.getTableName()+graph.size());					
					q.add(c);
					height.put(c, height.get(n)+1);				
				}
			}
		}
		Graph lastShortestLeg = new Graph();
		if (lastLeaf != null) {
			while (lastLeaf != this) {
//System.out.println(lastLeaf.getId());
//System.out.println("P:"+lastLeaf.parent.getId());				

				lastShortestLeg.nodes.add(lastLeaf);
				
				lastLeaf = lastLeaf.parent;
			}
			lastShortestLeg.nodes.add(this);
			lastShortestLeg.setRoot(this);
		}
		return lastShortestLeg;
	}
	public ArrayList<Graph> getLegs(byte minId, int maxSize, GraphNode prevLegRoot, Query query, Time timeGetLegs) {
		timeGetLegs.start();
		ArrayList<Graph> legs = new ArrayList<Graph>();
		LinkedList<Graph> q = new LinkedList<Graph>();
		
		Graph singleNodeGraph = new Graph();
		GraphNode singleNode = new GraphNode(minId, this, singleNodeGraph);
		//singleNode.convertToFree();
		singleNode.setId(minId);
		singleNodeGraph.addNode(singleNode);
		singleNodeGraph.setRoot(singleNode);
		singleNode.setParent(null);
		singleNodeGraph.setDFCS(singleNode);
		q.add(singleNodeGraph);
		//if (query != null && query.getMatchedKeywordSize(getTableName()) > 0
		//		|| query == null && schema.textCols.containsKey(getTableName())) {
		//	//singleNode.convertToNonFree();
		//	legs.add(singleNodeGraph);//!!! used for changing a free tuple set into nonfree!!!
		//}	
		
		while (!q.isEmpty()) {
			Graph g = q.poll();
			int compare = -1;
			if (g.size() == 1) compare = 0;
			//if new leg is shorter than prevLeg, compare = -1; otherwise compare from leg root
			if (prevLegRoot != null && g.size() > 1 && g.size() == prevLegRoot.subtreeHeight + 1) {
				compare = g.compareFront(prevLegRoot, g.getNodes().get(1));
			}
			if (g.size() < maxSize && compare <= 0) {
				GraphNode n;
				//if (g.size() == 0) n = this;			//
				n = g.nodes.get(g.size()-1);		//the last node
				GraphNode nodeInSchema = schema.tableNameDict.get(n.getTableName());	
				for (GraphEdge e: nodeInSchema.getEdges()) {
				//for (GraphNode neighbor: nodeInSchema.getLinkedNodes()) {
					if (!g.containsPFP(n, nodeInSchema, e)) {
						GraphNode neighbor = e.getNeighbor(nodeInSchema);
						if (query != null && query.getMatchedKeywordSize(neighbor.getTableName()) > 0
							|| query == null && schema.textCols.containsKey(neighbor.getTableName())) {
							Graph newLeg = new Graph(g);
							newLeg.grow((byte)(minId + g.size()), newLeg.copy(n), nodeInSchema, e, true);
							if (prevLegRoot == null) {
								legs.add(newLeg);
							} else {
								GraphNode newLegRoot = newLeg.getNodes().get(1);
								newLeg.buildDFCF(newLegRoot);
								//if (newLeg.inOrder(prevLegRoot, newLegRoot)) {
								if (compare < 0 || 
										compare == 0 && newLeg.inOrder(prevLegRoot, newLegRoot)) {
									legs.add(newLeg);
								}
							}								
						}
					//	if (query != null && neighbor.matchStar
					//		|| query == null && schema.textCols.containsKey(neighbor.getTableName())) {
					//		Graph newLeg = new Graph(g);
					//		newLeg.grow(newLeg.copy(n), nodeInSchema, e, false);
					//		if (prevLeg == null | (prevLeg != null && newLeg.greaterOrEqual(prevLeg))) {
					//			legs.add(newLeg);
					//		}
					//	}	
						if (g.size() < maxSize - 1) {		
							Graph newPartLeg = new Graph(g);					
							newPartLeg.grow((byte)(minId + g.size()),newPartLeg.copy(n), nodeInSchema, e, false);
							q.add(newPartLeg);
							if (query != null && query.getMatchedKeywordSize(neighbor.getTableName()) > 0
							|| query == null && schema.textCols.containsKey(neighbor.getTableName())) {
								Graph newPartLeg2 = new Graph(g);
								newPartLeg2.grow((byte)(minId + g.size()),newPartLeg2.copy(n), nodeInSchema, e, true);
								q.add(newPartLeg2);
							}
						}
					}	
				}	
			}
		}
		timeGetLegs.stop();
		return legs;
	}
	public String toString() {
		return getTableName();
	}
}
