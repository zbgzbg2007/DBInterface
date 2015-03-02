package discover;

import java.util.*;
import java.sql.*;
public class TPSolution extends Solution implements Comparable{	//(tree structured) CN for tree pipeline algorithm
//	------------------for tree pipeline-------------------------
	TreeMap<String, TodoItem> todoMap;

	ArrayList<SolutionEdge> ppEdges;
	SolutionEdge epEdge;

	public TodoItem getTodo(int[] c) {
		StringBuffer cString = new StringBuffer();
		for (int i=1; i <= size(); i++) {
			if (c[i] >= 0) cString.append(c[i]).append(" ");
		}
		if (todoMap.containsKey(cString.toString())) {
			return todoMap.get(cString.toString());
		}
		return null;
	}

	public void reset() throws SQLException{
		super.reset();
		todoMap.clear();
	}
	public int compareTo(Object t) {
		int c0 = (int)(size() - ((TPSolution)t).size());
		if (c0 != 0) return c0;
		int c = (int)(sumNonFreeTuple - ((TPSolution)t).sumNonFreeTuple);
		if (c != 0) return c;
		return ((Solution)t).getKeyString().compareTo(getKeyString());
	}
	public TPSolution(Query _query) {
		super(_query);
		init();
	}

	public TPSolution(SchemaGraph s) {
		super(s);
		init();
	}
	public void init() {
		todoMap = new TreeMap<String, TodoItem>();
		ppEdges = new ArrayList<SolutionEdge>();
		epEdge = null;

	}
	public TPSolution(TPSolution X) {
		super(X);
		todoMap = new TreeMap<String, TodoItem> ();
		ppEdges = new ArrayList<SolutionEdge>();
		epEdge = null;
	}
	/*
	protected boolean hasVilidTodoItem(TodoItem todo) {		//check duplication
		if (todoMap == null | todoMap.size() == 0) return true;
		
		Iterator<SolutionEdge> i = ppEdges.iterator();
		SolutionEdge pe = i.next();
		while (i.hasNext()) {
			SolutionEdge e = i.next();
			if (e.isValid() && pe.pSol == e.pSol && 
					todo.curCur.get(pe.nonFreeNode) < todo.curCur.get(e.nonFreeNode)) 
				return false;
			pe = e;
		}
		return true;
	}*/
	protected SolutionEdge createSolutionEdge(GraphEdge e, HashMap<String, TPSolution> keyStringIndex) {
//two partitions divided by the graph edge between n(non-free) and n2
		SolutionEdge solEdge = new SolutionEdge(this);

		Graph tmpGraph = new Graph(this);
		GraphNode n;
		GraphNode n2;
		if (e.pNode.isNonFree()) {
			n = tmpGraph.copy(e.pNode);
			n2 = tmpGraph.copy(e.fNode);
		} else {
			n = tmpGraph.copy(e.fNode);
			n2 = tmpGraph.copy(e.pNode);
		}
		GraphEdge nEdge = n.getEdge(n2);
		GraphEdge n2Edge = n2.getEdge(n);

		n.deleteEdge(nEdge);
		n2.deleteEdge(n2Edge);
		//tmpGraph.deleteEdge(e);
		tmpGraph.deleteComponent(n2);
		tmpGraph.setKeyString();
		TPSolution pp = keyStringIndex.get(tmpGraph.getKeyString());

		Partition p = solEdge.newPartition(pp);
		p.nodeMapping = tmpGraph.getNodeMapping(pp, size());
		
		tmpGraph = new Graph(this);
		if (e.pNode.isNonFree()) {
			n = tmpGraph.copy(e.pNode);
			n2 = tmpGraph.copy(e.fNode);
		} else {
			n = tmpGraph.copy(e.fNode);
			n2 = tmpGraph.copy(e.pNode);
		}
		nEdge = n.getEdge(n2);
		n2Edge = n2.getEdge(n);

		n.deleteEdge(nEdge);
		n2.deleteEdge(n2Edge);
		
		tmpGraph.deleteComponent(n);
		n.addEdge(nEdge);
		n2.addEdge(n2Edge);

		tmpGraph.deleteLeg(n, solEdge.linkNodes);
		tmpGraph.setKeyString();
		pp = keyStringIndex.get(tmpGraph.getKeyString());
		p = solEdge.newPartition(pp);
		p.nodeMapping = tmpGraph.getNodeMapping(pp, size());
		

		return solEdge;
		
	}
	protected void initPP(HashMap<String, TPSolution> keyStringIndex) {	//find PPs; build CN Graph
//		System.out.println(keyString);		
		ppEdges.clear();
		if (ksize() == 1) return;
		

		for (GraphNode n: nodes) {
			for (GraphEdge e: n.getEdges()) {
				GraphNode n2 = e.getNeighbor(n);			
			//for (GraphNode n2: n.getLinkedNodes()) {
				//if (n.isNonFree() && ((n2.isNonFree() && n.id > n2.id)| (!n2.isNonFree()))) {
				if ((n.isNonFree()|n2.isNonFree()) && n.id > n2.id) {
					SolutionEdge solEdge;
					//if (n.isNonFree()) 
					solEdge = createSolutionEdge(e, keyStringIndex);
					//else solEdge = createSolutionEdge(n2, n, keyStringIndex);
					boolean dup = false;
					for (SolutionEdge ee: ppEdges) {
						if (ee.compareTo(solEdge) == 0) {
							dup = true;
//System.out.println("!!!!!!!!!!!!!"+ee.sol);
							break;
						}
					}
					if (!dup) {
						ppEdges.add(solEdge);
					}
//System.out.println(keyString+" is PP of "+tmpGraph.keyString+ " "+(pp == null));
				}
			}
		}
//System.out.println(keyString+" has "+ppEdges.size()+" edges.");		
		//Arrays.sort(ppEdges.toArray());
		//List list = Collections.synchronizedList(ppEdges);
	}

	public ArrayList<SolutionEdge> getShortestPath(int[] curCurList) throws SQLException{
		//get all todo items in this run; used when ADAPTIVE_EP = true
		HashMap<SolutionEdge, ArrayList<SolutionEdge>> partitions;
		partitions = new HashMap<SolutionEdge, ArrayList<SolutionEdge>>();
		SolutionEdge best = null;
		
		if (isHead()) return new ArrayList<SolutionEdge>();
		boolean pruned = false;
		for (SolutionEdge e: ppEdges) {	
		
			if (e.isValid() && !pruned) {
				ArrayList<SolutionEdge> partitionsWithE = new ArrayList<SolutionEdge>();
				partitionsWithE.add(e);

				for (Partition p: e.pList) {
					if (!pruned) {
					
						int[] ppCurCurList = p.getPPCurCurVal(curCurList);
						TodoItem pTodo = p.pSol.getTodo(ppCurCurList);
						if (pTodo != null && pTodo.pResult != null) {
							if (pTodo.pResult.isEmpty()) {
								pruned = true;
							}	
						} else {
							ArrayList<SolutionEdge> pPartitions = p.pSol.getShortestPath(ppCurCurList);
							if (pPartitions == null) {
								pruned = true;
							} else {
								partitionsWithE.addAll(pPartitions);
							}
						}
					}
				}
				
				
				if (!pruned) {
					partitions.put(e, partitionsWithE);
				
					if (best == null || partitions.get(best).size() > partitionsWithE.size()) {
				
						best = e;
					}	
				} else best = null;
			}	
		}
		if (best == null) return null;
		return partitions.get(best);
	}
	public ArrayList<SolutionEdge> getStaticPath(int[] curCurList) throws SQLException {
		//get all todo items in this run; used when ADAPTIVE_EP = false
		ArrayList<SolutionEdge> partition = new ArrayList<SolutionEdge>();
		boolean pruned = false;
		for (Partition p: epEdge.pList) {
			if (!pruned) {
		
				int[] pCurCurList = p.getPPCurCurVal(curCurList);
				TodoItem epTodo = p.pSol.getTodo(pCurCurList);
				if (epTodo != null && epTodo.pResult != null) {
					if (epTodo.pResult.isEmpty()) {
						pruned = true;			//pruned		
					}
				} else {
					ArrayList<SolutionEdge> pPartition = getStaticPath(pCurCurList);
					if (pPartition == null) {
						pruned = true;
					} else {
						partition.addAll(pPartition);
					}
				}
			}	
		}
		if (pruned) return null;
		return partition;
		
	}
	


	protected void initEP() {		//find EP
		long maxSize = 0;
		SolutionEdge maxEdge = null;
		if (!isHead()) {
			for (SolutionEdge e: ppEdges) {
				for (Partition p: e.pList) {
					if (p.pSol.isHead() && e.isValid()) {
						long size = query.getNonFreeTupleSize(p.pSol.getNodes().get(0).getTableName());
						if (size > maxSize) {
							maxSize = size;
							maxEdge = e;
						}
					}	
				}
			}	
			maxEdge.setEpEdge();
		}	
	}


}
