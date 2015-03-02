package discover;

import java.util.HashMap;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.TreeSet;
import java.sql.*;
import java.util.*;

public class SolutionTree {
	ArrayList<TPSolution> sols;

	SchemaGraph schema;
	Query query;
	private Global global;

	public SolutionTree(Query _query) {
		query = _query;
		sols = new ArrayList<TPSolution>();
		schema = query.schema;
		global = schema.global;
	}
	public SolutionTree(SchemaGraph _schema) {
		query = null;
		schema = _schema;
		sols = new ArrayList<TPSolution>();
		global = schema.global;
	}

	protected void initPP() throws SQLException {
		HashMap<String, TPSolution> keyStringIndex = new HashMap<String, TPSolution>();
		for (TPSolution s: sols) {
			keyStringIndex.put(s.getKeyString(), s);
		}
//		int i = 0;
		for (TPSolution s: sols) {
//System.out.println(i+": "+s+" "+s.size()+" "+s.nodes.get(0).getEdges().size());
//i++;
			s.initPP(keyStringIndex);			
		}
	}	
	protected void initEP() {
		for (TPSolution s: sols) {
			s.initEP();
		}
	}
	public PriorityQueue<TodoItem> getInitTodoList() throws SQLException {
		PriorityQueue<TodoItem> todoList = new PriorityQueue<TodoItem>();
		TodoItem todo = null;
		for (TPSolution sol: sols) {
			sol.reset();
			if (query.topKAlgorithm == Global.GPIPELINE) {

				sol.curCur = new HashMap<GraphNode, Integer>();
				sol.initCur();
			}
			todo = new TodoItem(sol);
			todo.initCur();
			todo.possibleMax = todo.getUpperScore(query.nonFreeSets);
			sol.todoMap.put(todo.getCurCurString().toString(), todo);

			todoList.add(todo);
System.out.println(todo);
			if (query.isTreePipeline() && !global.ADAPTIVE_EP) {
				for (Partition p: sol.epEdge.pList) {
					todo.linkToEpTodo(p, todoList);
				}	
			}
		}	
		return todoList;
	}

	public int size() {
		return sols.size();
	}

	public void closeQuery() throws SQLException {
		for (TPSolution sol: sols) {
			//for (TodoItem todo: sol.todoMap.values()) {
			//	if (todo.stmt != null) todo.stmt.close();				
			//}
			//if (sol.stmt != null) sol.stmt.close();
			if (sol.stmtStr != null) sol.reset();
		}
	}

	public void scanCNG(int userCN) throws SQLException {	//find the sub-tree of all CNs for the given query 
		System.err.print("START Scaning for Networks...");
		System.out.print("START Scaning for Networks...");
		for (int i=0; i<schema.stree.sols.size(); i++) {
			TPSolution s = schema.stree.sols.get(i);
			boolean valid = true;
			if (userCN >= 0 && userCN < i) {
				valid = false;
			} 
			if (s.size() > global.searchRange) {
					valid = false;
					break;
			}	
			for (GraphNode n: s.getNodes()) {
				if (n.isNonFree() && !query.nonFreeSets.containsKey(n.getTableName())) {
					valid = false;
					break;
				}
				if (!schema.tableNameDict.get(n.getTableName()).matchStar && 
						n.isLeaf() && !n.isNonFree()) {
					valid = false;
					break;
				}
			}
			if (valid && s.ksize > 0 && !(userCN >= 0 && userCN != i)) {
				s.inResults = true;
//System.out.println("userCN: "+s.keyString);
			}
			else s.inResults = false;
			if (valid) {
				s.setQuery(query);
				sols.add(s);
			} else {
				s.setQuery(null);
			}
		}
		System.out.println(sols.size());
		System.err.println(sols.size());

	}
	/*
	public void generateValidCNG() throws SQLException{
		TreeSet<TPSolution> q = new TreeSet<TPSolution>(); // the run-time queue
		HashSet<String> existingSols = new HashSet<String>();
		Time time = new Time();
		time.start();
		for (GraphNode n: schema.getNodes()) {
			if (query != null && query.getMatchedKeywordSize(n.getTableName()) > 0 ||
					query == null && schema.textCols.containsKey(n.getTableName())) {
				TPSolution sol;
				if (query == null) {
					sol = new TPSolution(schema);
				} else {
					sol = new TPSolution(query);
				}
				GraphNode newnode = new GraphNode(n, sol);
				newnode.convertToNonFree();
				sol.addNode(newnode);
				sol.convertToMinimalSolution();
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
				existingSols.add(sol.keyString);
				q.add(sol);
			}
		} 
		System.err.print("START Searching for Networks...");
		System.out.print("START Searching for Networks...");
//		boolean stopSearch = false;
		int i = 0; 
		while (i < sols.size()) {
				TPSolution old_sol = sols.get(i++);
				if (old_sol.size() < global.searchRange) {
//System.out.println("sol- " + old_sol.keyString);
					// normal case, grow the solutions
					for (GraphNode node: old_sol.getNodes()) {

//						System.out.println("Start from table- " + node.getNonFreeTableName() );
						for (Graph leg: node.getLegs(global.searchRange-old_sol.size()+1, null, query)) { //all legs without PFP
						
							TPSolution temp_sol = new TPSolution(old_sol);

							//GraphNode oldNode = temp_sol.getNode(node.getId());
							//GraphNode newNode = new GraphNode(another_node, temp_sol);
							if (temp_sol.mergeLeg(temp_sol.copy(node), leg)) {
							
//							System.out.println(oldNode.getTableName()+"("+oldNode.hashCode()+")-"+newNode.getTableName()+"("+newNode.hashCode()+")");								
							
								temp_sol.setKeyString();
								if (!existingSols.contains(temp_sol.getKeyString())) {
//System.out.println(sols.size()+ " "+temp_sol);
									
									// found a new solution 
									//temp_sol.ksize = old_sol.ksize + 1;
									temp_sol.setGlobalWeight();
									sols.add(temp_sol);
									existingSols.add(temp_sol.keyString); // could have cached the keyString() result;
									//q.add(temp_sol);			
	//System.out.println(q.size()+" "+temp_sol);		
								}
							}	
						}
					}  
				}
				
			 
		}
//		q.clear();
		existingSols.clear();
		initPP();
		System.out.println(sols.size());
		System.err.println(sols.size());
		time.stopNPrint();
	}
	*/
	public long generateNoDupCNG() throws SQLException{
		//TreeSet<TPSolution> q = new TreeSet<TPSolution>(); // the run-time queue
		HashSet<String> existingSols = new HashSet<String>();
		Time time = new Time();
		time.start();
		
		Time timeGetLegs = new Time();

		for (GraphNode n: schema.getNodes()) {
			if (query != null && query.getMatchedKeywordSize(n.getTableName()) > 0 ||					
					query == null && schema.textCols.containsKey(n.getTableName())) {
				TPSolution sol;
				if (query == null) {
					sol = new TPSolution(schema);
				} else {
					sol = new TPSolution(query);
				}
				GraphNode newnode = new GraphNode((byte)1, n, sol);
				newnode.convertToNonFree();
				sol.addNode(newnode);
				sol.convertToMinimalSolution();
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
			}
			/*
			if (query != null && n.matchStar || 
					query == null && schema.textCols.containsKey(n.getTableName())) {
				TPSolution sol;
				if (query == null) {
					sol = new TPSolution(schema);
				} else {
					sol = new TPSolution(query);
				}
				GraphNode newnode = new GraphNode(n, sol);
				sol.addNode(newnode);
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
			}*/
		} 
		System.err.print("START CN Generation no-dup...");
		System.out.print("START CN Generation no-dup...");
//		boolean stopSearch = false;
		int i = 0; 
		while (i < sols.size()) {
				TPSolution old_sol = sols.get(i);
				if (old_sol.ksize() == 0) 
					sols.remove(i);
				else i++;

				if (old_sol.size() <= global.searchRange) {
//System.out.println("from- " + i+" "+old_sol);
//System.out.println();
					// normal case, grow the solutions
			
					for (GraphNode node: old_sol.getKnotNodes()) {
//System.out.println(node.getId());					
						int maxLegLength = global.searchRange - old_sol.size() + 1;
						//GraphNode prevLegRoot = old_sol.getNodes().get(0);
						//if (old_sol.size() > 1) {
						GraphNode prevLegRoot = node.getPrevLegRoot();
						if (prevLegRoot != null && prevLegRoot.subtreeHeight + 2 < maxLegLength) 
							maxLegLength = prevLegRoot.subtreeHeight + 2;
						//}						
						//						System.out.println("Start from table- " + node.getNonFreeTableName() );
						for (Graph leg: node.getLegs((byte)(old_sol.size()), maxLegLength, prevLegRoot, query, timeGetLegs)) { //all legs without PFP
							TPSolution temp_sol = new TPSolution(old_sol);							
							if (temp_sol.mergeLegInOrder(temp_sol.copy(node), leg, prevLegRoot)) {

								// found a new solution 
								temp_sol.setGlobalWeight();
								//if (temp_sol.ksize > 0) 
									sols.add(temp_sol);
//System.out.println(temp_sol.size()+"  "+temp_sol);
								//q.add(temp_sol);			
							}							
						}
					}  
				}
				
			 
		}
//		q.clear();
		/*
		for (Solution s: ret) {
    System.err.println(s.keyString+":"+s.tieCol);
    for (Solution c: s.children) {
        System.err.println("                "+c.keyString);
    }
}*/
		existingSols.clear();
		System.out.println(sols.size());
		System.err.println(sols.size());
		System.out.print("NoDupTime: ");
		time.stopNPrint();
		System.out.println("TimeGetLegs:"+timeGetLegs.getTime());
		
		initPP();
		return time.getTime();
	
	}
	
	public long generateCNG() throws SQLException{			//breadth first search
		TreeSet<TPSolution> q = new TreeSet<TPSolution>(); // the run-time queue
		HashSet<String> existingSols = new HashSet<String>();
		Time time = new Time();
		time.start();
		for (GraphNode n: schema.getNodes()) {
			if (query != null && query.getMatchedKeywordSize(n.getTableName()) > 0 ||
					query == null && schema.textCols.containsKey(n.getTableName())) {
				TPSolution sol;
				if (query == null) {
					sol = new TPSolution(schema);
				} else {
					sol = new TPSolution(query);
				}
				GraphNode newnode = new GraphNode((byte)1, n, sol);
				newnode.convertToNonFree();
				sol.addNode(newnode);
				sol.inResults = true; //ADDED into results
				sol.convertToMinimalSolution();
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
				existingSols.add(sol.getKeyString());
				q.add(sol);
			}
		}  
		System.err.print("START CN Generation...");
		System.out.print("START CN Generation...");
		boolean stopSearch = false;
		while (!stopSearch) {
			if (q.size()==0) {
				stopSearch = true;
			}
			else {
				TPSolution old_sol = q.first();
				q.remove(old_sol);
				if (old_sol.size() < global.searchRange) {
//System.out.println("sol- " + old_sol.keyString);
					
//if (old_sol.keyString.equals("7Cite8Item_NF9Item_NF#CrossRef$Reference8Item_NF9Item_NF#CrossRef$ItemId"))
//System.out.println("HERE");

					// normal case, grow the solutions
					for (GraphNode node: old_sol.getNodes()) {

//						System.out.println("Start from table- " + node.getNonFreeTableName() );
						GraphNode nodeInFullSchema = schema.tableNameDict.get(node.getTableName());
						
						for (GraphEdge e: nodeInFullSchema.getEdges()) {
							int candidateNum = 1;
							GraphNode another_node = e.getNeighbor(nodeInFullSchema);
							if (query != null && query.getMatchedKeywordSize(another_node.getTableName()) > 0
									|| query == null && schema.textCols.containsKey(another_node.getTableName())) {
								candidateNum = 2;
							}
							if (old_sol.containsPFP(node, nodeInFullSchema, e)) candidateNum = 0;
							for (int i = 0; i < candidateNum; i++) {
								TPSolution temp_sol = new TPSolution(old_sol);

								GraphNode oldNode = temp_sol.getNode(node.getId());
								GraphNode newNode = new GraphNode((byte)(temp_sol.size() + 1), another_node, temp_sol);
								if (i == 1) {
									newNode.convertToNonFree();
								}
//								temp_sol.addEdge(oldNode, newNode);
								temp_sol.addEdge(oldNode, newNode, e);

//								System.out.println(oldNode.getTableName()+"("+oldNode.hashCode()+")-"+newNode.getTableName()+"("+newNode.hashCode()+")");								
								temp_sol.addNode(newNode);
								temp_sol.setKeyString();
								if (!existingSols.contains(temp_sol.getKeyString())) 
										//&& !temp_sol.containsPFP(newNode, oldNode, schema.allEdges))
								{

									if (i==1 && temp_sol.convertToMinimalSolution()) {
										// found a new solution 
										temp_sol.inResults = true;
										temp_sol.setGlobalWeight();
										sols.add(temp_sol);
										existingSols.add(temp_sol.getKeyString()); // could have cached the keyString() result;
										q.add(temp_sol);
//System.out.println(temp_sol.size()+"  "+temp_sol.getKeyString()+" *");		
									}
									//else if (i!=1) {    //if the current node in non-free but there is a free leaf, the the free leaf must be append last
									else if (temp_sol.size() < global.searchRange) {
//if (temp_sol.keyString.equals("7Person8Proceeding9InProceeding_NFProceedingId#EditorId#9Proceeding_NFEditorId#"))
//System.out.println("HERR");
					
										existingSols.add(temp_sol.getKeyString()); // could have cached the keyString() result;
										q.add(temp_sol);			
									}
								}
							}
						}
					}  
				}
			}  
		}
		q.clear();
		/*for (Solution s: ret) {
    System.err.println(s.keyString+":"+s.tieCol);
    for (Solution c: s.children) {
        System.err.println("                "+c.keyString);
    }
}*/
		System.out.println(sols.size());
		System.err.println(sols.size());
		System.out.print("NaiveTime: ");
		time.stopNPrint();
		
		existingSols.clear();
		initPP();
		
		return time.getTime();		
	}

	public long generateSlowCNG() throws SQLException{			//breadth first search

		TreeSet<TPSolution> q = new TreeSet<TPSolution>(); // the run-time queue
		HashSet<String> existingSols = new HashSet<String>();
		Time time = new Time();
		Time timeGetLegs = new Time();
		time.start();
		for (GraphNode n: schema.getNodes()) {
			if (query != null && query.getMatchedKeywordSize(n.getTableName()) > 0 ||
					query == null && schema.textCols.containsKey(n.getTableName())) {
				TPSolution sol;
				if (query == null) {
					sol = new TPSolution(schema);
				} else {
					sol = new TPSolution(query);
				}
				GraphNode newnode = new GraphNode((byte)1, n, sol);
				newnode.convertToNonFree();
				sol.addNode(newnode);
				sol.convertToMinimalSolution();
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
				existingSols.add(sol.getKeyString());
				q.add(sol);
			}
		}  
		System.err.print("START CN Generation...");
		System.out.print("START CN Generation...");
		boolean stopSearch = false;
		while (!stopSearch) {
			if (q.size()==0) {
				stopSearch = true;
			}
			else {
				TPSolution old_sol = q.first();
				q.remove(old_sol);
				if (old_sol.size() < global.searchRange) {
					
					
					
					
					
					
					// normal case, grow the solutions
					for (GraphNode node: old_sol.getKnotNodes()) {

						GraphNode prevLegRoot = node.getPrevLegRoot();
						for (Graph leg: node.getLegs((byte)(old_sol.size()), 2, prevLegRoot, query, timeGetLegs)) { //all legs without PFP
							TPSolution temp_sol = new TPSolution(old_sol);							
							if (temp_sol.mergeLegInOrder(temp_sol.copy(node), leg, prevLegRoot)) {
								if (temp_sol.convertToMinimalSolution()) {
								// found a new CN
									temp_sol.setGlobalWeight();
								//if (temp_sol.ksize > 0) 
									sols.add(temp_sol);									
								}
								q.add(temp_sol);
								
							}
						}
					}  
				}
			}  
		}
		q.clear();
		/*for (Solution s: ret) {
    System.err.println(s.keyString+":"+s.tieCol);
    for (Solution c: s.children) {
        System.err.println("                "+c.keyString);
    }
}*/
		System.out.println(sols.size());
		System.err.println(sols.size());
		System.out.print("NaiveTime: ");
		time.stopNPrint();
		
		existingSols.clear();
		initPP();
		
		return time.getTime();		
	}


	public long generateAllCNG(ArrayList<String> nonFreeSets) throws SQLException{			
	//New added; generate CNG according to nonFreeSets
		TreeSet<TPSolution> q = new TreeSet<TPSolution>(); // the run-time queue
		HashSet<String> existingSols = new HashSet<String>();
		Time time = new Time();
		time.start();
		for (GraphNode n: schema.getNodes()) {
			if (nonFreeSets.contains(n.getTableName())) {
				TPSolution sol;
				sol = new TPSolution(schema);
				GraphNode newnode = new GraphNode((byte)1, n, sol);
				newnode.convertToNonFree();
				sol.inResults = true; //ADDED into results
				sol.addNode(newnode);
				sol.convertToMinimalSolution();
				sol.setKeyString();
				sol.setGlobalWeight(); //compute treeWeight and sumidf
				sols.add(sol);
				existingSols.add(sol.getKeyString());
				q.add(sol);
			}
		}  
		System.err.print("START CN Generation...");
		System.out.print("START CN Generation...");
		boolean stopSearch = false;
		while (!stopSearch) {
			if (q.size()==0) {
				stopSearch = true;
			}
			else {
				TPSolution old_sol = q.first();
				q.remove(old_sol);
				if (old_sol.size() < global.searchRange) {
//System.out.println("sol- " + old_sol.keyString);
					
//if (old_sol.keyString.equals("7Cite8Item_NF9Item_NF#CrossRef$Reference8Item_NF9Item_NF#CrossRef$ItemId"))
//System.out.println("HERE");

					// normal case, grow the solutions
					for (GraphNode node: old_sol.getNodes()) {

//						System.out.println("Start from table- " + node.getNonFreeTableName() );
						GraphNode nodeInFullSchema = schema.tableNameDict.get(node.getTableName());
						
						for (GraphEdge e: nodeInFullSchema.getEdges()) {
							int candidateNum = 1;
							GraphNode another_node = e.getNeighbor(nodeInFullSchema);
							if (query != null && query.getMatchedKeywordSize(another_node.getTableName()) > 0
									|| query == null && schema.textCols.containsKey(another_node.getTableName())) {
								candidateNum = 2;
							}
							if (old_sol.containsPFP(node, nodeInFullSchema, e)) candidateNum = 0;
							for (int i = 0; i < candidateNum; i++) {
								TPSolution temp_sol = new TPSolution(old_sol);

								GraphNode oldNode = temp_sol.getNode(node.getId());
								GraphNode newNode = new GraphNode((byte)(temp_sol.size() + 1), another_node, temp_sol);
								if (i == 1) {
									newNode.convertToNonFree();
								}
//								temp_sol.addEdge(oldNode, newNode);
								temp_sol.addEdge(oldNode, newNode, e);

//								System.out.println(oldNode.getTableName()+"("+oldNode.hashCode()+")-"+newNode.getTableName()+"("+newNode.hashCode()+")");								
								temp_sol.addNode(newNode);
								temp_sol.setKeyString();
								if (!existingSols.contains(temp_sol.getKeyString())) 
										//&& !temp_sol.containsPFP(newNode, oldNode, schema.allEdges))
								{

									if (i==1 && temp_sol.convertToMinimalSolution()) {
										// found a new solution 
										temp_sol.inResults = true;
										temp_sol.setGlobalWeight();
										sols.add(temp_sol);
										existingSols.add(temp_sol.getKeyString()); // could have cached the keyString() result;
										q.add(temp_sol);
//System.out.println(temp_sol.size()+"  "+temp_sol.getKeyString()+" *");		
									}
									//else if (i!=1) {    //if the current node in non-free but there is a free leaf, the the free leaf must be append last
									else if (temp_sol.size() < global.searchRange) {
//if (temp_sol.keyString.equals("7Person8Proceeding9InProceeding_NFProceedingId#EditorId#9Proceeding_NFEditorId#"))
//System.out.println("HERR");
					
										existingSols.add(temp_sol.getKeyString()); // could have cached the keyString() result;
										q.add(temp_sol);			
									}
								}
							}
						}
					}  
				}
			}  
		}
		q.clear();
		System.out.println(sols.size());
		System.err.println(sols.size());
		System.out.print("NaiveTime: ");
		time.stopNPrint();
		
		existingSols.clear();
		initPP();
		
		return time.getTime();		
	}

	public void scanCNG(Set<String> NonFreeSets) throws SQLException {	//find the sub-tree of all CNs for the given query 
		System.err.print("START Scaning for Networks...");
		System.out.print("START Scaning for Networks...");
		for (int i = 0; i < schema.strees.size(); i++) {
			System.out.println(NonFreeSets.size());System.out.println(schema.nonfreesets.get(i).size()); System.out.println(schema.nonfreesets.get(i).get(0));
			if (NonFreeSets.containsAll(schema.nonfreesets.get(i)) 
			&& NonFreeSets.size() == schema.nonfreesets.get(i).size()) { // find the corresponding stree
				this.sols = schema.strees.get(i).sols;
				for (int j = 0; j < this.sols.size(); j++)
					sols.get(j).setQuery(query);
				break;
			}
		}
		System.out.println(sols.size());
		System.err.println(sols.size());

	}
}
