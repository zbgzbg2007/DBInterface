package discover;

import java.util.*;
/*
class treeNode implements Comparable {
	GraphNode oriNode;
	treeNode[] children;
//	byte id;
	int subtreeHeight;
	StringBuffer DFCS;
	treeNode(GraphNode n) {
		oriNode = n;
//		id = n.id.byteValue();
		DFCS = null;
		children = null;
	}
	public int compareTo(Object o) {
		int c = (DFCS.toString().compareTo(((treeNode)o).DFCS.toString()));
	//	if (c == 0) return hashCode() - o.hashCode();
		return c;
	}
}
*/

public class Graph{
	Global global;

	protected ArrayList<GraphNode> nodes;
	// record all edge info
	protected HashMap<Byte, GraphNode> tableIdDict;
//	public String keyString;		//only for CNs, not for DB schema
	
//	protected GraphNode lastKnot;
	protected GraphNode root;		//if it is a tree
	public byte[] BFIdList;			//BF ID list, for node mapping
	protected int lastKnotId;		//BFIdList[lastKnotId] is the id of the last knot 
	
	protected void setRoot(GraphNode n) {
		root = n;		
		//n.setParent(null);
	}
	protected Graph() {
		nodes = new ArrayList<GraphNode>();
		tableIdDict = new HashMap<Byte, GraphNode>();
	}
	protected boolean mergeLeg1(GraphNode knotNode, Graph g) {		
		if (g.size() == 1) {				//!!!
			if (!knotNode.isNonFree()) {
				knotNode.convertToNonFree();
				return true;
			}
			return false;
		}
		byte lastId = size();
		GraphNode knotInG = g.getNodes().get(0);
		if (!containsPFP(knotNode, knotInG, knotInG.getFirstEdge())) {
			
			//g.deleteEdge(g.getNodes().get(1), g.getNodes().get(0));		
			knotInG.mergeTo(knotNode);
			g.deleteNode(knotInG);
			GraphNode tieNode = g.getNodes().get(0);		
			
//			tieNode.addEdge(knotNode);
			tieNode.setParent(knotNode);
			for (GraphNode n: g.getNodes()) {
				n.setId(++lastId);
				tableIdDict.put(lastId, n);
				n.graph = this;
			}
			nodes.addAll(g.getNodes());
			return true;
		}
		return false;
		       
	}
	protected boolean checkOrder(byte[] old_BFIdList, Graph g, Graph prevLeg) {
		//g must be on the right side of prevLeg; DFCS(g) > DFCS(prevLeg)
		//int inew = 0;
		//int iold = 0;
		//boolean foundPrevTieNode = false;
		if (old_BFIdList.length == 1) {
			if (size() > 2 && root.childrenId[1] < root.childrenId[0]) return false;
			                            //BFIdList[1] < BFIdList[2]) return false;
			if (size() == 2 && root.id > root.childrenId[0]) return false;
											//BFIdList[0] > BFIdList[1]) return false;
			//while (inew < size()) {
			//	if (BFIdList[inew] == prevLeg.nodes.get(0).id) foundPrevTieNode = true;
			//	if (BFIdList[inew] == g.nodes.get(g.size()-1).id && !foundPrevTieNode) return false;
			//	inew++;
			//}			
			return true;
		}
		/*if (g != null && g.size() < prevLeg.size()-1) return true;
		while (inew < size()) {
			if (old_BFIdList[iold] == prevLeg.nodes.get(prevLeg.size()-2).id) 
				foundPrevTieNode = true;
			if (old_BFIdList[iold] != BFIdList[inew]) return false;
			iold++;
			inew++;
			while (inew < size() && BFIdList[inew] > old_BFIdList.length) {
				if (foundPrevTieNode == false) return true;
				inew++;
			}
		}*/
		return false;
	}
	protected boolean mergeLegInOrder(GraphNode knotNode, Graph g, GraphNode prevLegRoot) {
		byte prevSize = size();
		byte lastId = prevSize;
		
		if (!containsPFP(knotNode, g.getNodes().get(0), g.getNodes().get(0).getFirstEdge())) {
			//buildDFCF(g.getNodes().get(0));
			
//			g.deleteEdge(g.getNodes().get(1), g.getNodes().get(0));
			g.getNodes().get(0).mergeTo(knotNode);
			g.deleteNode(g.getNodes().get(0));
			GraphNode tieNode = g.getNodes().get(0);		
		
			//addEdge(tieNode, knotNode);
			tieNode.setParent(knotNode);
			for (GraphNode n: g.getNodes()) {
				n.setId(++lastId);
				tableIdDict.put(lastId, n);
				n.graph = this;
			}
			nodes.addAll(g.getNodes());
			
			if (prevSize == 1) {

				setKeyString();
				if (root.childrenId.length > 1) {
					if (root.childrenId[0] > root.childrenId[1]) {						
//System.out.println(keyString+"  -"+g.getNodeString());
						return false;
					}
					return true;
				} else {
					if (root.getId() < root.childrenId[0])
						return false;
					return true;
				}

			}

			int l = knotNode.childrenId.length;
			byte[] newChildrenId = new byte[l + 1];
			System.arraycopy(knotNode.childrenId, 0, newChildrenId, 0, l);
			newChildrenId[l] = tieNode.getId();
			knotNode.childrenId = newChildrenId;
			//setKeyString();
			
			setDFCS(knotNode);
			setBFIdList(root);
			while (knotNode != root) {
				knotNode = knotNode.parent;
				tieNode = tieNode.parent;
				//knotNode.DFCS = setDFCS(knotNode);
				if (!updateDFCS(knotNode, tieNode))
					return false;
			}
			//keyString = root.DFCS;
			//if (checkOrder(old_BFIdList, g, prevLeg)) return true;
			return true;
		}
		//if (prevLeg.size() > 1) {
//System.out.println(keyString+"  -"+g.getNodeString());
		//}
		return false;
		       
	}
	protected StringBuffer getNodeString() {
		StringBuffer b = new StringBuffer();
		for (GraphNode n: getNodes()) {
			b.append(n.getTableName());
			if (n.isNonFree()) b.append("_NF");
			b.append("-");
		}
		return b;
	}
	
	protected void grow(byte id, GraphNode knot, GraphNode knotInSchema, GraphEdge e, Boolean _nonFree) {
		GraphNode tie = new GraphNode(id, e.getNeighbor(knotInSchema), this);	//inserted node
		if (_nonFree) {
			tie.convertToNonFree();
		} else {
			tie.convertToFree();
		}
		addNode(tie);
		if (size() > 1) {
			addEdge(knot, tie, e);
			//knot.setParent(tie);
			tie.setParent(knot);			
		}
//		while (tie != null) {
//			setDFCS(tie);
//			tie = tie.parent;
//		}

	}
	protected boolean inOrder(GraphNode prevLegRoot, GraphNode newLegRoot) {
		if (prevLegRoot == null) {		//grow from single node
			return true;
		}
//		if ((newLegRoot.DFCS + newLegRoot.getParentEdge().getLabel()).compareTo
//			(prevLegRoot.DFCS + prevLegRoot.getParentEdge().getLabel()) >= 0) return true;
		if ((newLegRoot.DFCS).compareTo(prevLegRoot.DFCS) >= 0) return true;

		return false;		
	}
	protected int compareFront(GraphNode prevLegRoot, GraphNode newLegRoot) {
		while (true) {
			int comp = prevLegRoot.getNonFreeTableName(null).compareTo(newLegRoot.getNonFreeTableName(null));
			if (comp > 0) return 1;
			if (comp < 0) return -1;
			if (newLegRoot.isLeaf() || prevLegRoot.isLeaf()) return 0;
			prevLegRoot = prevLegRoot.getChildren()[0];
			newLegRoot = newLegRoot.getChildren()[0];			
		}		
	}
	public void resetGraph(){
		nodes = new ArrayList<GraphNode>();
		tableIdDict = new HashMap<Byte, GraphNode>();		
	}
	protected Graph(Graph X) {
		nodes = new ArrayList<GraphNode>();
		tableIdDict = new HashMap<Byte, GraphNode>();
		for (GraphNode sn: X.nodes) {
			GraphNode new_node = new GraphNode(sn.id, sn, null);		//copy the sn.sol to the new node
			//new_node.setPossibleMax(sn.getPossibleMax());
			//schemaGraph.add(sn);
			new_node.setId(sn.id);
			tableIdDict.put(new_node.getId(), new_node);
			new_node.graph = this;			
			nodes.add(new_node);
		}
		
//		X.tableIdDict);

		for (GraphNode sn: X.getNodes()) {
			GraphNode copysn = copy(sn);
			for (GraphEdge e: sn.getEdges()) {
				if (e.in) {
					GraphNode copyFNode = copy(e.fNode);					
					GraphEdge copyEIn = new GraphEdge(copysn, copyFNode, true, e.getFCol());
					copysn.addEdge(copyEIn);
					GraphEdge copyEOut = new GraphEdge(copysn, copyFNode, false, e.getFCol());
					copyFNode.addEdge(copyEOut);
				}
			}
			
				
		}
		for (GraphNode sn: X.getNodes()) {
			GraphNode copysn = copy(sn);
			if (sn.parent != null) {
				copysn.setParent(copy(sn.parent));			
			}
			else copysn.setParent(null);
		}
		if (X.root != null) root = copy(X.root);
		//keyString = X.keyString;
		if (X.BFIdList != null) BFIdList = (byte[])X.BFIdList.clone();
	}
	protected GraphNode copy(GraphNode n) {
		if (tableIdDict != null && n!= null && tableIdDict.containsKey(n.getId())) {
			return tableIdDict.get(n.getId());
		}
		return null;
	}
	/*protected ArrayList<GraphNode> copy(ArrayList<GraphNode> nodes) {
		ArrayList<GraphNode> copy = new ArrayList<GraphNode>();
		for (GraphNode n: nodes) {
			copy.add(copy(n));			
		}
		return copy;
	}*/
	protected boolean containsNode(GraphNode n) {
		return nodes.contains(n);
	}
	protected GraphNode getNode(Byte id) {
		return tableIdDict.get(id);
	}	
	protected void addNode(GraphNode newNode) {
		nodes.add(newNode);
		//newNode.setId(size());
		tableIdDict.put(newNode.getId(), newNode);
	}
	protected void deleteNode(GraphNode n) {
		nodes.remove(n);
		tableIdDict.remove(n.getId());
	}
	//add a edge between pNode and fNode, 
	protected void addEdge(GraphNode knotNode, GraphNode tieNode, GraphEdge e) {
		
		if (e.in) {
			GraphEdge newEdgeIn = new GraphEdge(knotNode, tieNode, true, e.getFCol());
			knotNode.addEdge(newEdgeIn);
			GraphEdge newEdgeOut = new GraphEdge(knotNode, tieNode, false, e.getFCol());			
			tieNode.addEdge(newEdgeOut);
		
		} else {
			GraphEdge newEdgeOut = new GraphEdge(tieNode, knotNode, false, e.getFCol());
			knotNode.addEdge(newEdgeOut);
			GraphEdge newEdgeIn = new GraphEdge(tieNode, knotNode, true, e.getFCol());			
			tieNode.addEdge(newEdgeIn);
		}
	}
	protected void deleteEdge(GraphNode knotNode, GraphNode tieNode) {
		GraphEdge e = knotNode.getEdge(tieNode);
		knotNode.deleteEdge(e);
		e = tieNode.getEdge(knotNode);
		tieNode.deleteEdge(e);
	}
	
	protected ArrayList<GraphNode> getKnotNodes() { 
		//non-leaf nodes after (include) the last node that has been a knot
		if (size() < 2) return getNodes();
		
		ArrayList<GraphNode> knotNodes = new ArrayList<GraphNode>();
		for (int i = size() - 1; i >= 0; i--) {
			GraphNode n = getNode(BFIdList[i]);
			//if (n.getLinkedNodesSize() > 1 && !n.isNonFree()) knotNodes.add(n);
			//if (n.getLinkedNodesSize() > 2 || (n.getLinkedNodesSize() > 1 && n.isNonFree())) break;
			if (n.getLinkedNodesSize() > 1) knotNodes.add(n);
			if (n.getLinkedNodesSize() > 2) break;
		}
		return knotNodes;
	}
	protected void deleteLeg(GraphNode n, byte[] linkNodes) {
		//ArrayList<GraphNode> legNodes = new ArrayList<GraphNode>();
		GraphNode tmpN = null;
		GraphNode linked = n;
		
		do {
			tmpN = linked;
			linked = tmpN.getFirstLinkedNode();
			deleteEdge(tmpN, linked);
			
			deleteNode(tmpN);
			linkNodes[tmpN.getId()] = 1;
//			legNodes.add(tmpN);
		} while (!linked.isNonFree() && linked.getLinkedNodesSize() == 1);
		linkNodes[linked.getId()] = 0;
		linkNodes[n.getId()] = 0;
//		legNodes.add(linked);
	}
	protected void deleteComponent(GraphNode n) {
		LinkedList<GraphNode> q = new LinkedList<GraphNode>();
		q.add(n);
		while (!q.isEmpty()) {
			n = q.poll();
			deleteNode(n);
			while (n.getLinkedNodesSize() > 0) {
				GraphNode n2 = n.getFirstLinkedNode();
				q.add(n2);
				deleteEdge(n, n2);
				//n.deleteEdge(n.getFirstEdge());
			}
		}
	}
	
	
	
	//the number nodes in schema graph
	public byte size() {
		return (byte)(nodes.size());
	}

	//return the adjacent list of schema graph
	public ArrayList<GraphNode> getNodes() {
		return nodes;
	} 
	public boolean checkNonFreeRange(int nonFreeRange) {
		int i = 0;
		//System.out.println(nonFreeRange);
		for (GraphNode n: getNodes()) {
			if (n.isNonFree()) i++;
		}
		if (i <= nonFreeRange) return true;
		else return false;
	}
	//check if 3 nodes are P-F-P: neighbor of knot via e (in schema), knot, and a neighbor of knot
	public boolean containsPFP(GraphNode knot, GraphNode knotInSchema, GraphEdge e) {
		boolean ret = false;	
		if (e != null && e.fNode == knotInSchema && !e.in) {
			for (GraphEdge knotEdge: knot.getEdges()) {
				if (knotEdge.fNode == knot && e.getFCol().equals(knotEdge.getFCol()) 
						&&	e.pNode.getTableName().equals(knotEdge.pNode.getTableName())) {
					ret = true;
					break;				
				}
			}
		}	    
		return ret;
	}	    
	
	protected byte[] getNodeMapping(Graph p, int size) {	//for two isomophic graphs
		byte[] mapping = new byte[size+1];
		for (int i=0; i < mapping.length; i++) mapping[i] = -1;
		if (p != null) {
			for (int i=0; i<BFIdList.length; i++) {
				int id = BFIdList[i];
				mapping[id] = p.BFIdList[i]; 
			}
		}
		return mapping;		//mapping[n.id] <- n.corresponding_node_in_p.id; mapping[0] not used
	}
	// could be put into a HashSet or HashTable
	public String getKeyString() {
		return root.DFCS;
	}
	public String toString() {
		StringBuffer BFIdListString = new StringBuffer();
		return root.DFCS+" "+BFIdListString;
	}

	protected GraphNode getRoot() {
		Graph tmpGraph = new Graph(this);		
		int leftNodes = size();

		while (leftNodes > 2) {		//1 or 2: stop
			ArrayList<GraphNode> leaves = new ArrayList<GraphNode> ();
			for (GraphNode n: tmpGraph.nodes) {
				if (n.getLinkedNodesSize() == 1) {
					leaves.add(n);
				}
			}	
			for (GraphNode n: leaves) {
				GraphNode p = n.getFirstLinkedNode();
				copy(n).setParent(copy(p));
				//addChild(children, copy(p), copy(n));
				deleteEdge(n, p);
				//tmpGraph.deleteEdge(n, p);
				tmpGraph.deleteNode(n);					
				leftNodes--;
			}
		}
		if (leftNodes == 1) {
			return copy(tmpGraph.nodes.get(0));
		}
		//2 nodes left, ottherwise
		GraphNode n1 = copy(tmpGraph.nodes.get(0));
		GraphNode n2 = copy(tmpGraph.nodes.get(1));
		
		n2.setParent(n1);
		setRoot(n1);
		root.setParent(null);
		buildDFCF(n1);
		String DFCS1 = n1.DFCS;
		
		n1.setParent(n2);
		setRoot(n2);
		root.setParent(null);
		buildDFCF(n2);
		String DFCS2 = n2.DFCS;
		
		int compare = DFCS1.compareTo(DFCS2);
		if (compare == 0) {
			compare = n2.id - n1.id;
		}
		if (compare < 0) {		//DFCS1 < DFCS2 or (DFCS1 = DFCS2 and n2.id < n1.id)
			n2.setParent(n1);
			return n1;
		}		
		n1.setParent(n2);
		return n2;
		
	}
	protected String setDFCS(GraphNode n) {
//		for rooted sorted tree or subtree
		n.DFCS = new String();
		StringBuffer DFCStmp = new StringBuffer();
		GraphNode[] children = n.getChildren();
	//	int prevh = n.subtreeHeight;
		n.subtreeHeight = 0;
		for (int j = 0; j < children.length; j++) {
			n.subtreeHeight = Math.max(n.subtreeHeight, children[j].subtreeHeight + 1);
		}
	//	if (prevh != n.subtreeHeight && n.subtreeHeight == 2) {
	//		System.out.println("JERE");
	//	}
		DFCStmp.append(9 - n.subtreeHeight);
		DFCStmp.append(n.getTableName());
		if (n.isNonFree()) DFCStmp.append("_NF");
		if (n.parent != null) {
			DFCStmp.append(n.getParentEdge().getLabel());
		}

		for (int j = 0; j < children.length; j++) {
			DFCStmp.append(children[j].DFCS);
		}
		if (n.subtreeHeight > 0)
			DFCStmp.append("%");
//		if (n.parent != null) {
//			DFCStmp.append(n.getParentEdge().getLabel());
//		}
		n.DFCS = DFCStmp.toString();
		return n.DFCS;
	}
	protected boolean updateDFCS(GraphNode n, GraphNode newSubtree) {		
		//for rooted sorted tree or subtree
		//after adding the rightmost leg
		//check if the new subtree is on the left side of its right-side brother
		GraphNode nextSubtree;
		for (int i = 0; i < n.childrenId.length - 1; i++) {
			if (n.childrenId[i] == newSubtree.getId()) {
				nextSubtree = getNode(n.childrenId[i + 1]);
				if (!inOrder(newSubtree, nextSubtree)) 
//System.out.println(n.DFCS+"  "+newSubtree.DFCS);					
				//if (newSubtree.DFCS.compareTo(nextSubtree.DFCS) < 0) 
					return false;
			}
		}		
		n.DFCS = setDFCS(n);
		return true;
	}	
	
	protected void buildDFCF(GraphNode n) {		//for rooted unsorted tree
		//DFCS: depth-first canonical string
		//StringBuffer out = new StringBuffer(root.getNonFreeTableName());		
		n.DFCS = new String();
		StringBuffer DFCStmp = new StringBuffer();
		GraphNode[] children = n.getChildren();
		
		if (children.length >= 1) {
			n.subtreeHeight = 0;
			for (GraphNode c: children) {
				//GraphNode n = e.getNeighbor(root);
				//if (!(n == root.parent)) {		//n is a child of root
					buildDFCF(c);					
					if (c.subtreeHeight + 1 > n.subtreeHeight)
						n.subtreeHeight = c.subtreeHeight + 1;
				//}
				
			}

			Arrays.sort(children);
			n.childrenId = new byte[children.length];
			for (int j=0; j < children.length; j++) {
				n.childrenId[j] = children[j].getId();
			}
			
			DFCStmp.append(9 - n.subtreeHeight);
			DFCStmp.append(n.getTableName());
			if (n.isNonFree()) DFCStmp.append("_NF");
			//DFCStmp.append("(");
			if (n.parent != null) {
				DFCStmp.append(n.getParentEdge().getLabel());
			}

			for (int j = 0; j < children.length; j++) {
				DFCStmp.append(children[j].DFCS);
				//tnode.DFCS.append(n.DFCS);
			}
			DFCStmp.append("%");

		} else {
			n.subtreeHeight = 0;
			n.childrenId = null;
			//if (root.isNonFree())
			DFCStmp.append(9-n.subtreeHeight);
			DFCStmp.append(n.getNonFreeTableName(null));
			if (n.parent != null) {
				DFCStmp.append(n.getParentEdge().getLabel());
			}
		}
		//for (GraphEdge e: n.getEdges()) {
		//	if (!e.in) DFCStmp.append(e.getFCol());
		//}
		n.DFCS = DFCStmp.toString();
		//return;
	}

	protected void setBFIdList(GraphNode root) {
		//pre-order; right-left depth-first search
		LinkedList<GraphNode> q = new LinkedList<GraphNode>();
		q.add(root);
		int i = 0;
		BFIdList = new byte[size()];
		while (!q.isEmpty()) {
			GraphNode n = q.poll();
			BFIdList[i] = n.id;
			i++;
			if (n.childrenId != null) {
				//for (int j=n.childrenId.length-1; j>=0; j--) {
				for (int j=0; j<n.childrenId.length; j++) {
					q.addFirst(tableIdDict.get(n.childrenId[j]));
				}
			}
		}
	}
	public void setKeyString() {

		//HashMap<GraphNode, ArrayList<GraphNode>> children;
		//children = new HashMap<GraphNode, ArrayList<GraphNode>>();
		GraphNode root = getRoot();
		root.setParent(null);
		setRoot(root);
		buildDFCF(root);
		setBFIdList(root);
		//setBFIdList(treeRoot);
		//keyString = root.DFCS.toString();
		
	}
}
