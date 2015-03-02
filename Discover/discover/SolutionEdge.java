package discover;

import java.util.ArrayList;
//import java.util.HashMap;
class Partition implements Comparable {
	TPSolution pSol;
	SolutionEdge edge;
	byte[] nodeMapping;
	
	public String toString() {
		return pSol.getKeyString();
	}
	protected String getAlignName() {
		return "P"+edge.pList.indexOf(this);
	}
	public int compareTo(Object p) {
		int c = pSol.compareTo(((Partition)p).pSol);
		if (c==0) return hashCode() - p.hashCode();
		return c;
		//return (pSol == ((Partition)p).pSol)?0:1;
	}
	protected Partition(SolutionEdge _edge, TPSolution _pSol) {
		pSol = _pSol;
		edge = _edge;
		nodeMapping = null;
	}
	protected boolean equals(Partition p) {
		if (pSol == p.pSol) return true;
		return false;
	}
	protected GraphNode mapNode(GraphNode sn) {		//from a node on the larger CN to smaller CN
//		System.out.println("HERE"+epSol.nonFreeNode.getId()+" "+sn.getId()+" "+(ppNodeMapping.size()));		
		return pSol.tableIdDict.get(nodeMapping[sn.getId()]);
	}
	protected byte inverseMapNodeIndex(int id) {
		for (byte i=1; i<nodeMapping.length; i++) {
			if (nodeMapping[i] == id) return i;
		}
		return 0;
	}
	protected int inverseMapNodeIndex(GraphNode sn) {		//from a node on smaller CN to larger CN
//		System.out.println("HERE"+epSol.nonFreeNode.getId()+" "+sn.getId()+" "+(ppNodeMapping.size()));
		return inverseMapNodeIndex(sn.getId());
	}
	protected ArrayList<GraphNode> getNodes() {
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		for (byte i=1; i<nodeMapping.length; i++) {
			if (nodeMapping[i] > 0) {
				nodes.add(edge.sol.getNode(i));
			}
		}
		return nodes;
	}
	public int[] getPPCurCurVal(int[] c) {
		int[] pc = new int[pSol.size()+1];
		for (int i=0; i<pc.length; i++) pc[i] = -1;
//		for (int i=1; i <=  size(); i++) 
		//   		c[i] = curCur.get(tableIdDict.get(i));
		//int[] mapping = ppNodeMapping.get(pp);
		for (int i=1; i<=edge.sol.size();i++) {
//			System.out.print(mapping[i]+" "+c[i]+":"); 			
			if (nodeMapping[i] > 0) pc[nodeMapping[i]] = c[i];
		}
//		System.out.println("mapping "+keyString+" "+pp.keyString);		
		return pc;
	}

	public String getPPCurCurStr(int[] c) {
		int[] pc = getPPCurCurVal(c);
		StringBuffer pcs = new StringBuffer();
		for (int i=1; i<=pSol.size(); i++) {
			if (pc[i] >= 0) pcs.append(pc[i]).append(" ");
		}
		return pcs.toString();
	}
/*	protected boolean isHead() {
		if (pSol == null) return true;
		return false;
	}
*/	
/*	public ArrayList<GraphNode> getNodes() {
		ArrayList<GraphNode> compNodes = new ArrayList<GraphNode>();
		for (int i=1; i<=edge.sol.size(); i++) {
			//if (mapping[i] < 0) _compNodes.add(tableIdDict.get(i));
			if (nodeMapping[i] < 0 || isHead()) 
				compNodes.add(edge.sol.tableIdDict.get(i));
		}
		return compNodes;
	}
*/

}
public class SolutionEdge implements Comparable{
	TPSolution sol;
	ArrayList<Partition> pList;
	byte[] linkNodes;	//1: is linkNode; -1: not linkNode; 0: knotNode (in a partition)
	//ArrayList<GraphNode> linkNodes;
	boolean valid;
	ArrayList<GraphNode> symmetry;
	
	public int compareTo(Object e) {
		int res = 1; 
		if (pList.get(0).equals(((SolutionEdge)e).pList.get(0)) && pList.get(1).equals(((SolutionEdge)e).pList.get(1))) {
			byte id1 = pList.get(0).inverseMapNodeIndex(1);
			byte id2 = ((SolutionEdge)e).pList.get(0).inverseMapNodeIndex(1);
			if (id1 != id2) {
				symmetry.add(sol.getNode(id1));
				symmetry.add(sol.getNode(id2));
			}
			res = 0;			
		}
		
		if (pList.get(0).equals(((SolutionEdge)e).pList.get(1)) && pList.get(1).equals(((SolutionEdge)e).pList.get(0))) {
			byte id1 = pList.get(0).inverseMapNodeIndex(1);
			byte id2 = ((SolutionEdge)e).pList.get(1).inverseMapNodeIndex(1);
			if (id1 != id2) {
				symmetry.add(sol.getNode(id1));
				symmetry.add(sol.getNode(id2));
			}
			res = 0;		
		}
		return res;
	}
/*	public int compareTo(Object s) {
		
		if (isHead()) return 0;
//System.out.println(sol+" "+pSol+" "+((SolutionEdge)s).pSol);
		//return (pSol.hashCode() - ((SolutionEdge)s).pSol.hashCode());
		return pSol.compareTo(((SolutionEdge)s).pSol);
	}*/
	
	protected SolutionEdge(TPSolution _sol) {
		sol = _sol;
		valid = true;
		pList = new ArrayList<Partition>();
		symmetry = new ArrayList<GraphNode>();
		
		linkNodes = new byte[_sol.size()+1];
		for (int i=0; i<linkNodes.length; i++) linkNodes[i] = -1;
	}
	protected ArrayList<GraphNode> getLinkNodes() {
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		for (byte i=1; i<=sol.size(); i++) {
			if (linkNodes[i] != -1) nodes.add(sol.getNode(i));
		}
		return nodes;
	}
	protected ArrayList<GraphNode> getLinkNodesWithoutKnots() {
		ArrayList<GraphNode> nodes = new ArrayList<GraphNode>();
		for (byte i=1; i<=sol.size(); i++) {
			if (linkNodes[i] == 1) nodes.add(sol.getNode(i));
		}
		return nodes;
	}
	protected Partition newPartition(TPSolution parent) {
		Partition p = new Partition(this, parent);
		pList.add(p);
		return p;
	}
	protected boolean isValid() {
		for (Partition p: pList) {
			if (!p.pSol.isValid()) {
				return false;
			}
		}
		return true;
	}
	
/*	protected boolean isHead() {
		return (linkNodes == null);
	}
*/	
	protected SolutionEdge(SolutionEdge e) {
		if (e.sol != null) sol = e.sol;
		pList = new ArrayList<Partition>(e.pList);
		symmetry = new ArrayList<GraphNode>(e.symmetry);
	}
	protected void setEpEdge() {
		//if (pSol != null) {
		//	pSol.children.add(sol);
		//}	
		sol.epEdge = this;
	}
}
