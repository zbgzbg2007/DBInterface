import java.util.*;

class Node {
	String tablename;
	List<Integer> neighbors; 
	Node(String n) {
		this.tablename=n;	
	}
	void add(int x) {
		this.neighbors.add(x);
	}
		
}

public class Graph {

	List<Node> vertices;
	Graph(List<List<String>> tables) {
	//edges contain all the tables
	//the first element is the name of table, then the names of attributes
	//build the schema graph
		vertices = new ArrayList<Node>();
		HashMap<String, HashSet<Integer>> index = new HashMap<String, HashSet<Integer>>();
		for (int j = 0; j < tables.size(); j++) {
			List<String> relation = tables.get(j);
			vertices.add(new Node(relation.get(0)));
			int s = relation.size();
			for (int i = 1; i < s; i++) {
				String x = relation.get(i);
				if (index.containsKey(x) == false)
					index.put(x, new HashSet<Integer>());	
				index.get(x).add(j);
			}
		}
		for (int j = 0; j < tables.size(); j++) {
			List<String> relation = tables.get(j);
			int s = relation.size();
			for (int i = 1; i< s; i++) {
				String x = relation.get(i);
				for (Integer k: index.get(x)) 
					if (k != j) 
						vertices.get(j).add(k);
			}
		}
	}

}
