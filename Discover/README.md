The source code is from http://www.cse.unsw.com:443/~weiw/project/SPARK.html .
The code contains different algorithms for execution engine, and they can be chosen by "ALGS" in the input file.
The code also sppourts different semantics for keywords, see parseKeywords(Stirng) in Query.java. I just use "AND" by "+".


I modified the following blocks in the code and my DB: 

1. hard code the new Database information in Global.java, still named DBLP 
2. add one column, named "dl" for "document length", for each talbe in my Database 
3. add one more option in the input file to show if generate CNs offline and modify the code in Global.java 
4. add the function that generate all CNs according to non-free tuple sets in SchemaGraph.java (I hard code the non-free tuple sets for my DB, but it could be modified to enumerate all the possible non-free sets) 
5. fix the bug in the function generateCNG() in SolutionTree.java 
6. add new function scanCNG(Set<String>) which scan CNs generated offline in SolutionTree.java 
7. modify the way to search SolutionTree in queryProcessing of Query.java 
8. add offline generated CNs and corresponding non-free tuple sets in SchemaGraph.java 

The idea is to build an index for all possible non-free tuple sets, and use it we can directly find the corresponding CNs instead of computing all CNs.
