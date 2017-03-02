# DBInterface

We try to improve the performance of the keyword search interface over MySQL from the paper 
"Efficient IR-Style Keyword Search over Relational Databases" by V. Hristidis et al.  

The architecture consists of three components: IR Engine, Candidate Network Generator, and Execution Engine. 

- IR-Engine: using the IR Index, IR-Engine extracts from each relation the tuples set which consists of the tuples with a non-zero score for given keywords.

- Candidate Network Generator: receiving the non-empty tuple set from the IR Engine, together with the database schema and a parameter, Candidate Network Generator produces Candidate Networks (CNs), which are join expressions to be used to create joining trees of tuples that will be considered as potential answers.

-  Execution Engine: Execution Engine takes as input a set of CNs together with the non- free tuple sets and contacts the database system’s query execution engine repeatedly to identify the final results.


The main idea of our improvement is to precompute all the possible Candidate Network (CN) sets and build an index for them so that we can remove the CN Generator in our engine.
We observe that for different keyword-queries, the resulting CN set could be the same. This is because for different keyword-queries, the non-free tuple sets, that are tuples contain- ing at least one keyword and then positive scores, may be in the same non-free table sets. And the CN set will be computed according to these non-free table sets and the schema of the database. Thus, we can find a map from table sets to CN sets. Further, compared to the number of tuples, the number of relations in the schema is always relatively small and stable, so the computation of CN set for all relation subset is not very costly. Notice that the specific computation of one CN set is the same as the original method.
Based on the above analysis, instead of compute CN set for every keyword-query, we could build a CN-index for every possible subset of relations that contain meaningful attributes in the database: the key of our index is a relation subset and the value is its corresponding CN set. This CN-index will be precomputed when the search engine starts.

Then we found the source code for Spark2, so we implemented our
idea by modifying the code. You can check their paper "Spark: top-k keyword query in relational databases" 
by Yi Luo, Xuemin Lin, Wei Wang and Xiaofang Zhou, which also includes a similar idea of building index for CNs.



