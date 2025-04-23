Flask backend with 2 API endpoints :
"/yourVCS" -> the graph rendering component hits this via fetch async call and 
"/graph-rag" -> is hit by the rag component frontend via fetch async call


CORS enabled for cross origin 

connected with the neo4j database and is responsible for rendering the neo4j graph to react via CQL query

is also responible for invoking the rag functionality in rag1.py file

running on port 5005 and is configured to accept all connections '0.0.0.0' 



rag1.py :
2 step rag process , 
step 1 = uses LLM to generate cypher queries and smartly extract the relevant subgraph required to answer the query
step 2 = makes a decision whether or not the commit versions are required

adds the content gotten from (step 1 + step 2 + template + user query) ----------> context relevant query --------->pre trained LLM --------------> much better output due to context 

