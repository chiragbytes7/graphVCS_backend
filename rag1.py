from together import Together
import boto3
from neo4j import GraphDatabase
import json
import ast
import difflib
import time
import logging
import os 
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class VCSQueryProcessor:
    def __init__(self, neo4j_uri, neo4j_user, neo4j_password, s3_client):
        self.llm = Together(api_key="f51a6509135357ca5d205583e2de7c9b765b9db38feac8fe673907f246aa0280")
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_password))
        self.s3_client = s3_client
        self.bucket_name = "neo4jvcs"

    def process_query(self, user_query):
        logger.info(f"Processing query: {user_query}")
        start_time = time.time()
        
        try:
            analysis_dict = self.analyze_query(user_query)
            logger.info(f"Query analysis completed in {time.time() - start_time:.2f} seconds")
            
            neo4j_subgraph = self.get_neo4j_data(analysis_dict["query"])
            logger.info(f"Neo4j data fetched in {time.time() - start_time:.2f} seconds")
            
            s3_data = None
            if analysis_dict.get("verdict") == "yes":
                s3_data = self.get_s3_versions(analysis_dict["branches"], neo4j_subgraph)
                logger.info(f"S3 data fetched in {time.time() - start_time:.2f} seconds")
            
            response = self.generate_response(user_query, neo4j_subgraph, s3_data)
            logger.info(f"Total query processing completed in {time.time() - start_time:.2f} seconds")
            return response
            
        except Exception as e:
            logger.error(f"Error processing query: {str(e)}")
            return f"Error processing query: {str(e)}"

    def analyze_query(self, user_query):
        start_time = time.time()
        prompt = """
    ### Project Overview
This is a Neo4j-based version control system that implements core VCS functionality similar to Git, using a graph database for tracking commits and branches and their heads ,
and an S3 bucket for storing each commit's version
all the version history for each commit is stored in an s3 bucket with the folder for each commit have a name as the commit id for that commit

### Data Model
1. **Nodes**:
   - `Commit`: Represents a snapshot of code
     - Properties: `id`, `message`, `timestamp`
   - `User`: Represents contributors
     - Properties: `id`
   - `HEAD`: Represents the current state of a branch
     - Properties: `branch` (branch name)
   - `Branch`: Represents different code branches
     - Properties: `name`

2. **Relationships**:
   - `MADE_BY`: (Commit)->[User] - Links commits to their authors
   - `PARENT`: (Commit)->[Commit] - Links commits to their parent commits
   - `POINTS_TO`: (HEAD)->[Commit] - Shows which commit a branch currently points to
   - `BASE`: (Branch)->[Commit] - Shows the initial commit of a branch

### Key Operations and Their Cypher Patterns

1. **Finding Current Branch Head**:
```cypher
MATCH (n:HEAD {branch: $branch_name})-[:POINTS_TO]->(c:Commit)
RETURN c.id as commit_id
```

2. **Creating New Commits**:
```cypher
CREATE (c:Commit {id: $commit_id, message: $message, timestamp: $timestamp})
MERGE (u:User {id: $user_id})
CREATE (c)-[:MADE_BY]->(u)
```

3. **Linking Parent Commits**:
```cypher
MATCH (child:Commit {id: $commit_id}), (parent:Commit {id: $parent_commit_id})
CREATE (child)-[:PARENT]->(parent)
```

4. **Branch Management**:
```cypher
CREATE (b:Branch {name: $branch_name})
MERGE (c:Commit {id: $commit_id})
CREATE (b)-[:BASE]->(c)
```

5. **Finding Common Ancestors** (for merge operations):
```cypher
MATCH path = (n:Commit)-[:PARENT*]->(a:Commit)
WHERE n.id = $commit_id
RETURN a.id AS id, length(path) AS distance
```

### Common Use Cases for NLP Translation

1. **Branch Operations**:
   - "Show me the current commit on branch X"
   - "List all commits on branch X"
   - "Find the latest commit by user Y on branch X"

2. **Commit History**:
   - "Show commit history for file Z"
   - "Find all commits by user Y"
   - "Show commits between dates X and Y"

3. **Merge Analysis**:
   - "Find common ancestor between branches X and Y"
   - "Show all commits unique to branch X compared to branch Y"
   - "List all merge commits on branch X"

4. **User Activity**:
   - "Show all branches user X has committed to"
   - "Find the most active contributors"
   - "List commits by user X in the last week"

### Special Considerations for NLP Translation

1. **Entity Recognition**:
   - Branch names
   - Commit IDs
   - User IDs
   - Timestamps
   - File paths

2. **Relationship Understanding**:
   - Parent-child relationships between commits
   - Branch pointing relationships
   - User authorship relationships

3. **Temporal Queries**:
   - Date ranges
   - Relative time expressions ("last week", "recent commits")
   - Commit order and sequence

4. **Graph Traversal Patterns**:
   - Finding common ancestors
   - Tracing commit history
   - Branch divergence points

### Common Query Patterns

1. **path based queries**:
```cypher
MATCH path = (start:Node)-[:RELATIONSHIP*]->(end:Node)
WHERE start.property = $value
RETURN path
```

2. **aggregation qeries**:
```cypher
MATCH (u:User)-[:MADE_BY]-(c:Commit)
RETURN u.id, count(c) as commit_count
ORDER BY commit_count DESC
```

3. **Time-based Queries**:
```cypher
MATCH (c:Commit)
WHERE datetime(c.timestamp) > datetime($start_date)
RETURN c
```
Analyze this query: {query}

Determine if we need:
1. Only Neo4j subgraph data (for queries about commit history, user stats, etc.)
2. Both Neo4j data AND actual file versions from S3 (for merge conflicts, file diffs, etc.)

Return ONLY a Python dictionary with these keys:
- verdict: "yes" if we need S3 data, "no" if only Neo4j data needed
- branches: list of branch names if verdict is "yes" (for fetching their latest commits)
- query: Cypher query to get exactly the subgraph needed (no extra data)

Example responses:
For "find the latest commit in branch X":
{{
    "verdict" : "no",
    "branches" : [],
    "query" : "cypher MATCH (b:Branch {name: 'hotfix'})-[:HEAD*1]-(c:Commit) RETURN c.commit_id, c.message, c.timestamp order by c.timestamp desc 1"
}}

For "Show all commits by user X":
{{
    "verdict": "no",
    "branches": [],
    "query": "MATCH (u:User {{id: 'X'}})<-[:MADE_BY]-(c:Commit) RETURN c.id, c.message, c.timestamp ORDER BY c.timestamp DESC"
}}

For "Will branches feature and master have merge conflicts?":
{{
    "verdict": "yes",
    "branches": ["feature", "master"],
    "query": "MATCH (source:HEAD {{branch: 'feature'}})-[:POINTS_TO]->(source_commit:Commit), (target:HEAD {{branch: 'master'}})-[:POINTS_TO]->(target_commit:Commit) MATCH path1 = (source_commit)-[:PARENT*]->(a:Commit), path2 = (target_commit)-[:PARENT*]->(a) RETURN source_commit.id, target_commit.id, a.id as lca_id ORDER BY length(path1) + length(path2) LIMIT 1"
}}

NOTE: if a single commit node is retrieved , consider that itself as sufficient enough to answer such a question
Return only the dictionary, no other text.
"""
        
        try:
            final_prompt = prompt.replace("{query}", user_query)
            response = self.llm.chat.completions.create(
                model="meta-llama/Llama-3.3-70B-Instruct-Turbo-Free",
                messages=[
                    {"role": "system", "content": final_prompt},
                    {"role": "user", "content": user_query}
                ]
            )
            
            llm_response = response.choices[0].message.content.strip()
            logger.info(f"Raw LLM response: {llm_response}")
            
            # Try to clean up the response if it's not a valid Python dict
            if not llm_response.startswith('{'):
                llm_response = llm_response[llm_response.find('{'):llm_response.rfind('}')+1]
            
            # Parse the response
            try:
                result = ast.literal_eval(llm_response)
                if not isinstance(result, dict):
                    raise ValueError("Response is not a dictionary")
                if not all(k in result for k in ['verdict', 'branches', 'query']):
                    raise ValueError("Missing required keys in response")
                logger.info(f"LLM analysis completed in {time.time() - start_time:.2f} seconds")
                return result
            except (SyntaxError, ValueError) as e:
                logger.error(f"Failed to parse LLM response: {str(e)}")
                return {
                    "verdict": "no",
                    "branches": [],
                    "query": "MATCH (m:Commit {type: 'merge'}) RETURN m.id as commit_id, m.timestamp as time ORDER BY m.timestamp DESC LIMIT 1"
                }
                
        except Exception as e:
            logger.error(f"Error in LLM call: {str(e)}")
            raise

    def get_neo4j_data(self, cypher_query):
        with self.driver.session() as session:
            result = session.run(cypher_query)
            neo =  [dict(record) for record in result]          # dict(record) creates a dictionary of record object 
        print(neo)
        return neo

    def get_s3_versions(self, branches, neo4j_data):
        versions = {}
        print(neo4j_data)
        print(type(neo4j_data))
        # Get commit IDs from Neo4j data
        print("sexy")
        commit_ids = {
            "source": neo4j_data[0]['source_commit.id'],
            "target": neo4j_data[0]['target_commit.id'],          
            "lca": neo4j_data[0]['lca_id']
            }

        print(commit_ids)
        
        # Fetch versions for each commit
        for commit_type, commit_id in commit_ids.items():
            prefix = f"{commit_id}/"
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix                                                   # similar to the VCS.py file , same way to fetch the folders from s3 for each commit 
            )
            
            commit_files = {}
            for obj in response.get('Contents', []):
                key = obj['Key']
                if not key.endswith('/'):                                       # skip directories 
                    file_obj = self.s3_client.get_object(
                        Bucket=self.bucket_name,
                        Key=key
                    )
                    content = file_obj['Body'].read().decode('utf-8')
                    rel_path = key[len(prefix):]
                    commit_files[rel_path] = content
            
            versions[commit_type] = commit_files
        
        return versions

    def generate_response(self, query, neo4j_data, s3_data):
        context = {
            "query": query,
            "neo4j_data": neo4j_data,
            "s3_data": s3_data
        }
        
        prompt = """
        You are a VCS expert analyzing data from a Neo4j-based version control system.

        ### Available Data
        1. Neo4j subgraph data showing commit history, relationships, and metadata
        2. S3 file versions (if available) showing actual file contents for specific commits

        ### Query Context
        User Query: {query}

        ### Data to Analyze
        Neo4j Data: {neo4j_data}
        S3 Versions: {s3_data}

        ### Response Guidelines
        1. If analyzing merge conflicts:
           - Compare file versions between branches and LCA
           - Identify specific conflicts in each file
           - Suggest resolution strategies

        2. If analyzing commit history:
           - Summarize commit patterns
           - Highlight key changes
           - Identify relevant timestamps and authors

        3. If analyzing user activity:
           - Summarize contribution patterns
           - Highlight significant commits
           - Provide relevant statistics
           
        IF there is a clear cut answer: for example , which is the 2nd commit made, answer in a properly structured format, so it looks presentable
        Provide a clear, detailed response that directly answers the user's query using the available data.
        Aside from the clear explanation, if you have a definitive answer to give , give that neatly first , followed by any example
        Also, to determine the latest commit in any branch, you dont need the branch's entire history, just the commit node where the branch head points , so when asked about the latest commit, fetching only a single commit node pointed to by the head of the branch would suffice
        Dont be a reasoning agent, just give me whatever information you are able to, dont walk through it for me and dont give any warnings and caution messages
        just pure information 
        """
        
        response = self.llm.chat.completions.create(
            model="meta-llama/Llama-3.3-70B-Instruct-Turbo-Free",
            messages=[
                {"role": "system", "content": prompt},
                {"role": "user", "content": json.dumps(context)}
            ]
        )
        
        return response.choices[0].message.content

def main(query):
    logger.info(f"Starting RAG process for query: {query}")
    start_time = time.time()
    
    s3_client = boto3.client('s3')
    processor = VCSQueryProcessor(
        neo4j_uri="bolt://localhost:7689",
        neo4j_user="neo4j",
        neo4j_password="Bangalore@01",
        s3_client=s3_client
    )
    
    response = processor.process_query(query)
    logger.info(f"Total RAG process completed in {time.time() - start_time:.2f} seconds")
    return response

if __name__ == "__main__":
    main() 