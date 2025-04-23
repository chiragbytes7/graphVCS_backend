from flask import Flask, jsonify, request
from flask_cors import CORS
from neo4j import GraphDatabase
import json
from rag1 import main
import time
import logging
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="backend/.env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_from_neo4j():
    start_time = time.time()
    
    uri = os.getenv("NEO4J_URI")
    user = os.getenv("NEO4J_USER")
    password = os.getenv("NEO4J_PASSWORD")
    
    driver = GraphDatabase.driver(uri, auth=(user, password))

    with driver.session() as session:
        logger.info("Fetching nodes from Neo4j...")
        node_result = session.run("match (n) return id(n) as id, labels(n) as labels, properties(n) as properties")
        edge_result = session.run("match (a) - [r] -> (b) return id(a) as source, id(b) as target, type(r) as type, properties(r) as properties")

        nodes = []
        for record in node_result:
            label = record["labels"][0] if record["labels"] else "Node"
            nodes.append({
                "id": record["id"],
                "label": f"{label}: {record['properties'].get('id', '')}",
                "group": label
            })
        
        edges = []
        for record in edge_result:
            edges.append({
                "from": record["source"],
                "to": record["target"],
                "label": record["type"],
                "arrows": "to"
            })
    driver.close()
    logger.info(f"Neo4j fetch completed in {time.time() - start_time:.2f} seconds")
    return {"nodes": nodes, "edges": edges}

def rag_query(user_input):
    start_time = time.time()
    logger.info(f"Processing RAG query: {user_input}")
    llm_response = main(user_input)
    logger.info(f"RAG processing completed in {time.time() - start_time:.2f} seconds")
    return {"response": llm_response}

app = Flask(__name__)
CORS(app)

@app.route('/yourVCS', methods=['POST'])
def display_graph():
    try:
        start_time = time.time()
        logger.info("Received graph visualization request")
        data = fetch_from_neo4j()
        logger.info(f"Graph request completed in {time.time() - start_time:.2f} seconds")
        return jsonify(data)
    except Exception as e:
        logger.error(f"Error in graph visualization: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/graph-rag', methods=['POST'])
def graph_rag():
    try:
        start_time = time.time()
        data = request.get_json()
        logger.info(f"Received RAG request with query: {data.get('query', '')}")
        
        if not data or 'query' not in data:
            return jsonify({"error": "No query provided"}), 400
        
        llm_answer = rag_query(data['query'])
        logger.info(f"Total RAG request completed in {time.time() - start_time:.2f} seconds")
        return jsonify(llm_answer)
    except Exception as e:
        logger.error(f"Error in RAG processing: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5005, host = '0.0.0.0') 



# app.py endpoint has to be modified to return even the rag answers

    