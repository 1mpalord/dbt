import os
import json
import requests
from dotenv import load_dotenv
from pathlib import Path

# Load environment variables from .env in the same directory
load_dotenv()

# Configuration
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")
# Since this script is now in dbt/scripts/, the manifest is at ../target/manifest.json
MANIFEST_PATH = Path("target/manifest.json")

def main():
    if not N8N_WEBHOOK_URL:
        print("Error: N8N_WEBHOOK_URL environment variable is not set in .env")
        return

    if not MANIFEST_PATH.exists():
        print(f"Error: Manifest file not found at {MANIFEST_PATH}")
        print("Run 'dbt compile' or 'dbt run' first.")
        return

    try:
        print(f"Reading manifest from {MANIFEST_PATH}...")
        with open(MANIFEST_PATH, "r", encoding="utf-8") as file:
            manifest_data = json.load(file)

        nodes = manifest_data.get("nodes", {})
        sources = manifest_data.get("sources", {})
        metadata = manifest_data.get("metadata", {})

        payload = {
            "metadata": metadata,
            "project_context": {
                "total_nodes": len(nodes),
                "total_sources": len(sources)
            },
            "nodes": [
                {
                    "name": node.get("name"),
                    "description": node.get("description"),
                    "columns": node.get("columns", {}),
                    "raw_sql": node.get("raw_sql", "") or node.get("raw_code", ""),
                    "depends_on": node.get("depends_on", {})
                }
                for _, node in nodes.items() if node.get("resource_type") == "model"
            ]
        }

        print(f"Sending {len(payload['nodes'])} models to n8n...")
        headers = {'Content-Type': 'application/json'}
        response = requests.post(N8N_WEBHOOK_URL, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        print(f"Success! Sync status: {response.status_code}")

    except Exception as e:
        print(f"Error during sync: {e}")

if __name__ == "__main__":
    main()
