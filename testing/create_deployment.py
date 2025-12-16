"""
Deployment configuration for the SciCat ingestion Docker container flow.

This file creates a Prefect deployment that can be scheduled and executed by a Prefect worker.
"""

import os
from pathlib import Path

from scicat_beamline_ingestion.flows.scicat_ingest_flow import \
    scicat_ingest_flow

BASE_DIR = Path(__file__).parent.parent.absolute()

# Apply the deployment using the newer API
if __name__ == "__main__":
    import sys

    from dotenv import load_dotenv
    load_dotenv()

    # Set default to local server
    if 'PREFECT_API_URL' not in os.environ:
        os.environ['PREFECT_API_URL'] = 'http://localhost:4200/api'
        print(f"⚠️ Using default URL for prefect: {os.environ['PREFECT_API_URL']}")

    print("Creating deployment...")
    print(f"Flow code location: {BASE_DIR}\n")

    from prefect.runner.storage import GitRepository

    source_url = "https://github.com/als-computing/scicat_beamline_ingestion.git"

    # Parse command line arguments
    # --branch <name>: Deploy from a specific branch (default: main)
    source_branch = "2025/12/10-flow"
    if "--branch" in sys.argv:
        try:
            branch_idx = sys.argv.index("--branch")
            source_branch = sys.argv[branch_idx + 1]
        except (IndexError, ValueError):
            print("❌ Error: --branch flag requires a branch name")
            print("Usage: python deployments/deployment.py --branch <branch-name>")
            sys.exit(1)
    
    print(f"🐙 Deploying from GitHub branch: {source_branch}")
    print("   Workers will clone code from GitHub\n")
    
    # Use HTTPS URL (works in Docker without SSH keys)
    # For private repos, set GITHUB_TOKEN in .env file
    github_token = os.getenv("GITHUB_TOKEN")
    
    if github_token:
        from prefect.blocks.system import Secret
        #from pydantic import SecretStr

        print("   Using GitHub token for authentication")
        
        # Create or update Secret block for GitHub token
        try:
            secret_block = Secret.load("github-token")
            print("   Found existing GitHub token secret")
        except:
            print("   Creating GitHub token secret block")
            #secret_block = Secret(value=SecretStr(github_token)) # Prefect 3 wants a SecretStr
            secret_block = Secret(value=github_token)
            secret_block.save("github-token", overwrite=True)

        source = GitRepository(
            url=source_url,
            branch=source_branch,
            credentials={"access_token": secret_block}
        )
    else:
        print("   No GitHub token found - repository must be public")
        source = GitRepository(
            url=source_url,
            branch=source_branch,
        )

    parameters = {
        "ingester_spec": os.getenv("INGEST_SPEC", "blTEST"),
        "dataset_path": os.getenv("IMPORT_SUBFOLDER", "bltest"),
        "ingest_user": os.getenv("INGEST_USER", "datasetIngestor"),
        "base_url": os.getenv("SCICAT_URL", "https://dataportal-staging.als.lbl.gov/api/v3"),
        "username": os.getenv("SCICAT_USERNAME", None),
        "password": os.getenv("SCICAT_PASSWORD", None)
    }

    tags = ["scicat", "beamline", "ingest"]

    try:
        # Create deployment with chosen source
        deployment_id = scicat_ingest_flow.from_source(
            source=source,
            entrypoint="scicat_beamline_ingestion/flows/scicat_ingest_flow.py:scicat_ingest_flow"
        ).deploy(
            name="scicat-ingest-deployment",
            work_pool_name="import_worker_pool",
            work_queue_name="import_worker_queue",
            parameters=parameters,
            tags=tags,
        )
        
        print(f"✅ Deployment 'scicat-ingest-deployment' created successfully!")
        print(f"   Deployment ID: {deployment_id}")
        print(f"   Source: {source_url} branch {source_branch}")
            
    except Exception as e:
        print(f"❌ Error creating deployment: {e}")
        print(f"\n💡 Make sure:")
        print(f"   - Prefect server is running")
        print(f"   - Work pool exists")
        print(f"   - Code is pushed to GitHub on branch '{source_branch}'")
        raise
