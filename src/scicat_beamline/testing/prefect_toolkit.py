#!/usr/bin/env python
"""
Prefers tools for test runs

This script can create a work pool and a deployment.
"""

import argparse
import os
import subprocess
import sys
import time

from dotenv import load_dotenv
from prefect import get_client
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
from prefect.exceptions import ObjectAlreadyExists


def print_section(title):
    """Print a section header."""
    h = f"\n{'=' * (len(title)+4)}\n"
    print(f"{h}  {title}{h}")


def check_prefect_server():
    """Check if Prefect server is running."""
    url = os.environ.get('PREFECT_API_URL', 'http://localhost:4200/api')
    try:
        # Use the direct health endpoint
        health_result = subprocess.run(
            f"curl -f {url}/health",
            shell=True,
            check=False,  # Don't raise exception on non-zero exit
            text=True,
            capture_output=True
        )
        
        if health_result.returncode == 0 and health_result.stdout.strip() == "true":
            print("✅ Prefect server is running and healthy!")
            return True
        else:
            print("Server not responding or not healthy")
            if health_result.stdout:
                print(f"Response: {health_result.stdout}")
            if health_result.stderr:
                print(f"Error: {health_result.stderr}")
            print(f"Please ensure the Prefect server is running on {url}")
            return False
    except Exception as e:
        print(f"Error checking Prefect server status: {e}")
        return False


def create_work_pool(name="ingest_worker_pool", pool_type="process", base_job_template={}):
    """
    Create a Prefect work pool.
    
    Args:
        name: Name of the work pool
        pool_type: Type of work pool (process, kubernetes, docker, etc.)
        
    Returns:
        bool: True if work pool was created or already exists, False if failed
    """
    with get_client(sync_client=True) as client:

        print_section(f"Creating work pool '{name}' (type: {pool_type})")

        try:
            client.create_work_pool(
                WorkPoolCreate(
                    name=name,
                    type=pool_type,
                    base_job_template=base_job_template,
                    description="Process-based work pool for running flows from GitHub storage",
                )
            )
            print(f"✅ Work pool '{name}' created successfully!")
        except ObjectAlreadyExists:
            print(f"✅ Work pool '{name}' already exists. Updating...")
            client.update_work_pool(
                work_pool_name=name,
                work_pool=WorkPoolUpdate(
                    base_job_template=base_job_template,
                    is_paused=False,
                    description="Process-based work pool for running flows from GitHub storage",
                    concurrency_limit=None,
                ),
            )
        except Exception as e:
            print(f"❌ Error creating/updating work pool: {e}")
            return False
    return True


def start_worker(pool_name="ingest_worker_pool", wait_seconds=5):
    """Start a Prefect worker in the background."""
    print_section(f"Starting worker for pool '{pool_name}'")
    
    # Start worker in the background
    worker_process = subprocess.Popen(
        f"prefect worker start -p {pool_name}",
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True
    )
    
    print(f"Worker started with PID: {worker_process.pid}")
    print(f"Waiting {wait_seconds} seconds for worker to initialize...")
    time.sleep(wait_seconds)
    
    # Check if worker is still running
    if worker_process.poll() is None:
        print("Worker is running!")
        return worker_process
    else:
        print("Worker failed to start.")
        stdout, _ = worker_process.communicate()
        print(f"Worker output: {stdout}")
        return None


def create_deployment():

    from prefect.blocks.system import Secret
    from prefect.runner.storage import GitRepository

    from scicat_beamline.testing.flow import scicat_ingest_flow

    print("Creating deployment...")

    source_url = "https://github.com/als-computing/scicat_beamline_ingestion.git"

    # Parse command line arguments
    # --branch <name>: Deploy from a specific branch (default: main)
    source_branch = "main"
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
        from pydantic import SecretStr

        print("   Using GitHub token for authentication")
        
        # Create or update Secret block for GitHub token
        try:
            secret_block = Secret.load("github-token")
            print("   Found existing GitHub token secret")
        except:
            print("   Creating GitHub token secret block")
            secret_block = Secret(value=SecretStr(github_token)) # Prefect 3 wants a SecretStr
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
        "ingester_spec": "bltest",
        "dataset_path": "bltest", # Base folder will come from the environment
    }

    tags = ["scicat", "beamline", "ingest"]

    try:
        scicat_ingest_flow_from_source = scicat_ingest_flow.from_source(
            source=source,
            entrypoint="src/scicat_beamline/testing/flow.py:scicat_ingest_flow"
        )

        deployment_id = scicat_ingest_flow_from_source.deploy(
            name="scicat-ingest-deployment",
            work_pool_name="ingest_worker_pool",
            work_queue_name="ingest_worker_queue",
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


def main():
    """Main entry point."""
    args = argparse.ArgumentParser(description="Prefect management tools")
    args.add_argument("--create_work_pool", "-w", action='store_true', dest='create_work_pool',
                      help="Create work pool")
    args.add_argument("--create_deployment", "-d", action='store_true', dest='create_deployment',
                      help="Create deployment")
    args = args.parse_args()

    load_dotenv()

    # Set default to local server
    if 'PREFECT_API_URL' not in os.environ:
        os.environ['PREFECT_API_URL'] = 'http://localhost:4200/api'
        print(f"⚠️ Using default URL for prefect: {os.environ['PREFECT_API_URL']}")

    if args.create_work_pool:
        return create_work_pool(name="ingest_worker_pool")
    if args.create_deployment:
        return create_deployment()

    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
