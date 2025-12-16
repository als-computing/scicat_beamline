#!/usr/bin/env python
"""
Create a Prefect work pool for GPU resource-aware deployments.

This script creates a "import_worker_pool" work pool that can be used for
running deployments that need to check GPU resources.
"""

import argparse
import os
import subprocess
import sys
import time

from prefect import get_client
from prefect.client.schemas.actions import WorkPoolCreate, WorkPoolUpdate
from prefect.exceptions import ObjectAlreadyExists


def print_section(title):
    """Print a section header."""
    h = f"\n{'=' * (len(title)+4)}\n"
    print(f"{h}  {title}{h}")


def check_prefect_server():
    """Check if Prefect server is running."""
    print_section("Checking Prefect server status")
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
            print("Server is running and healthy!")
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


def create_work_pool(name="import_worker_pool", pool_type="process", base_job_template={}):
    """
    Create a Prefect work pool.
    
    Args:
        name: Name of the work pool
        pool_type: Type of work pool (process, kubernetes, docker, etc.)
        
    Returns:
        bool: True if work pool was created or already exists, False if failed
    """
    # Note: We can't use the Prefect client to do this until we upgrade to Prefect 3,
    # and splash_flows is currently on Prefect 2.
    import json

    job_template_str = json.dumps(base_job_template)

    command = f"""
        prefect work-pool create \
            --type "{pool_type}" \
            --paused false "{name}" || \
        prefect work-pool update \
            --description "Process-based work pool for running flows from GitHub storage" \
            "{name}"
    """
    print_section(f"Creating work pool '{name}' (type: {pool_type})")
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            text=True,
            capture_output=True
        )
        print(f"✅ Work pool '{name}' created/updated successfully!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error creating/updating work pool: {e.stderr}")
        return False
    return True
    

def create_work_pool_prefect3(name="import_worker_pool", pool_type="process", base_job_template={}):
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


def start_worker(pool_name="import_worker_pool", wait_seconds=5):
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


def main():
    """Main entry point."""
    args = argparse.ArgumentParser(description="Prefect management tools")
    args.add_argument("--create_work_pool", "-w", action='store_true', dest='create_work_pool',
                      help="Create work pool")
    args = args.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    # Set default to local server
    if 'PREFECT_API_URL' not in os.environ:
        os.environ['PREFECT_API_URL'] = 'http://localhost:4200/api'
        print(f"⚠️ Using default URL for prefect: {os.environ['PREFECT_API_URL']}")

    if args.create_work_pool:
        return create_work_pool(name="import_worker_pool")

    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
