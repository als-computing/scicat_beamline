#!/usr/bin/env python3
"""
CPU Test Runner for experiment_flow.py

Runs tests that work on macOS/Linux without GPU requirements.
These tests verify basic functionality like volume binding, 
configuration, and non-GPU components.

Safe to run on:
- macOS (Intel or Apple Silicon)
- Linux (with or without GPUs)
- CI/CD environments
"""
import asyncio
import sys
from pathlib import Path
from typing import Optional

from docker_toolkit import check_docker

BASE_DIR = Path(__file__).parent.parent.absolute()


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


async def trigger_flow(
    deployment_name: str = "scicat-ingest-deployment",
    flow_name: str = "scicat-ingest-flow",
) -> Optional[str]:
    """
    Trigger a SciCat ingest via Prefect deployment.    
    
    Args:
        deployment_name: Name of the Prefect deployment
        flow_name: Name of the flow
        
    Returns:
        Flow run ID if successful, None otherwise
    """
    print(f"\n📋 Configuration:")
    print(f"   Deployment: {deployment_name}")
    print(f"   Flow: {flow_name}")

    from prefect import get_client
    from prefect.client.schemas.filters import (DeploymentFilter,
                                                DeploymentFilterName)

    async with get_client() as client:
        # Find the deployment
        print(f"🔍 Looking for deployment '{deployment_name}'...")

        deployment = await client.read_deployment_by_name(f"{flow_name}/{deployment_name}")
        if deployment is None:
            print(f"❌ Deployment '{deployment_name}' not found!")
            return None

        #dname:DeploymentFilterName = DeploymentFilterName()
        #dname.any_ = [deployment_name]

        #deployments = await client.read_deployments(
        #    deployment_filter=DeploymentFilter(
        #        name=dname
        #    )
        #)

        #if not deployments:
        #    print(f"❌ Deployment '{deployment_name}' not found!")
        #    return None
        #deployment = deployments[0]

        print(f"✅ Found deployment: {deployment.id}")
        print()
        
        # Create flow run with custom parameters
        print(f"🚀 Creating flow run...")
        flow_run = await client.create_flow_run_from_deployment(
            deployment.id,
            parameters={
                #"experiments": experiments,
            }
        )
        
        print(f"✅ Flow run created:")
        print(f"   ID: {flow_run.id}")
        print(f"   Name: {flow_run.name}")
        print(f"   State: {flow_run.state.type if flow_run.state else 'PENDING'}")
        


def main():
    print_header("SCICAT INGEST PREFECT FLOW TESTER")
    
    # Load .env file if it exists
    from dotenv import load_dotenv
    load_dotenv()

    # This must be loaded AFTER we load_dotenv or Prefect's settings will be based on
    # the 'ephemeral' local installation, which can be quite vexing.
    from prefect_toolkit import check_prefect_server

    # Check prerequisites
    print("🔍 Checking prerequisites...")
    
    # Check Prefect server
    if not check_prefect_server():
        return 1
    
    # Check Docker
    if not check_docker():
        return 1
    
    # Run tests
    print_header("RUNNING TEST")
    
    # Trigger the flow
    flow_run_id = asyncio.run(
        trigger_flow()
    )

    print("\nFinished.")

if __name__ == "__main__":
    sys.exit(main())
