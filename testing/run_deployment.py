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
import sys
import asyncio
from pathlib import Path
from prefect import get_client
from prefect.client.schemas.filters import DeploymentFilter, DeploymentFilterName
from typing import Dict, Any, Optional, Tuple

from docker_toolkit import check_docker
from prefect_toolkit import check_prefect_server


def print_header(text):
    """Print a formatted header."""
    print("\n" + "=" * 70)
    print(text)
    print("=" * 70)


def load_env_file():
    """Load .env file if it exists."""
    from pathlib import Path
    from dotenv import load_dotenv

    env_file = Path(__file__).parent / '.env'
    if env_file.exists():
        print("  ℹ️  Loading .env file...")
        load_dotenv(env_file)
        return True
    return False


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

    dname:DeploymentFilterName = DeploymentFilterName()
    dname.any_ = [deployment_name]

    async with get_client() as client:
        # Find the deployment
        print(f"🔍 Looking for deployment '{deployment_name}'...")
        
        deployments = await client.read_deployments(
            deployment_filter=DeploymentFilter(
                name=dname
            )
        )

        print(f"deployments:")
        for r in deployments:
            print(r.model_dump())

        if not deployments:
            print(f"❌ Deployment '{deployment_name}' not found!")
            print(f"\n💡 Make sure you've deployed the flow:")
            print(f"   python deployments/deployment_experiments.py --branch <your-branch>")
            return None
        
        deployment = deployments[0]
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
        print()
        


def main():
    print_header("SCICAT INGEST PREFECT FLOW TESTER")
    
    # Load .env file if it exists
    load_env_file()
    
    # Check prerequisites
    print("🔍 Checking prerequisites...")
    
    # Check Prefect server
    if not check_prefect_server():
        return 1
    
    # Check Docker
    if not check_docker():
        return 1
    
    # Platform info
    import platform
    print(f"  ℹ️  Platform: {platform.system()} {platform.machine()}")
    
    # Run tests
    print_header("RUNNING TEST")
    
    # Trigger the flow
    flow_run_id = asyncio.run(
        trigger_flow()
    )
    
    # Print result
    print(flow_run_id)

    print("\nFinished.")
    return 0 if flow_run_id else 1

if __name__ == "__main__":
    sys.exit(main())
