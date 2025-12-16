#!/usr/bin/env python
"""
Tools for dealing with Docker
"""

import os, sys
import argparse
import subprocess
from pathlib import Path


def check_docker():
    """Check if Docker is available and running."""
    import docker
    try:
        client = docker.from_env()
        # Try to ping Docker
        client.ping()
        print("✅ Docker is available and running")
        return True
    except Exception as e:
        print(f"❌ Docker not available: {e}")
        print("\n💡 Please make sure Docker Desktop is running")
        return False


def check_and_build_image(image_name="prefect-test-volume:latest", docker_file="Dockerfile.volume_test"):
    """Check if test image exists, build if needed.
    
    Args:
        image_name: Name/tag for the Docker image
        docker_file: Path to Dockerfile (can be absolute or relative to repo root)
    """
    import docker
    client = docker.from_env()
    
    try:
        client.images.get(image_name)
        print(f"✅ Image {image_name} found")
        return True
    except docker.errors.ImageNotFound:
        print(f"⚠️ Image {image_name} not found, attempting to build...")
        
        # Convert to absolute path
        dockerfile_path = Path(docker_file).absolute()
        
        if not dockerfile_path.exists():
            print(f"❌ Dockerfile {dockerfile_path} not found.")
            return False
        
        # Build from repo root (parent of parent of this file)
        # This assumes testing_toolkit is at repo_root/testing_toolkit/
        repo_root = Path(__file__).parent.parent.absolute()
        
        # Make dockerfile path relative to repo root for the -f flag
        try:
            dockerfile_rel = dockerfile_path.relative_to(repo_root)
        except ValueError:
            # If dockerfile is not under repo_root, use absolute path
            dockerfile_rel = dockerfile_path
        
        print(f"   Building from: {repo_root}")
        print(f"   Dockerfile: {dockerfile_rel}")
        
        try:
            import subprocess
            result = subprocess.run(
                ["docker", "build", "-f", str(dockerfile_rel), "-t", image_name, "."],
                cwd=repo_root,  # Build from repo root, not dockerfile directory
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout
            )
            
            if result.returncode == 0:
                print("✅ Image built successfully")
                return True
            else:
                print(f"❌ Failed to build image:")
                print(result.stderr)
                return False
        except Exception as e:
            print(f"❌ Error building image: {e}")
            return False


def login_to_ghcr():
    """Login to GitHub Container Registry using credentials from .env"""
    print("\n" + "=" * 70)
    print("GHCR AUTHENTICATION")
    print("=" * 70)
    
    username = os.getenv('GITHUB_USERNAME')
    token = os.getenv('GITHUB_TOKEN')
    
    if not username:
        print("❌ GITHUB_USERNAME not found in .env")
        print("   Please add: GITHUB_USERNAME=your_github_username")
        return False
    
    if not token:
        print("❌ GITHUB_TOKEN not found in .env")
        print("   Please add a token with 'read:packages' scope")
        return False
    
    print(f"📝 Username: {username}")
    print(f"🔑 Token: {'*' * 20} (hidden)")
    print()
    
    # Login to GHCR
    try:
        cmd = f"echo {token} | docker login ghcr.io -u {username} --password-stdin"
        result = subprocess.run(
            cmd,
            shell=True,
            check=True,
            text=True,
            capture_output=True
        )
        print("✅ Successfully logged in to ghcr.io")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to login to ghcr.io:")
        print(f"   {e.stderr}")
        return False


def main():
    """Main entry point."""
    args = argparse.ArgumentParser(description="Prefect management tools")
    args.add_argument("--check_docker", "-d", action='store_true', dest='check_docker',
                      help="Check to see if Docker is available")
    args.add_argument("--login_to_ghcr", "-l", action='store_true', dest='login_to_ghcr',
                      help="Log in to GHCR using .env credentials")
    args = args.parse_args()

    from dotenv import load_dotenv
    load_dotenv()

    if args.check_docker:
        return check_docker()
    if args.login_to_ghcr:
        return login_to_ghcr()

    return True


if __name__ == "__main__":
    sys.exit(0 if main() else 1)
