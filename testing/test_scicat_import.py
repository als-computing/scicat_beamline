#!/usr/bin/env python3
"""
Test: Volume Binding Verification (CPU-only, macOS compatible)

This test verifies that:
1. Input files/directories can be mounted and read by containers
2. Output directories can be written to by containers
3. Files written by containers are accessible from the host
4. Environment variables are passed correctly

This test does NOT require GPUs and should work on:
- macOS (Intel/Apple Silicon with Docker Desktop)
- Linux (with or without GPUs)
"""
import os, sys, time
import json
import docker
import tempfile
from pathlib import Path

# We'll test volume binding without GPU requirements

from prefect import flow, task, get_run_logger

from testing_toolkit import check_docker, check_and_build_image

from prefect_resource_check.flows.experiment_flow import run_experiments


@task
def run_containerized_experiment_cpu(
    algorithm_image: str,
    input_file: str,
    output_dir: str,
    params: dict,
    container_command: list,
    data_base_path: str
) -> dict:
    """
    Run a container experiment WITHOUT GPU requirements.
    For CPU-only testing of volume binding.
    """
    logger = get_run_logger()
    
    # Construct full paths
    full_input_path = os.path.join(data_base_path, input_file)
    full_output_path = os.path.join(data_base_path, output_dir)
    
    # Ensure output directory exists
    os.makedirs(full_output_path, exist_ok=True)
    
    logger.info(f"🐳 Starting CPU-only experiment")
    logger.info(f"   Image: {algorithm_image}")
    logger.info(f"   Input: {full_input_path}")
    logger.info(f"   Output: {full_output_path}")
    logger.info(f"   Params: {params}")
    
    try:
        client = docker.from_env()
        
        # Prepare environment variables
        env_vars = {
            "PARAMS_JSON": json.dumps(params),
            "INPUT_FILE": "/input",
            "OUTPUT_DIR": "/output"
        }
        
        # Add all params as individual env vars too (for convenience)
        for key, value in params.items():
            env_vars[f"PARAM_{key.upper()}"] = str(value)
        
        # Run container WITHOUT GPU device requests
        start_time = time.time()
        
        container = client.containers.run(
            image=algorithm_image,
            command=container_command,
            environment=env_vars,
            volumes={
                full_input_path: {"bind": "/input", "mode": "ro"},
                full_output_path: {"bind": "/output", "mode": "rw"}
            },
            # NO device_requests - CPU only!
            detach=False,  # Wait for completion
            remove=True,   # Auto-remove after completion
            stdout=True,
            stderr=True
        )
        
        elapsed_time = time.time() - start_time
        
        # Get logs (container is already removed, so we got logs from run())
        logs = container.decode('utf-8') if isinstance(container, bytes) else str(container)
        
        logger.info(f"✅ Experiment completed in {elapsed_time:.1f}s")
        
        return {
            "success": True,
            "elapsed_time": elapsed_time,
            "logs": logs,
            "output_dir": full_output_path,
            "input_file": full_input_path,
            "params": params
        }
        
    except docker.errors.ImageNotFound:
        logger.error(f"❌ Docker image not found: {algorithm_image}")
        return {
            "success": False,
            "error": f"Image not found: {algorithm_image}"
        }
        
    except Exception as e:
        logger.error(f"❌ Error running experiment: {e}")
        return {
            "success": False,
            "error": str(e)
        }

def setup_test_data(base_dir: Path):
    """Create test input files and directory structure."""
    print(f"📁 Setting up test data in {base_dir}")
    
    # Create input file
    input_file = base_dir / "inputs" / "test_input.txt"
    input_file.parent.mkdir(parents=True, exist_ok=True)
    
    with open(input_file, 'w') as f:
        f.write("This is test input data.\n")
        f.write("Line 2 of test data.\n")
        f.write("Line 3 of test data.\n")
    
    print(f"  ✅ Created input file: {input_file}")
    
    # Create output directories
    output_dirs = [
        base_dir / "outputs" / "exp1",
        base_dir / "outputs" / "exp2",
    ]
    
    for out_dir in output_dirs:
        out_dir.mkdir(parents=True, exist_ok=True)
    
    return str(input_file.relative_to(base_dir))

def verify_output(base_dir: Path, experiment_name: str, expected_files: list):
    """Verify that expected output files were created."""
    output_dir = base_dir / "outputs" / experiment_name
    
    print(f"\n🔍 Verifying output for {experiment_name}:")
    print(f"  Directory: {output_dir}")
    
    if not output_dir.exists():
        print(f"  ❌ Output directory does not exist!")
        return False
    
    all_files = list(output_dir.iterdir())
    print(f"  Found {len(all_files)} files:")
    for f in all_files:
        if f.is_file():
            size = f.stat().st_size
            print(f"    - {f.name} ({size} bytes)")
    
    # Check expected files
    success = True
    for expected_file in expected_files:
        file_path = output_dir / expected_file
        if file_path.exists():
            size = file_path.stat().st_size
            print(f"  ✅ {expected_file} exists ({size} bytes)")
            
            # Print content preview for text files
            if expected_file.endswith('.txt') or expected_file.endswith('.log'):
                try:
                    with open(file_path, 'r') as f:
                        content = f.read(200)
                        preview = content[:100].replace('\n', ' ')
                        print(f"     Preview: {preview}...")
                except Exception as e:
                    print(f"     Could not read file: {e}")
            elif expected_file.endswith('.json'):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        print(f"     JSON keys: {list(data.keys())}")
                except Exception as e:
                    print(f"     Could not parse JSON: {e}")
        else:
            print(f"  ❌ {expected_file} NOT FOUND")
            success = False
    
    return success

@flow(name="cpu-volume-binding-test")
def run_cpu_volume_test(base_dir: Path, experiments: list):
    """
    Run volume binding tests without GPU requirements.
    
    This flow doesn't use GPU acquisition - it runs containers
    with CPU only to test volume binding.
    """
    results = []
    
    for exp in experiments:
        # Run without GPU using our CPU-only function
        result = run_containerized_experiment_cpu(
            algorithm_image=exp["algorithm_image"],
            input_file=exp["input_file"],
            output_dir=exp["output_dir"],
            params=exp.get("params", {}),
            container_command=exp.get("command", ["python3", "/app/test_volume_binding.py"]),
            data_base_path=str(base_dir)
        )
        results.append(result)
    
    return results


def main():
    print("=" * 70)
    print("TEST: VOLUME BINDING VERIFICATION (CPU-only)")
    print("=" * 70)
    print("This test works on macOS and Linux without GPU requirements")
    print("=" * 70)
    print()
    
    # Check Docker
    if not check_docker():
        return 1
    
    # Check test image
    if not check_and_build_image(image_name="prefect-test-volume:latest", docker_file=str(Path(__file__).parent.parent.parent / "test_images" / "Dockerfile.volume_test")):
        print("\n💡 You may need to manually build the test image to proceed.")
        return 1
    
    # Check for --keep-files flag to preserve test data for debugging
    import sys
    keep_files = "--keep-files" in sys.argv
    
    if keep_files:
        # Create a persistent directory for debugging
        import time
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        base_dir = Path(f"/tmp/prefect_test_{timestamp}")
        base_dir.mkdir(parents=True, exist_ok=True)
        print(f"\n📂 Test directory: {base_dir}")
        print(f"⚠️  --keep-files flag: Directory will NOT be deleted after test\n")
        
        try:
            return_code = run_test(base_dir)
        finally:
            if return_code == 0:
                print(f"\n💾 Test files preserved at: {base_dir}")
            else:
                print(f"\n💾 Test files preserved for debugging at: {base_dir}")
        
        return return_code
    else:
        # Use temporary directory (auto-cleanup)
        # If a HOST_DATA_PATH exists, we make a temp folder inside it, otherwise we use a system temp folder
        with tempfile.TemporaryDirectory(dir=os.getenv('HOST_DATA_PATH', None)) as tmpdir:
            base_dir = Path(tmpdir)
            print(f"\n📂 Test directory: {base_dir}")
            print(f"ℹ️  Temporary directory - will be deleted after test")
            print(f"   (Use --keep-files flag to preserve test data)\n")
            
            return run_test(base_dir)


def run_test(base_dir: Path) -> int:
    """Run the test with the given base directory."""
    # Setup test data
    input_file_rel = setup_test_data(base_dir)
    
    # Define experiments (without GPU requirements)
    experiments = [
        {
            "name": "exp1",
            "algorithm_image": "prefect-test-volume:latest",
            "input_file": input_file_rel,
            "output_dir": "outputs/exp1",
            "params": {
                "test_param_1": "value_1",
                "test_param_2": 42,
                "test_param_3": 3.14
            },
            "command": ["python3", "/app/test_volume_binding.py"]
        },
        {
            "name": "exp2",
            "algorithm_image": "prefect-test-volume:latest",
            "input_file": input_file_rel,
            "output_dir": "outputs/exp2",
            "params": {
                "different_param": "different_value",
                "number_param": 999
            },
            "command": ["python3", "/app/test_volume_binding.py"]
        }
    ]
    
    print("\n" + "=" * 70)
    print("RUNNING EXPERIMENTS (CPU-only, no GPU required)")
    print("=" * 70)
    print()
    
    # Run experiments using our CPU-only flow
    try:
        results = run_cpu_volume_test(base_dir, experiments)
    except Exception as e:
        print(f"❌ Error running experiments: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    print("\n" + "=" * 70)
    print("VERIFYING RESULTS")
    print("=" * 70)
    
    # Verify results
    expected_files = ["test_output.txt", "results.json", "execution.log"]
    
    all_success = True
    for i, result in enumerate(results):
        exp_name = experiments[i]["name"]
        print(f"\n📊 Experiment: {exp_name}")
        
        if result.get("success"):
            print(f"  ✅ Execution successful")
            print(f"  Elapsed time: {result.get('elapsed_time', 0):.2f}s")
            
            # Verify output files
            if verify_output(base_dir, exp_name, expected_files):
                print(f"  ✅ All expected output files found")
            else:
                print(f"  ❌ Some output files missing")
                all_success = False
        else:
            print(f"  ❌ Execution failed: {result.get('error', 'Unknown error')}")
            if 'logs' in result:
                print(f"  Container logs:\n{result['logs'][:500]}")
            all_success = False
    
    print("\n" + "=" * 70)
    if all_success:
        print("✅ TEST PASSED: Volume binding is working correctly!")
        print("\n💡 This confirms:")
        print("   • Docker containers can read mounted input files")
        print("   • Docker containers can write to mounted output directories")
        print("   • Output files persist after container exits")
        print("   • Environment variables are passed correctly")
    else:
        print("❌ TEST FAILED: Issues with volume binding")
    print("=" * 70)
    
    return 0 if all_success else 1

if __name__ == "__main__":
    sys.exit(main())
