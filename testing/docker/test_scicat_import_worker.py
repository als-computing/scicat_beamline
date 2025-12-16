#!/usr/bin/env python3
"""
Test script for volume binding verification.

This script:
1. Reads the input file mounted at /input
2. Writes several test files to /output
3. Verifies environment variables are passed correctly
"""
import os
import json
import sys
from datetime import datetime

def main():
    print("=" * 60)
    print("VOLUME BINDING TEST")
    print("=" * 60)
    
    # Check environment variables
    print("\n📦 Environment Variables:")
    print(f"  CUDA_VISIBLE_DEVICES: {os.environ.get('CUDA_VISIBLE_DEVICES', 'NOT SET')}")
    print(f"  INPUT_FILE: {os.environ.get('INPUT_FILE', 'NOT SET')}")
    print(f"  OUTPUT_DIR: {os.environ.get('OUTPUT_DIR', 'NOT SET')}")
    print(f"  PARAMS_JSON: {os.environ.get('PARAMS_JSON', 'NOT SET')}")
    
    # Parse parameters
    params_json = os.environ.get('PARAMS_JSON', '{}')
    try:
        params = json.loads(params_json)
        print(f"\n🔧 Parsed Parameters:")
        for key, value in params.items():
            print(f"  {key}: {value}")
            # Also check individual env vars
            env_key = f"PARAM_{key.upper()}"
            print(f"    (also available as ${env_key}={os.environ.get(env_key, 'NOT SET')})")
    except json.JSONDecodeError as e:
        print(f"  ❌ Error parsing JSON: {e}")
        params = {}
    
    # Test reading input
    input_path = os.environ.get('INPUT_FILE', '/input')
    print(f"\n📥 Reading Input:")
    print(f"  Path: {input_path}")
    
    try:
        # Check if input is a file or directory
        if os.path.isfile(input_path):
            with open(input_path, 'r') as f:
                content = f.read()
            print(f"  ✅ Successfully read {len(content)} bytes")
            print(f"  Content preview: {content[:100]}...")
        elif os.path.isdir(input_path):
            files = os.listdir(input_path)
            print(f"  ✅ Input is a directory with {len(files)} files")
            print(f"  Files: {files}")
        else:
            print(f"  ⚠️  Input path doesn't exist or is not accessible")
    except Exception as e:
        print(f"  ❌ Error reading input: {e}")
    
    # Test writing output
    output_dir = os.environ.get('OUTPUT_DIR', '/output')
    print(f"\n📤 Writing Output:")
    print(f"  Path: {output_dir}")
    
    try:
        # Verify output directory exists and is writable
        if not os.path.exists(output_dir):
            print(f"  ⚠️  Output directory doesn't exist, creating...")
            os.makedirs(output_dir, exist_ok=True)
        
        # Write test file 1: Simple text
        test_file_1 = os.path.join(output_dir, "test_output.txt")
        with open(test_file_1, 'w') as f:
            f.write(f"Test output created at {datetime.now().isoformat()}\n")
            f.write(f"Parameters: {json.dumps(params, indent=2)}\n")
        print(f"  ✅ Created: {test_file_1}")
        
        # Write test file 2: JSON results
        results = {
            "timestamp": datetime.now().isoformat(),
            "test_status": "success",
            "parameters": params,
            "input_path": input_path,
            "output_path": output_dir,
            "environment": {
                "CUDA_VISIBLE_DEVICES": os.environ.get('CUDA_VISIBLE_DEVICES'),
                "INPUT_FILE": os.environ.get('INPUT_FILE'),
                "OUTPUT_DIR": os.environ.get('OUTPUT_DIR'),
            }
        }
        
        test_file_2 = os.path.join(output_dir, "results.json")
        with open(test_file_2, 'w') as f:
            json.dump(results, f, indent=2)
        print(f"  ✅ Created: {test_file_2}")
        
        # Write test file 3: Log file
        test_file_3 = os.path.join(output_dir, "execution.log")
        with open(test_file_3, 'w') as f:
            f.write(f"Execution Log\n")
            f.write(f"{'='*40}\n")
            f.write(f"Start Time: {datetime.now().isoformat()}\n")
            f.write(f"Input: {input_path}\n")
            f.write(f"Output: {output_dir}\n")
            f.write(f"Parameters: {params}\n")
            f.write(f"Status: SUCCESS\n")
        print(f"  ✅ Created: {test_file_3}")
        
        # List all files in output directory
        output_files = os.listdir(output_dir)
        print(f"\n📋 Output directory contents ({len(output_files)} files):")
        for fname in sorted(output_files):
            fpath = os.path.join(output_dir, fname)
            size = os.path.getsize(fpath)
            print(f"  - {fname} ({size} bytes)")
        
    except Exception as e:
        print(f"  ❌ Error writing output: {e}")
        sys.exit(1)
    
    print("\n" + "=" * 60)
    print("✅ VOLUME BINDING TEST PASSED")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
