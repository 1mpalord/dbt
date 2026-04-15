import os
import sys
import subprocess
from dotenv import load_dotenv

# Load variables from .env into os.environ
load_dotenv()

def main():
    if len(sys.argv) < 2:
        print("Usage: python run_dbt.py <dbt_command>")
        print("Example: python run_dbt.py debug")
        return

    # Extract the dbt command arguments
    dbt_args = sys.argv[1:]
    
    # Path to the dbt executable in the venv
    dbt_exe = os.path.join(".venv", "Scripts", "dbt.exe")
    
    if not os.path.exists(dbt_exe):
        print(f"Error: dbt executable not found at {dbt_exe}")
        return

    # Construct the full command
    # We always append --profiles-dir . for convenience
    cmd = [dbt_exe] + dbt_args + ["--profiles-dir", "."]
    
    print(f"🚀 Running: {' '.join(cmd)}")
    
    try:
        # Run dbt with the loaded environment variables
        result = subprocess.run(cmd, env=os.environ)
        sys.exit(result.returncode)
    except Exception as e:
        print(f"Error executing dbt: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
