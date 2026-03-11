import subprocess
import os
import signal

def get_gpu_processes():
    cmd = [
        "nvidia-smi",
        "--query-compute-apps=pid,process_name,used_memory",
        "--format=csv,noheader,nounits"
    ]
    
    result = subprocess.run(cmd, capture_output=True, text=True)
    lines = result.stdout.strip().split("\n")
    
    processes = []
    for line in lines:
        if line.strip() == "":
            continue
        pid, name, mem = [x.strip() for x in line.split(",")]
        processes.append({
            "pid": int(pid),
            "name": name,
            "memory_mb": int(mem)
        })
    
    return processes


def kill_process(pid):
    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Killed PID {pid}")
    except ProcessLookupError:
        print(f"Process {pid} already gone")


def main():
    procs = get_gpu_processes()
    
    if not procs:
        print("No GPU processes found.")
        return
    
    print("GPU Processes:")
    for p in procs:
        print(f"PID {p['pid']} | {p['name']} | {p['memory_mb']} MB")
    
    confirm = input("Kill all GPU processes? (y/n): ")
    
    if confirm.lower() == "y":
        for p in procs:
            kill_process(p["pid"])
    else:
        print("Aborted.")


if __name__ == "__main__":
    main()