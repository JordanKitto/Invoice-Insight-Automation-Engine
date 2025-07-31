from Job_Runner import vendor_master_runner, transaction_master_runner
import os
import time

def run_step(step_name, runner_function):
    print(f"Starting step: {step_name}")
    runner_function()

    # Wait for corresponding done.txt
    done_file = os.path.join("Output_Files", "done.txt")
    timeout = 5 # seconds
    waited = 0
    while not os.path.exists(done_file) and waited < timeout:
        print(f"\rWaiting for {done_file}...")
        time.sleep(1)
        waited += 1

    if os.path.exists(done_file):
        print(f"{step_name} completed successfully.\n")
        os.remove(done_file)
    else:
        print(f"Timeout waiting for {done_file} - skipping remaining steps")
        return False
    return True

def main():
    if not run_step("Vendor Master Export", vendor_master_runner.main):
        return
    if not run_step("Transaction Master Export", transaction_master_runner.main):
        return
    
if __name__ == "__main__":
    main()

