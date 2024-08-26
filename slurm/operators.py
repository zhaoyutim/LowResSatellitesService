from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import subprocess
import os
import time
from datetime import timedelta

class SlurmOperator(BaseOperator):
    @apply_defaults
    def __init__(self, script, conda_path, env, log_path, script_args=[], mem_per_cpu='4600M', cpus_per_task=8, num_gpus=1, poke_interval=60, timeout=3600, *args, **kwargs):
        super(SlurmOperator, self).__init__(*args, **kwargs)
        self.poke_interval = poke_interval
        self.timeout = timeout
        self.log_path = log_path
        os.makedirs(log_path,exist_ok=True)

        self.batch_script_content = [
        "#!/usr/bin/env bash",
        f"#SBATCH --output={log_path}/%J_slurm.out",
        f"#SBATCH --error={log_path}/%J_slurm.err",
        f"#SBATCH --time={timedelta(seconds=timeout)}",
        f"#SBATCH --mem-per-cpu={mem_per_cpu}",
        f"#SBATCH --gres=gpu:{num_gpus}",
        f"#SBATCH --cpus-per-task={cpus_per_task}",
        "",
        f". {conda_path}",
        f"conda activate {env}",
        f"PYTHONUNBUFFERED=1 python3 {script} {' '.join(script_args)}"
        ]
        self.log_tracker = [0,0]

    def submit_slurm_job(self):
        batch_script_file = f'{os.path.dirname(os.path.abspath(__file__))}/{self.dag.dag_display_name}_{self.task_id}.sbatch'
        with open(batch_script_file, 'w') as f:
            for line in self.batch_script_content:
                f.write(line + '\n')

        self.start_time = time.time()
        result = subprocess.run(['sbatch', batch_script_file], capture_output=True, text=True)
        self.job_active = True
        os.remove(batch_script_file)

        if result.returncode != 0:
            self.log.error(f"Failed to submit Slurm job: {result.stderr}")
            raise Exception(f"Slurm job submission failed: {result.stderr}")
        
        self.job_id = result.stdout.strip().split()[-1]
        self.log.info(f"Submitted Slurm job with ID: {self.job_id}")
        self.out_path = f'{self.log_path}/{self.job_id}_slurm.out'
        self.err_path = f'{self.log_path}/{self.job_id}_slurm.err'

    def monitor_slurm_job(self):
        while self.job_active:
            time.sleep(self.poke_interval)

            command = f"squeue -h -j {self.job_id} -o '%T'"
            result = subprocess.run(command, shell=True, capture_output=True, text=True)

            if result.returncode != 0:
                self.log.error(f"Failed to check Slurm job status: {result.stderr}")
                raise Exception(f"Failed to monitor Slurm job status: {result.stderr}")
            
            self.log_slurm()

            job_state = result.stdout.strip()

            if job_state:
                self.log.info(f"Job ID {self.job_id} is currently in state: {job_state}")
            else:
                self.log.info(f"Job ID {self.job_id} is no longer in the queue (likely completed)")
                self.job_active = False
        
    def log_slurm(self):
        if os.path.exists(self.out_path):
            with open(self.out_path, 'r') as f:
                for i, line in enumerate(f):
                    if i >= self.log_tracker[0]:
                        self.log.info(f"Slurm Output: {line.strip()}")
                self.log_tracker[0] = i+1

        if os.path.exists(self.err_path):
            with open(self.err_path, 'r') as f:
                for i, line in enumerate(f):
                    if i >= self.log_tracker[1]:
                        self.log.error(f"Slurm Error: {line.strip()}")
                self.log_tracker[1] = i+1

    def execute(self, context):
        self.submit_slurm_job()
        self.monitor_slurm_job()

        return self.job_id
    
    def on_kill(self):
        if self.job_id:
            self.log.info(f"Cancelling Slurm job with ID: {self.job_id}")
            cancel_command = f"scancel {self.job_id}"
            result = subprocess.run(cancel_command, shell=True, capture_output=True, text=True)
            if result.returncode != 0:
                self.log.error(f"Failed to cancel Slurm job: {result.stderr}")
            else:
                self.log.info(f"Slurm job {self.job_id} canceled successfully.")

