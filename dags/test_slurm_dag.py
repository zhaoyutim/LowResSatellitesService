import pendulum
from airflow import DAG
import sys
from pathlib import Path
root_path = str(Path(__file__).resolve().parents[1]) + "/"
sys.path.insert(0,root_path)

from slurm.operators import SlurmOperator
from utils.config import slurm_config

dag = DAG(
    dag_id="slurm_custom_operator_dag",
    start_date=pendulum.datetime(2024, 5, 1, tz="UTC"),
    schedule=None,
    dagrun_timeout=pendulum.duration(minutes=60),
    catchup=False,
)

test_slurm_task = SlurmOperator(
    task_id="test_slurm_task",
    script_args=["--string1=\"Testing...\"", 
                 "--string2=\"Testing complete.\""],
    script=root_path+"scripts/test_slurm.py",
    conda_path=slurm_config['conda_path'],
    env=slurm_config['env'],
    log_path=slurm_config['log_path'],
    poke_interval=5, 
    timeout=3600,
    dag=dag,
)

test_slurm_task