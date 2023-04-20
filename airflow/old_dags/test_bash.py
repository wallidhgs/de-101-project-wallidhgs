from airflow import DAG

from airflow.operators.bash import BashOperator
from os import environ

# We need to add this to Docker path
import sys
sys.path.append('/opt/airflow/dags/common_de_project')
from common_de_project import constants

with DAG(dag_id="test_bash_operator", default_args=constants.dag_default_args, schedule_interval=None, tags=["de_project"]) as dag:
    test_cli = BashOperator(
        task_id='test_cli2',
        bash_command="""
            aws --version
        """
    )
    remove_list_files = BashOperator(
        task_id='remove_list_files',
        bash_command="""
            cd /opt/airflow/tmp && \
            [ -e products_list.txt ] && rm -- products_list.txt && \
            [ -e sales_list.txt ] && rm -- sales_list.txt
        """
    )
    rename_folder = BashOperator(
        task_id='rename_folder',
        bash_command="""
            cd /opt/airflow/tmp && \
            mv data processed
        """
    )
    upload_lake_s3 = BashOperator(
        task_id='upload_lake_s3',
        bash_command=f"""
            export AWS_ACCESS_KEY_ID={environ.get('AWS_ACCESS_KEY_ID')} && \
            export AWS_SECRET_ACCESS_KEY={environ.get('AWS_SECRET_ACCESS_KEY')} && \
            cd /opt/airflow/tmp && \
            aws s3 sync . s3://de-project-local
        """
    )
    remove_lake_files = BashOperator(
        task_id='remove_lake_files',
        bash_command="""
            cd /opt/airflow/tmp && \
            [ -e processed ] && rm -rf -- processed
        """
    )

    test_cli >> [remove_list_files, rename_folder] >> upload_lake_s3 >> remove_lake_files
