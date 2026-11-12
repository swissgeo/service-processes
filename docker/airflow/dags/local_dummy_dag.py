"""Dummy DAG for local development, only logs the conf it was triggered with.

The dag_id matches the process seeded in the local database.
Add a DAG like this for every process you want to start locally.
"""

from airflow.sdk import dag, get_current_context, task


@dag(dag_id="developers-some_local_dataset_dummy", schedule=None, tags=["local-dev"])
def local_dummy_dag() -> None:
    @task
    def show_conf() -> None:
        print("conf:", get_current_context()["dag_run"].conf)  # noqa: T201

    show_conf()


local_dummy_dag()
