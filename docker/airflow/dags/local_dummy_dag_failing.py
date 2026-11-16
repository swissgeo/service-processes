"""Dummy DAG for local development that fails on purpose, to test failed jobs.

The dag_id matches the failing process seeded in the local database.
"""

from airflow.sdk import dag, get_current_context, task


@dag(dag_id="developers-some_local_dataset_dummy_failing", schedule=None, tags=["local-dev"])
def local_dummy_dag_failing() -> None:
    @task
    def show_conf() -> None:
        print("conf:", get_current_context()["dag_run"].conf)  # noqa: T201

    @task
    def successful_task() -> None:
        print("Successful task")  # noqa: T201

    # No retries, so that the DAG run fails right away
    @task(retries=0)
    def failing_task() -> None:
        print("Failing task")  # noqa: T201
        raise RuntimeError("Intentional failure for testing")

    @task
    def unreachable_task() -> None:
        print("Unreachable task (you should not be seeing this)")  # noqa: T201

    show_conf() >> successful_task() >> failing_task() >> unreachable_task()


local_dummy_dag_failing()
