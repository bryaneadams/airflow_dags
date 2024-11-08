"""
### ETL DAG Tutorial Documentation
This ETL DAG is designed to read a file from the `///` directory.
"""

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

default_args = {
    "owner": "airflow",
    "schedule_interval": None,
    "start_date": days_ago(1),
}


@dag(description="DAG used for testing", tags=["example"], default_args=default_args)
def local_read_write_dag():

    @task(task_id="test_pull", templates_dict=default_args)
    def _print_hello():

        print("Hello World!!!")

        test_phrase = "test phrase"

        return test_phrase

    @task(task_id="print_test_phrase", templates_dict=default_args)
    def _print_test_phrase(test_phrase):

        print(test_phrase)

    # Invoke tasks and set dependencies
    test_phrase = _print_hello()
    _print_test_phrase(test_phrase)


dag = local_read_write_dag()

dag.doc_md = __doc__

# All text in dag.doc_md will appear at top of DAG in AirFlow.
# You are able to use markdown
dag.doc_md = """
### Example DAG for a simple pandas implementation

This DAG is an example for reading in writing from a local directory.
It is designed to show a quick and easy implementation for use in learning 
our commmon DAG design. This design uses pandas as we believe it will be
easier for others to learn.
"""
