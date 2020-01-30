import pytest
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class HelloOperator(BaseOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def execute(self, context):
        return "hello"


class HelloContextOperator(BaseOperator):
    template_fields = ("_saveme",)

    @apply_defaults
    def __init__(self, saveme, dest_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saveme = saveme
        self._dest_path = dest_path

    def execute(self, context):
        with open(self._dest_path, "w") as f:
            f.write(self._saveme)


def test_hello_operator():
    task = HelloOperator(task_id="test")  # no DAG required here
    result = task.execute(context={})
    assert result == "hello"


def test_hello_context_operator(test_dag, tmpdir):
    tmppath = tmpdir.join("output.txt")

    # DAG required here for context
    task = HelloContextOperator(
        task_id="templated_test", saveme="my task id is {{ task.task_id }}", dest_path=tmppath, dag=test_dag
    )
    pytest.helpers.run_task(task, test_dag)

    assert tmppath.read().replace("\n", "") == "my task id is templated_test"

