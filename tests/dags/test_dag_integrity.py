"""Tests the basic integrity of DAGs by loading the DAG files and determining
   if they contain a valid DAG object.

Based on example from https://github.com/danielvdende/data-testing-with-airflow/blob/master/integrity_tests/test_dag_integrity.py
Accompanying blog post: https://medium.com/wbaa/datas-inferno-7-circles-of-data-testing-hell-with-airflow-cef4adff58d8
"""
from os import path

import pytest
from airflow import models as airflow_models
from airflow.utils.dag_processing import list_py_file_paths

DAG_BASE_DIR = path.join(path.dirname(__file__), "..", "..", "dags")
DAG_PATHS = list_py_file_paths(DAG_BASE_DIR, safe_mode=True, include_examples=False)


def _import_file(module_name, module_path):
    import importlib.util

    spec = importlib.util.spec_from_file_location(module_name, str(module_path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    return module


@pytest.mark.parametrize("dag_path", DAG_PATHS)
def test_dag_integrity(dag_path):
    """Import DAG file and check for a valid DAG instance."""

    dag_name = path.basename(dag_path)
    module = _import_file(dag_name, dag_path)

    dag_objects = [var for var in vars(module).values() if isinstance(var, airflow_models.DAG)]
    assert dag_objects

    for dag in dag_objects:
        dag.test_cycle()

