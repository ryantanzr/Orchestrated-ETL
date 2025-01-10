import pytest

from airflow.models import DagBag

@pytest.fixture()
def dagbag():
    return DagBag(dag_folder='dags/', include_examples=False)

def test_dag_loaded(dagbag):
    assert len(dagbag.dags) == 1, "Incorrect number of dags"
    assert 'fetch_and_store_amazon_books_etl' in dagbag.dags, "Dag not found"
    assert len(dagbag.import_errors) == 0, "There are errors in the DAG"

def test_tasks_loaded(dagbag):
    dag = dagbag.get_dag('fetch_and_store_amazon_books_etl')
    assert len(dag.tasks) == 5, "Incorrect number of tasks"
    task_ids = list(map(lambda task: task.task_id, dag.tasks))
    assert 'create_sql_tables' in task_ids, "Task not found"
    assert 'extract_book_data' in task_ids, "Task not found"
    assert 'book_data_transformations.standardise_book_data' in task_ids, "Task not found"
    assert 'book_data_transformations.enrich_book_data' in task_ids, "Task not found"
    assert 'load_book_data' in task_ids, "Task not found"
