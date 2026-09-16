from pathlib import Path


def test_dag_imports_src_path_before_package_import():
    dag_source = Path("dags/nairobi_air_quality_dag.py").read_text()

    assert dag_source.index("sys.path.insert") < dag_source.index(
        "from nairobi_air_quality_batch_pipeline.api.openaq_client import OpenAQClient"
    )
