import profile
import os
import csv
from hera.task import Task
from hera.workflow import Workflow
from hera.workflow_service import WorkflowService
from hera.artifact import InputArtifact, OutputArtifact
import great_expectations as ge
from minio import Minio
import tarfile
import os.path

TOKEN = os.getenv("SECRET")
MINIO = os.getenv("MINIO_SECRET")

## client minio
def get_minio_client():
    client = Minio(
        "ge-minio:9000",
        access_key="admin",
        secret_key=MINIO,
        secure=False
    )
    return client


### tasks ###
## data
def generate_faker_data():
    from faker import Faker
    import numpy as np
    import pandas as pd

    fake = Faker()

    profs = []
    for _ in range(100):
        profs.append(fake.profile())

    feature1 = np.random.rand(100)
    feature2 = np.random.rand(100)

    data = pd.DataFrame(profs)
    data['feature1'] = feature1
    data['feature2'] = feature2

    data.to_csv("chapter1/data/faker_test.csv", index=False)  

## expectations
def great_expectations():
    from great_expectations.data_context.types.base import DataContextConfig, DatasourceConfig, FilesystemStoreBackendDefaults
    from great_expectations.data_context import BaseDataContext
    from great_expectations.core.batch import RuntimeBatchRequest
    from great_expectations.profile.user_configurable_profiler import UserConfigurableProfiler
    

    data_context_config = DataContextConfig(
        datasources={
            "pandas": DatasourceConfig(
                class_name="Datasource",
                execution_engine={
                    "class_name": "PandasExecutionEngine"
                },
                data_connectors={
                    "avocado_data": {
                        "class_name": "ConfiguredAssetFilesystemDataConnector",
                        "base_directory": "/data",
                        "assets": {
                            "avocado_data": {
                                "pattern": r"(.*)",
                                "group_names": ["data_asset"]
                            }
                        },
                    }
                },
            )
        },
        store_backend_defaults=FilesystemStoreBackendDefaults(root_directory="/ge-store"),
    )

    context = BaseDataContext(project_config=data_context_config)

    datasource_config = {
        "name": "avocado_data",
        "class_name": "Datasource",
        "module_name": "great_expectations.datasource",
        "execution_engine": {
            "module_name": "great_expectations.execution_engine",
            "class_name": "PandasExecutionEngine",
        },
        "data_connectors": {
            "default_runtime_data_connector_name": {
                "class_name": "RuntimeDataConnector",
                "module_name": "great_expectations.datasource.data_connector",
                "batch_identifiers": ["default_identifier_name"],
            },
            "default_inferred_data_connector_name": {
                "class_name": "InferredAssetFilesystemDataConnector",
                "base_directory": "chapter1/data/",
                "default_regex": {"group_names": ["data_asset_name"], "pattern": "(.*)"},
            },
        },
    }

    context.add_datasource(**datasource_config)

    context.create_expectation_suite(
        expectation_suite_name="test_suite", overwrite_existing=True
    )

    batch_request = RuntimeBatchRequest(
        datasource_name="avocado_data",
        data_connector_name="default_runtime_data_connector_name",
        data_asset_name="test",  # This can be anything that identifies this data_asset for you
        runtime_parameters={"path": "chapter1/data/"},  # Add your path here.
        batch_identifiers={"default_identifier_name": "default_identifier"},
    )

    validator = context.get_validator(
        batch_request=batch_request, expectation_suite_name="test_suite"
    )

    profiler = UserConfigurableProfiler(
        profile_dataset=validator,
        excluded_expectations=None,
        ignored_columns=None,
        not_null_only=False,
        primary_or_compound_key=False,
        semantic_types_dict=None,
        table_expectations_only=False,
        value_set_threshold="MANY"
    )

    suite = profiler.build_suite()

    validator.save_expectation_suite(discard_failed_expectations=False)

    # Create a tar.gz of the great expectations directory
    with tarfile.open("ge-store.tar.gz", "w:gz") as tar:
        tar.add("/ge-store", arcname=os.path.basename("/ge-store"))

    # Save the zip archive to Minio
    client = get_minio_client()

    client.fput_object(
        "great-expectations", "ge-store.tar.gz", "./ge-store.tar.gz",
    )


ws = WorkflowService(host="https://localhost:2746", verify_ssl=False, token=TOKEN)
w = Workflow("generate-expectations", ws, namespace="argo")

generate_task = Task("faker-context", generate_faker_data, image="lambertsbennett/argo-ge:v1", 
                    output_artifacts = [OutputArtifact(name="FakerData", path="FakerData", path="chapter1/data/faker_test.csv")])

ge_task = Task("great-expectations-val", great_expectations, image="lambertsbennett/argo-ge:v1", 
                    input_artifacts = [InputArtifact(name="FakerData", path="chapter1/data/faker_test.csv", from_task="faker-context", artifact_name="avocadoData")])

generate_task >> ge_task

w.add_tasks(generate_task, ge_task)
w.submit()
