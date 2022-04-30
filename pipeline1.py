import profile
import os
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

def data_context():

    context = ge.data_context.DataContext()

    suite = context.create_expectation_suite(
        'check_avocado_data',
        overwrite_existing=True
    )

    batch_kwargs = {
        'path': 'data/avocado.csv',
        'datasource': 'data_dir',
        'data_asset_name': 'avocado',
        'reader_method': 'read_csv',
        'reader_options': {
            'index_col': 0,
        }
    }

    return context.get_batch(batch_kwargs, suite)

## expectations
def expec_expectations(batch):
    batch = data_context()

    batch.expect_column_values_to_be_unique('type')
    batch.expect_column_values_to_not_be_null("region")
    batch.expect_column_to_exist("Date")
    batch.expect_column_values_to_match_strftime_format('Date', "%Y-%m-%d")
    batch.get_expectation_suite()

    batch.save_expectation_suite("great_expectations/expectations/avocado.json")
    batch.run_validation_operator('newbie_validation_operator', assets_to_validate=[batch])

        # Create a tar.gz of the great expectations directory
    with tarfile.open("ge-store.tar.gz", "w:gz") as tar:
        tar.add("/ge-store", arcname=os.path.basename("/ge-store"))

    # Save the zip archive to Minio
    client = Minio(
        "ge-minio:9000",
        access_key="admin",
        secret_key=MINIO,
        secure=False
    )

    client.fput_object(
        "great-expectations", "ge-store.tar.gz", "./ge-store.tar.gz",
    )


ws = WorkflowService(host="https://localhost:2746", verify_ssl=False, token=TOKEN)
w = Workflow("generate-expectations", ws, namespace="argo")

context_task = Task("data-context", data_context, image="lambertsbennett/argo-ge:v1", 
                    output_artifacts = [OutputArtifact(name="ContextData", path="/data/avocado_test.csv")])

ge_task = Task("great-expectations-val", expec_expectations, image="lambertsbennett/argo-ge:v1", 
                    input_artifacts = [InputArtifact(name="ContextData", path="/data/avocado_test.csv", from_task="data-context", artifact_name="FakerData")])

context_task >> ge_task

w.add_tasks(context_task, ge_task)
w.submit()
