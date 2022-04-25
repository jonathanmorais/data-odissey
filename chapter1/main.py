import great_expectations as ge
import boto3
from moto import mock_s3

def context_expectations():

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
    batch = context_expectations()

    batch.expect_column_values_to_be_unique('type')
    batch.expect_column_values_to_not_be_null("region")
    batch.expect_column_to_exist("Date")
    batch.expect_column_values_to_match_strftime_format('Date', "%Y-%m-%d")
    batch.get_expectation_suite()

    return batch

@mock_s3
def expec_save():
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='expectations-quality-control')
    batch = expec_expectations()
    return batch.save_expectation_suite("great_expectations/expectations/avocado.json")


def expec_asset():
    batch = expec_expectations()
    return batch.run_validation_operator('newbie_validation_operator', assets_to_validate=[batch])