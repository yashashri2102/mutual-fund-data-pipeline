"""
Job to put incremental data from AMFI to Bigquery
"""

import logging
from datetime import datetime, date as datetime_date, timedelta
import requests
import apache_beam as beam
from apache_beam.io import BigQueryDisposition
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
from apache_beam.options.pipeline_options import PipelineOptions

HEADER = [
    "scheme_code",
    "scheme_name ",
    "isin_growth",
    "isin_reinvest",
    "asset_value",
    "repurchase_price",
    "sale_price",
    "year",
    "month",
    "day",
]
HEADER_RAW = [
    "scheme_code",
    "scheme_name",
    "isin_growth",
    "isin_reinvest",
    "asset_value",
    "repurchase_price",
    "sale_price",
    "date",
]

FUND_TYPES = ["Open Ended Schemes", "Close Ended Schemes", "Interval Fund"]
TABLE_NAME = "NAV_Data"
TABLE_SCHEMA = {
    "fields": [
        {"name": "scheme_code", "type": "INTEGER"},
        {"name": "fund_type", "type": "STRING"},
        {"name": "isin_growth", "type": "STRING"},
        {"name": "isin_reinvest", "type": "STRING"},
        {"name": "scheme_name", "type": "STRING"},
        {"name": "asset_value", "type": "STRING"},
        {"name": "repurchase_price", "type": "STRING"},
        {"name": "sale_price", "type": "STRING"},
        {"name": "year", "type": "INTEGER"},
        {"name": "month", "type": "INTEGER"},
        {"name": "day", "type": "INTEGER"},
        {"name": "ingestion_time", "type": "TIMESTAMP"},
    ]
}
GCS_DIR = "gs://dataflow-harsh-gcp-learning2"


def get_table_configuration(destination):
    """Get partition/clustering config"""
    return {
        "rangePartitioning": {
            "field": "scheme_code",
            "range": {"end": 250000, "interval": 1000, "start": 100000},
        },
        "clustering": {"fields": ["year", "month"]},
    }


def get_value(element):
    """
    Returns pipeline value else return default
    """
    try:
        return element.get()
    except:  # pylint: disable=broad-except
        return element.default_value


def get_api_data(from_date):
    response = requests.get(
        f"https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={from_date}"
    )
    logging.info(from_date)
    return [line.decode("utf-8") for line in response.iter_lines()][1:]


class GetDates(beam.DoFn):
    """Get All the dates to fetch MF data for"""

    def __init__(self):
        pass

    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, some, inclusive_start_date):
        inclusive_start_date = inclusive_start_date.get()
        if inclusive_start_date:
            start_year = datetime.strptime(inclusive_start_date, "%d-%m-%Y")
            current_year = datetime.today()
            delta = current_year - start_year  # returns timedelta

            for i in range(delta.days):
                day = start_year + timedelta(days=i)
                yield day.strftime("%d-%b-%Y")
        else:
            final_date = (datetime_date.today() - timedelta(days=1)).strftime(
                "%d-%b-%Y"
            )
            yield final_date


class ProcessDates(beam.DoFn):
    """Process data /Create JSon out of raw data received"""

    def __init__(self):
        pass

    def to_runner_api_parameter(self, unused_context):
        pass

    def process(self, elements):
        fund_type = None
        for element in elements:
            split_data = element.split(";")
            if element and split_data:
                if len(split_data) == 1:
                    exists_ = list(filter(lambda i: i in element, FUND_TYPES))
                    if exists_:
                        fund_type = element
                else:
                    final_data = dict()
                    for k, v in zip(HEADER_RAW, split_data):
                        if k is "scheme_code":
                            final_data[k] = int(v)
                        elif k is "date":
                            date_object = datetime.strptime(v, "%d-%b-%Y")
                            final_data["year"] = date_object.year
                            final_data["month"] = date_object.month
                            final_data["day"] = date_object.day
                        else:
                            final_data[k] = v
                            final_data["fund_type"] = fund_type
                            final_data["ingestion_time"] = datetime.now()
                    yield final_data


class Ds2bqOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--inclusive_start_date",
            default=None,
            help="Mention the start date of the batch",
        )
        parser.add_value_provider_argument(
            "--dataset",
            default="Mutual_fund_Data",
            help="Mention the dataset to be used for data push",
        )


def run(argv=None):
    options = Ds2bqOptions(flags=argv)
    logging.info(options)
    project_id = options.view_as(
        beam.options.pipeline_options.GoogleCloudOptions
    ).project
    table_ref = bigquery.TableReference(
        projectId=project_id, datasetId=get_value(options.dataset), tableId=TABLE_NAME
    )
    with beam.Pipeline(options=options) as p:
        _ = (
            p
            | "Create empty PCollection" >> beam.Create([None])
            | "Get all the Dates based on start date"
            >> beam.ParDo(GetDates(), options.inclusive_start_date)
            | "Fetch Data from AMFI" >> beam.Map(get_api_data)
            | "Process Data" >> beam.ParDo(ProcessDates())
            | "Write to BigQuery"
            >> BigQueryBatchFileLoads(
                destination=table_ref,
                custom_gcs_temp_location=GCS_DIR,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                additional_bq_parameters=get_table_configuration,
                schema=TABLE_SCHEMA,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
