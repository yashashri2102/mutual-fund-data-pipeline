#####Run the command for creating/overwriting template for dataflow
```
python -m mf_data_ingestion_df \
--runner DataflowRunner \
--project <project-id> \
--staging_location gs://dataflow-<project-id>/mf_data_ingestion_df/staging \
--temp_location gs://dataflow-<project-id>/mf_data_ingestion_df/temp \
--template_location gs://dataflow-<project-id>/mf_data_ingestion_df/templates/mf_data_ingestion_df \
--max_num_workers 5 \
--region asia-south1 \
--setup_file=$(pwd)/setup.py
```

#### Running in local
```
python -m mf_data_ingestion_df --project <project-id> --inclusive_start_date <"%d-%m-%Y"> --dataset <dataset-id>
```
