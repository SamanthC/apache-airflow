# Purchase-predict DAG with Apache Airflow

This project creates the DAG of the whole purchase_predict project flow:
- loading the latest events on BigQuery
- processing the data thanks to Dataproc Cluster
- training and optimizing the LightGBM model
- deploying it on Mlflow
- getting access of the latest version from the API
- creating the Docker Image
- deploying it on a k8s service
