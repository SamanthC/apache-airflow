# apache-airflow

The poject creates the DAG of the whole purchase_predict project flow:
- loading the latest events
- processing the data
- training and optimizing the LightGBM model
- deploying it on mlflow
- getting access of the latest version from the API
- creating the Docker Image
- deploying it on a k8s service
