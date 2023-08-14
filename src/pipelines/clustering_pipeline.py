import pandas as pd
from datetime import datetime
from prefect import flow, task
from src.utils import connect_to_bigquery
from google.cloud import bigquery
import logging

from src.components.loader import DataIngestion
from src.components.preprocessor import DataTransformation, DataTransformationConfig
from src.components.cluster_generator import ClusterGenerator


@task
def extract_data():

    try:
        loader = DataIngestion()

        logging.info("Loading data.") 
        raw_data = loader.initiate_data_ingestion()
        return raw_data

    except Exception as e:
        logging.error(e)
        raise e

@task
def transform_data(raw_data):

    try:    
        preprocessor = DataTransformation()

        logging.info("Preprocessing clustering data.") 
        prep_data, pca_data = preprocessor.initiate_data_transformation(raw_data)

        return prep_data, pca_data
    
    except Exception as e:
        logging.error(e)
        raise e


@task
def clustering_data(prep_data, pca_data):
    try:
        cluster = ClusterGenerator()
        
        logging.info("Started cluster generation.") 
        cluster_data = cluster.generate_clusters(pca_data, prep_data)

        return cluster_data

    except Exception as e:
        logging.error(e)
        raise e



@task
def load_data():



@flow(name="Customer Clustering pipeline")
def generate_clusters():

    try:
       project_id = 'protean-fabric-386717'
       dataset_extract_id = "ml_datasets"
       table_extract_id = "churn_last"
       
       raw_data = extract_data(project_id, dataset_extract_id, table_extract_id)

       prep_data, pca_data = transform_data(raw_data)

       clusters = clustering_data(prep_data, pca_data)

       table_export_id= 'churn_predictions_results'
       dataset_export_id = 'ml_datasets'
       load_completed = load_data(clusters, table_export_id, dataset_export_id)
       
       return load_completed
    
    except Exception as e:
        logging.error(e)
        raise e



if __name__ == '__main__':
    generate_clusters()
