import pandas as pd
import os
import sys
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass
from src.components.preprocessor import DataTransformation, DataTransformationConfig
from src.components.cluster_generator import ClusterGenerator


@dataclass
class DataIngestionConfig:
    raw_data_path: str = os.path.join('artifacts', "raw.csv")

class DataIngestion:


    def __init__(self):
        self.ingestion_config= DataIngestionConfig()


    def initiate_data_ingestion(self):

        logging.info("Entered the data ingestion component")
        
        try:
            df = pd.read_csv('data/customer_segment.csv')

            def format_dtypes(df):
                 pass

            logging.info("Load the dataset as dataframe")

            os.makedirs(os.path.dirname(self.ingestion_config.raw_data_path), exist_ok= True)
            df.to_csv(self.ingestion_config.raw_data_path, index = False, header= True)

            logging.info("Ingestion is completed")

            return(
                self.ingestion_config.raw_data_path
            )

        except Exception as e:
            raise CustomException(e, sys)
        
  # test local component
if __name__=="__main__":
    loader = DataIngestion()
    raw_data = loader.initiate_data_ingestion()

    preprocessor = DataTransformation()
    prep_data, pca_data = preprocessor.initiate_data_transformation(raw_data)

    cluster = ClusterGenerator()
    cluster_data = cluster.generate_clusters(pca_data, prep_data)

