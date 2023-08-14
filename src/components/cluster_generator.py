import pandas as pd
import os
import sys
from src.exception import CustomException
from src.logger import logging
from dataclasses import dataclass

from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering


@dataclass
class ModelTrainerConfig:
    predictions: str = os.path.join('artifacts', "clusters.csv")


class ClusterGenerator:
    
    def __init__(self):
        self.clustering_config = ModelTrainerConfig()

    def generate_clusters(self, pca_data, prep_data):
        
        try:

           PCA_ds = pd.read_csv(pca_data)
           df = pd.read_csv(prep_data)
        
            #Initiating the Agglomerative Clustering model. 4 is the obtimal number of clusters for this data
           AC = AgglomerativeClustering(n_clusters=4)
        
           # fit model and predict clusters
           yhat_AC = AC.fit_predict(PCA_ds)
        

           PCA_ds["Clusters"] = yhat_AC
        
           #Adding the Clusters feature to the orignal dataframe.
           df["Clusters"]= yhat_AC

           logging.info("Clustering generation finished")

           os.makedirs(os.path.dirname(self.clustering_config.predictions), exist_ok= True)
           df.to_csv(self.clustering_config.predictions, index = False, header= True)

           return df
        
        except Exception as e:
            raise CustomException(e, sys)
