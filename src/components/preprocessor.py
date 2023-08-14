import sys
import os
from dataclasses import dataclass

import pandas as pd
import numpy as np
import datetime
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering
from sklearn import metrics

from src.exception import CustomException
from src.logger import logging


@dataclass
class DataTransformationConfig:
    prep_data_path: str = os.path.join('artifacts', "preprocessed_data.csv")
    pca_data_path: str = os.path.join('artifacts', "pca_data.csv")


class DataTransformation:
    
    def __init__(self):
        self.ingestion_config = DataTransformationConfig()

    def initiate_data_transformation(self, raw_data):

        try:

            df = pd.read_csv(raw_data)

            df = df.dropna()

            df["Dt_Customer"] = pd.to_datetime(df["Dt_Customer"],format="%d-%m-%Y")
 

            df["Age"] = pd.Timestamp.now().year - df["Year_Birth"]

            df['Spent'] = df["MntWines"]+ df["MntFruits"]+ df["MntMeatProducts"]+ df["MntFishProducts"]+ df["MntSweetProducts"]+ df["MntGoldProds"]

            # Simplifying living situation
            df["Living_With"]= df["Marital_Status"].replace(
                  {"Married":"Partner",
                   "Together":"Partner",
                   "Absurd":"Alone", 
                   "Widow":"Alone", 
                   "YOLO":"Alone", 
                   "Divorced":"Alone",
                   "Single":"Alone",}
                   )
            
            # Simplifying numer of childrens
            df["Children"] =df["Kidhome"]+ df["Teenhome"]
            
            #Feature pertaining parenthood
            df["Is_Parent"] = np.where(df.Children> 0, 1, 0)
            
            # Counting total members in the household
            df["Family_Size"] = df["Living_With"].replace({"Alone": 1, "Partner":2})+ df["Children"]
            
            # Segmenting education leves in 3 groups
            df["Education"]= df["Education"].replace(
                  {"Basic":"Undergraduate",
                   "2n Cycle":"Undergraduate",
                   "Graduation":"Graduate",
                   "Master":"Postgraduate",
                   "PhD":"Postgraduate"}
                   )
            
            #rename columns to simplify strings
            df = df .rename(columns={
                  "MntWines": "Wines",
                  "MntFruits":"Fruits",
                  "MntMeatProducts":"Meat",
                  "MntFishProducts":"Fish",
                  "MntSweetProducts":"Sweets",
                  "MntGoldProds":"Gold"})
            
            # Drop non necessary columns 
            to_drop = ["Marital_Status", "Dt_Customer", "Z_CostContact", "Z_Revenue", "Year_Birth", "ID"]
            df = df.drop(to_drop, axis=1)

            #handling some outliers
            df = df[(df["Age"]<90)]
            df = df[(df["Income"]<600000)]
            
            logging.info("Completed data preprocessing")

            # Encoding Categorical Variables
            s = (df.dtypes == 'object')
            object_cols = list(s[s].index)
            LE = LabelEncoder()
            for i in object_cols:
                df[i] = df[[i]].apply(LE.fit_transform)

            #Creating a copy of data
            ds = df.copy()

            cols_del = ['AcceptedCmp3', 'AcceptedCmp4', 'AcceptedCmp5', 'AcceptedCmp1','AcceptedCmp2', 'Complain', 'Response']
            ds = ds.drop(cols_del, axis=1)

            # Scale the dataframe 
            scaler = StandardScaler()
            scaler.fit(ds)
            scaled_ds = pd.DataFrame(scaler.transform(ds),columns= ds.columns )


            pca = PCA(n_components = 3)
            pca.fit(scaled_ds)
            PCA_ds = pd.DataFrame(pca.transform(scaled_ds), columns=(['col1', 'col2', 'col3']))

            logging.info("Load the datasets as dataframe")

            os.makedirs(os.path.dirname(self.ingestion_config.prep_data_path), exist_ok= True)
            df.to_csv(self.ingestion_config.prep_data_path, index = False, header= True)

            os.makedirs(os.path.dirname(self.ingestion_config.pca_data_path), exist_ok= True)
            PCA_ds.to_csv(self.ingestion_config.pca_data_path, index = False, header= True)


            return(
                self.ingestion_config.prep_data_path,
                self.ingestion_config.pca_data_path
            )

        except Exception as e:
            raise CustomException(e, sys)







        