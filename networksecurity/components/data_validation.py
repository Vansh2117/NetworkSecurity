from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.constant.train_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file
from networksecurity.utils.main_utils.utils import write_yaml_file

import os
import sys
import numpy as np
import pandas as pd
from typing import List
from scipy.stats import ks_2samp

class DataValidaton:
    def __init__(self,data_ingestion_artifact:DataIngestionArtifact,
                 data_validation_config:DataValidationConfig):
        
        try:
            self.data_ingestion_artifact=data_ingestion_artifact
            self.data_validation_config=data_validation_config
            self._schema_config = read_yaml_file(SCHEMA_FILE_PATH)

        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    @staticmethod
    def read_data(file_path)->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def validate_number_of_columns(self,dataframe:pd.DataFrame):
        try:
            number_of_columns=len(self._schema_config)
            logging.info(f"Required number of columns: {number_of_columns}")
            logging.info(f"Dataframe has columns: {len(dataframe.columns)}")
            if len(dataframe.columns)==number_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise NetworkSecurityException(e,sys)
        
    def numerical_columns(self,dataframe:pd.DataFrame):
        try:
            expected_numerical_cols=set(self._schema_config["numerical_columns"])
            actual_cols=set(dataframe.columns)
            unexpected_cols=actual_cols-expected_numerical_cols
            # checking if there are some non numeric columns
            if unexpected_cols:
                logging.info(f"Unexpected non numerical columns found: {unexpected_cols}")
                return False
            mising_cols = expected_numerical_cols-actual_cols
            # checking if there are any numeri columns missing
            if mising_cols:
                logging.info(f"Missing numericak columns: {mising_cols}")
                return False
            # checking if all expected columns are numeric
            for col in expected_numerical_cols:
                if not pd.api.types.is_numeric_dtype(dataframe[col]):
                    logging.error(f"Column {col} is not numeric. Found dtype: {dataframe[col].dtype}")
                    return False
            logging.info("All columns math numerical schema and are numeric.")
            return True
        
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def detect_data_drift(self,base_df,current_df,threshold=0.05)->bool:
        try:
            status=True
            report={}
            for column in base_df.columns:
                d1=base_df[column]
                d2=current_df[column]
                is_same_dist=ks_2samp(d1,d2)
                if threshold<=is_same_dist.pvalue:
                    is_found=False
                else:
                    is_found=True
                    status=False
                report.update({column:{
                    "p_value":float(is_same_dist.pvalue),
                    "drift_dtatus":is_found
                }})
            drift_report_file_path=self.data_validation_config.drift_report_file_path
            # Create directory
            dir_path=os.path.dirname(drift_report_file_path)
            os.makedirs(dir_path,exist_ok=True)
            write_yaml_file(file_path=drift_report_file_path,content=report)
             
        except Exception as e:
            raise NetworkSecurityException(e,sys)

    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            train_file_path=self.data_ingestion_artifact.trained_file_path
            test_file_path=self.data_ingestion_artifact.test_file_path

            # Read the data from train and test
            train_dataframe=DataValidaton.read_data(train_file_path)
            test_dataframe=DataValidaton.read_data(test_file_path)

            # Validate number of columns
            status_train=self.validate_number_of_columns(dataframe=train_dataframe)
            if not status_train:
                error_message=f"Train dataframe does not contain all columns. \n"
            status_test=self.validate_number_of_columns(dataframe=test_dataframe)
            if not status_test:
                error_message=f"Test dataframe does not contain all columns. \n"
            
            # Checking the Numeric columns
            status1_train=self.numerical_columns(dataframe=train_dataframe)
            if not status1_train:
                error_message=f"Train database has non numeric columns"
            status1_test=self.numerical_columns(dataframe=test_dataframe)
            if not status1_test:
                error_message=f"Test database has non numeric columns"

            # Checking the data drift
            status2=self.detect_data_drift(base_df=train_dataframe,current_df=test_dataframe)
            dir_path=os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path,exist_ok=True)
            train_dataframe.to_csv(
                self.data_validation_config.valid_train_file_path,index=False,header=True
                )
            test_dataframe.to_csv(
                self.data_validation_config.valid_test_file_path,index=False,header=True
                )
            data_validation_artifact=DataValidationArtifact(
                validation_status=status2,
                valid_train_file_path=self.data_ingestion_artifact.trained_file_path,
                valid_test_file_path=self.data_ingestion_artifact.test_file_path,
                invalid_train_file_path=None,
                invalid_test_file_path=None,
                drift_report_file_path=self.data_validation_config.drift_report_file_path
            )
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)


