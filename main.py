from networksecurity.components.data_ingestion import DataIngestion 
from networksecurity.components.data_validation import DataValidaton 
from networksecurity.entity.config_entity import DataIngestionConfig,DataValidationConfig
from networksecurity.entity.config_entity import TrainingPipelineConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact,DataValidationArtifact
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
import sys

if __name__=='__main__':
    try:
        trainingpipelineconfig=TrainingPipelineConfig()
        dataingestionconfig=DataIngestionConfig(trainingpipelineconfig)
        dataingestion=DataIngestion(dataingestionconfig)
        logging.info("Initiate the data ingestion")
        dataingestionartifact=dataingestion.initiate_data_ingestion()
        print(dataingestionartifact)
        logging.info("Data Initiation Completed")
        datavalidationconfig=DataValidationConfig(trainingpipelineconfig)
        data_validation=DataValidaton(dataingestionartifact,datavalidationconfig)
        logging.info("Initiate the Data Validation")
        data_validation_artifact=data_validation.initiate_data_validation()
        logging.info("Data Validation Completed")
        print(data_validation_artifact)
      

    except Exception as e:
        raise NetworkSecurityException(e,sys)
