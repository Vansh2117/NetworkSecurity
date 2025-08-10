from datetime import datetime
import os
from networksecurity.constant import train_pipeline

print(train_pipeline.PIPELINE_NAME)
print(train_pipeline.ARTIFACT_DIR)


class TrainingPipelineConfig:
    def __init__(self,timestamp=datetime.now()):
        timestamp=timestamp.strftime("%m_%d_%Y_%H_%M_%S")
        self.pipeline_name=train_pipeline.PIPELINE_NAME
        self.artifact_name=train_pipeline.ARTIFACT_DIR
        self.artifact_dir=os.path.join(self.artifact_name,timestamp)
        self.timestamp: str=timestamp


class DataIngestionConfig:
    def __init__(self,training_pipeline_config:TrainingPipelineConfig):
        self.data_ingestion_dir: str=os.path.join(
            training_pipeline_config.artifact_dir, train_pipeline.DATA_INGESTION_DIR_NAME
        )
        self.feature_Store_file_path: str =os.path.join(
            self.data_ingestion_dir, train_pipeline.DATA_INGESTION_FEATURE_STORE_DIR, train_pipeline.FILE_NAME
        )
        self.training_file_path: str =os.path.join(
            self.data_ingestion_dir, train_pipeline.DATA_INGESTION_INGESTED_DIR, train_pipeline.TRAIN_FILE_NAME 
        )
        self.testing_file_path: str = os.path.join(
            self.data_ingestion_dir, train_pipeline.DATA_INGESTION_INGESTED_DIR,train_pipeline.TEST_FILE_NAME
        )
        self.train_test_split_ratio: float = train_pipeline.DATA_INGESTION_TRAIN_TEST_SPLIT_RATIO
        self.collection_name: str = train_pipeline.DATA_INGESTION_COLLECTION_NAME
        self.database_name: str = train_pipeline.DATA_INGESTION_DATABASE_NAME
        
class DataValidationConfig:
    def __init__(self,traing_pipeline_config:TrainingPipelineConfig):
        self.data_validation_dir:str=os.path.join(traing_pipeline_config.artifact_dir, train_pipeline.DATA_VALIDATION_DIR_NAME)
        self.valid_data_dir: str=os.path.join(self.data_validation_dir, train_pipeline.DATA_VALIDATION_VALID_DIR)
        self.invalid_data_dir:str=os.path.join(self.data_validation_dir, train_pipeline.DATA_VALIDATION_INVALID_DIR)
        self.valid_train_file_path:str=os.path.join(self.valid_data_dir, train_pipeline.TRAIN_FILE_NAME)
        self.valid_test_file_path:str=os.path.join(self.valid_data_dir, train_pipeline.TEST_FILE_NAME)
        self.invalid_train_file_path:str=os.path.join(self.invalid_data_dir,train_pipeline.TRAIN_FILE_NAME)
        self.invalid_test_file_path:str=os.path.join(self.invalid_data_dir,train_pipeline.TEST_FILE_NAME)
        self.drift_report_file_path:str = os.path.join(
            self.data_validation_dir,
            train_pipeline.DATA_VALIDATION_DRIFT_REPORT_DIR,
            train_pipeline.DATA_VALIDATION_DRIFT_REPORT_FILE_NAME
        )