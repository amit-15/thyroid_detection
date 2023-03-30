# import schemafile from contant training_pipeline
 


from thyroid.constant.training_pipeline import SCHEMA_FILE_PATH
from thyroid.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from thyroid.entity.config_entity import DataValidationConfig
from thyroid.exception import ThyroidException
from thyroid.logger import logging
from thyroid.utils.main_utils import read_yaml_file, write_yaml_file
import pandas as pd
import os, sys


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise ThyroidException(e,sys)

    def validate_number_of_columns(self, dataframe: pd.DataFrame)->bool:
        try:
            status = len(dataframe.columns) == len(self._schema_config["columns"])
            logging.info(f"Is required column present:[{status}]")
            return status
                
        except Exception as e:
            raise ThyroidException(e,sys)
        
    def is_column_exits(self, dataframe: pd.DataFrame) -> bool:
        try:
            dataframe_columns = dataframe.columns
            status = True
            missing_numerical_columns = []
            missing_categorical_columns = []
            for column in self._schema_config['numerical_columns']:
                if column not in dataframe_columns:
                    status = False
                    missing_numerical_columns.append(column)
            logging.info(f"Missing numerical column: {missing_numerical_columns}")
            
            for column in self._schema_config['categorical_columns']:
                if column not in dataframe_columns:
                    status = False
                    missing_categorical_columns.append(column)
            logging.info(f"Missing categorical column: {missing_categorical_columns}")
            return status
        
        except Exception as e:
            raise ThyroidException(e,sys)
        
    @staticmethod
    def read_data(file_path) ->pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise ThyroidException(e,sys)    
        
    
    def detect_dataset_drift(self):
        pass
        
    
    def initiate_data_validation(self)->DataValidationArtifact:
        try:
            validation_error_msg = ""
            logging.info("Starting data validation")
            
            # define train & test file paths
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            
            #Reading data from train and test file location
            train_df = DataValidation.read_path(train_file_path)
            test_df = DataValidation.read_path(test_file_path)
            
            #Validate number of columns
            status = self.validate_number_of_columns(dataframe= train_df)
            logging.info(f"All required columns present in training dataframe:{status}")
            if not status:
                validation_error_msg += f"Columns number are missing in training dataframe"
                
            status = self.validate_number_of_columns(dataframe= test_df)
            logging.info(f"All required columns present in testing dataframe:{status}")
            if not status:
                validation_error_msg += f"Columns are missing in test dataframe"               
                      
            #validate numerical & categorical columns
            status = self.is_column_exits(dataframe= train_df)
            if not status:
                validation_error_msg += f"Columns are missing in training dataframe"
                
            status = self.is_column_exits(dataframe= test_df)
            if not status:
                validation_error_msg += f"Columns are missing in test dataframe"
                

                
            
        except Exception as e:
            raise ThyroidException(e, sys)


