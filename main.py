from thyroid.configuration.mongodb_connection import MongoDBClient
from thyroid.exception import ThyroidException
import os, sys
from thyroid.logger import logging
from thyroid.pipeline import training_pipeline
from thyroid.pipeline.training_pipeline import TrainPipeline 


if __name__ == '__main__':
    training_pipeline = TrainPipeline()
    training_pipeline.run_pipeline()
