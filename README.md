# lakehouse-practice-project
Implementing theoretical knowledge to end-to-end practical project. 

Overview: 
This is end-to-end Lakhouse project which performs ETL: 
Ingestion => Transformation => Load 

Why: 
This project demonstrates the implementation of an end-to-end Lakehouse architecture using PySpark and Databricks. It focuses on building production-inspired ETL pipelines, implementing the Medallion Architecture (Bronze, Silver, Gold), applying Delta Lake features, and following software engineering best practices such as modular code organization, logging, configuration management, and testing. 

Technologies used: 
VS Code 
Python 
Git/ GitHub 
PySpark 
Databricks 

Architecture: 
Data ingestion (system 1) => Databricks (system 2) => BRONZE => SILVER => GOLD 

What are Bronze/Silver/Gold? 
Bronze layer is for raw data ingestion layer that stores data as it arrives in as it format. 
In Silver layer raw data is cleaned and standardized to create ready to transform data that is consistent, correct, and valid. 
In Gold layer, data is aggregated and highly trannsformed to create business ready data that is ready to consume. 

The goal is to create a layered pipeline, where by every layer data quality is improved. 
