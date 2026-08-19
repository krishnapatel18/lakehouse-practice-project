# Architecture flow: 
Source => Bronze => Silver => Gold 

1. How is data ingested? through `ingest_table()` function 
2. How is incremental processing performed? through table write mode and function logic 
3. How are failures handled? by implementing error handling 
4. How are retries handled? using databricks workflow 
5. How is data quality checked? by defining custom UDFs 
*can specify automated rules for pipeline 
6. How are tables orchestrated? using databricks workflow 
7. How are secrets managed? 

## Data model 


## Incremental processing 
1. What is the watermark? modified_date 
2. What is the source of truth? 
3. How is the last successful batch determined?
4. What happens during a retry?
5. What happens if the pipeline fails halfway?