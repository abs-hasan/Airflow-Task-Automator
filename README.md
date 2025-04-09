# Airflow Task Automator

## What This Project Does
This project automates calculating customer experience scores for partners in the aged care industry. It pulls data, processes it, shares results with stakeholders, and also updates the scores in the CRM.

## How the Process Works
I pull the data from SQL Server, process the data using Python, and then push the data to Google Sheets and the CRM system.

## What I Did
- Wrote Python scripts to handle data processing: filtering, merging, and preparing the results.
- Set up an Airflow DAG to automate the pipeline—it runs once a week without manual work.
- Connected the pipeline to SQL Server, Google Sheets, and Salesforce, ensuring smooth data flow.
- Tested the pipeline thoroughly to ensure reliable updates.

## How It Helps the Business
- **Saves Time**: Before this pipeline, teams spent hours each week manually collecting data and preparing results. Now, it’s all automated.
- **Reduces Errors**: Manual work often had mistakes. This pipeline keeps data clean and accurate.
- **Improves Decisions**: Stakeholders can now view partner performance easily and take timely action to improve client outcomes and retention.

## Tools I Used
- **Python**: For data processing and calculations  
- **Apache Airflow**: To automate and schedule the pipeline  
- **SQL**: To pull data from SQL Server  
- **Salesforce**: To update partner records with scores on a regular basis  
- **Google Sheets**: To share insights with stakeholders  

## Code Files
- [DAG Code](https://github.com/abs-hasan/Airflow-Task-Automator/blob/main/Dag_File.py): Sets up the Airflow schedule and tasks.
- [Operator Code](https://github.com/abs-hasan/Airflow-Task-Automator/blob/main/Score_Calculation.py): Handles the data processing and connections.
