# portfolio_RSI_email_notification

This project deals with collecting real-time stock price data (any stock that is on yahoo finance), processing these stock price after mapping the attributes to the desired columns and then storing in the data for further analysis.

this airflow data pipeline does the following:
1. retrieve information of a stock, in this case "AAPL", from the yahoo finance api
2. using TA library to add more information such as the RSI, to the stock csv file
3. save the new stock information into HDFS
4. save the new stock information into hive
5. sends an email to the user once all that is done
the dag file can be found on portfolio_RSI_email_notification/mnt/airflow/dags/stock_data_pipeline_dennis.py

Tech stack used in this project:
- Apache Airflow
- Hadoop 
- Hive (HQL, similar to SQL)
- Spark will be used in a future project

