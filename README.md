# portfolio_RSI_email_notification
this airflow data pipeline does the following:
1. retrieve information of a stock, in this case "AAPL", from the yahoo finance api
2. using TA library to add more information such as the RSI, to the stock csv file
3. save the new stock information into HDFS
4. save the new stock information into hive
5. sends an email to the user once all that is done

i have covid right now, i will update the rest when i am feeling better.
