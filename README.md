ETL Movies Data Pipeline with Airflow
This repository contains an Apache Airflow data pipeline for Extract, Transform, and Load (ETL) operations on movie data. The pipeline leverages web scraping using Selenium to extract current movie data from IMDb's box office chart, integrates it with existing data in a PostgreSQL database, and loads the combined data back into the database. The project is scheduled to run daily, starting from November 10, 2023.

![image](https://github.com/Ataa55/ETL-IMDb-Movies-Pipeline/assets/115408306/eb042d25-d19d-4194-a0dc-88c514f9f6eb)

Pipeline Overview:
Scraping Movies:

Utilizes Selenium with Firefox to scrape movie data from IMDb's box office chart.
Implements a custom function (del_chars) to clean and transform scraped data, handling special characters in votes and gross amounts.
Returns a Pandas DataFrame with cleaned and filtered movie data.
Retrieve Data from Database:

Executes a SQL query to retrieve existing movie data from a PostgreSQL database using the PostgresHook.
Returns a Pandas DataFrame with the retrieved database data.
Integration and Processing:

Merges the scraped and retrieved data using Pandas, creating a comprehensive dataset.
Converts data types and handles any discrepancies between scraped and database data.
Load to Database:

Uses the PostgresHook to insert the integrated and processed data into a new table (load_movies) in the PostgreSQL database.

![image](https://github.com/Ataa55/ETL-IMDb-Movies-Pipeline/assets/115408306/d656544f-b8ae-4550-b5c1-0d81f4dab50a)

Write to CSV:
Exports the integrated and processed data to a CSV file located at /home/oem/Documents/final_Data.csv.
Project Structure:
dag_ETL_Movies.py: Defines the Airflow DAG (ETL_Movies) and tasks for each ETL step.
requirements.txt: Lists the Python dependencies required for the project.
/docs: Contains project documentation or additional information.
/sql: May include SQL scripts for database setup or other queries.
/tests: Can contain unit tests or testing scripts.
Getting Started:
Install dependencies: pip install -r requirements.txt
Configure Airflow with your settings, including database connection details.
Run the Airflow scheduler and web server: airflow scheduler and airflow webserver
Access the Airflow web UI to trigger the DAG and monitor the pipeline execution.
Notes:
The project uses Selenium, so ensure you have the appropriate WebDriver installed.
Modify the database connection details in the DAG file (dag_ETL_Movies.py) according to your setup.
The CSV file is saved to /home/oem/Documents/final_Data.csv. Adjust the path as needed.
Feel free to explore, adapt, and enhance this data pipeline according to your requirements! If you encounter any issues, refer to the documentation or feel free to reach out for assistance.

![Capture](https://github.com/Ataa55/ETL-IMDb-Movies-Pipeline/assets/115408306/58123aaf-d43e-4805-9b21-5803a8b89579)
