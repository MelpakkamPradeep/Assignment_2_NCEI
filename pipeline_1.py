# Importing necessary libraries
import random
from datetime import datetime
import os

# Importing Airflow components
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Importing libraries for web scraping
import requests
from bs4 import BeautifulSoup

# Defining the DAG
dag = DAG(
    'get_ncei_data',
    start_date=datetime(2024, 2, 25),
    description="Fetch all location datasets from the NCEI Database, for a given year.",
    max_active_runs=1,
    schedule_interval='*/4 * * * *',
    catchup=False
)

# Setting up directories and URLs
currdir = os.getcwd()    
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"
dest_dir = os.getcwd()
year = "1984"

# Step 1: Fetch the page containing the location-wise datasets
fetch_command = f"curl -o {currdir}/ncei_data_{year}.html {base_url}{year}/"
fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command=fetch_command,
    dag=dag
)

# Step 2: Select data files randomly from the available list
def select_random(year, number_of_files):
    """
    Selects a random sample of data files from the NCEI website.

    Parameters
    ----------
    year : str
        The year for which data files are to be selected.
    number_of_files : int
        The number of files to be selected randomly.

    Returns
    -------
    None
    """
    file_list = []
    os.system(f"mkdir {currdir}/temp/")
    with open(f"{currdir}/ncei_data_{year}.html", "r") as file:
        soup = BeautifulSoup(file, "html.parser")
        links = soup.find_all("a", href=True)
        for link in links:
            if link['href'].endswith('.csv'):
                file_list.append(link['href'])
    selected_files = random.sample(file_list, number_of_files)
    for file_path in selected_files:
        fetch_command = f"curl -o {currdir}/temp/{file_path} {base_url}{year}/{file_path}"
        os.system(fetch_command)

select_random = PythonOperator(
    task_id='select_random',
    python_callable=select_random,
    op_kwargs={'year': year, 'number_of_files': 5},
    dag=dag
)

def zip_files():
    """
    Zips the selected data files into a single archive.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """
    os.system(f"tar -C {currdir}/ -czvf {currdir}/data.tar.gz temp/")

zip_files = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    dag=dag
)

place_archive_and_delete = BashOperator(
    task_id="place_archive",
    bash_command= f"mkdir {dest_dir}/bdl/ && mv {currdir}/data.tar.gz {dest_dir}/bdl/ && rm -r {currdir}/temp/ && rm -r {currdir}/ncei_data_{year}.html",
    dag=dag
)

fetch_data >> select_random >> zip_files >> place_archive_and_delete
