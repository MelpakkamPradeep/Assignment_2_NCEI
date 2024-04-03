import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import requests
from bs4 import BeautifulSoup
import os

# Defining the DAG
dag = DAG(
    'get_ncei_data_conf',
    start_date=datetime(2024, 2, 25),
    description="Fetch all location datasets from the NCEI Database, for a given year.",
    schedule_interval='@daily',
    catchup=False
)

# Retrieving the year and destination directory from Airflow variables
year = "{{ dag_run.conf.year }}"
dest_dir = "{{ dag_run.conf.dest_dir }}"
num_of_files = "{{ dag_run.conf.num_of_files }}"

# Getting the current working directory and the base URL for NCEI data
currdir = os.getcwd()
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/"

# Bash command to fetch the NCEI HTML file for the specified year
fetch_command = f"curl -o {currdir}/ncei_data_{year}.html {base_url}{year}/"
fetch_data = BashOperator(
    task_id='fetch_data',
    bash_command=fetch_command,
    dag=dag
)

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
    os.system(f"mkdir -p {currdir}/temp/")  # Creating directory if it doesn't exist
    html_file_path = f"{currdir}/ncei_data_{year}.html"
    if not os.path.isfile(html_file_path):
        raise FileNotFoundError(f"NCEI HTML file for year {year} not found.")
    with open(html_file_path, "r") as file:
        soup = BeautifulSoup(file, "html.parser")
        links = soup.find_all("a", href=True)
        for link in links:
            if link['href'].endswith('.csv'):
                file_list.append(link['href'])
    if not file_list:
        raise ValueError(f"No CSV files found for year {year}.")
    selected_files = random.sample(file_list, min(int(number_of_files), len(file_list)))
    for file_path in selected_files:
        fetch_command = f"curl -o {currdir}/temp/{file_path} {base_url}{year}/{file_path}"
        os.system(fetch_command)

select_random = PythonOperator(
    task_id='select_random',
    python_callable=select_random,
    op_kwargs={'year': year, 'number_of_files': num_of_files},
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
    if not os.path.exists(f"{currdir}/temp/"):
        raise FileNotFoundError("Temporary directory 'temp' does not exist.")
    os.system(f"tar -C {currdir}/ -czvf {currdir}/data.tar.gz temp/")

zip_files = PythonOperator(
    task_id='zip_files',
    python_callable=zip_files,
    dag=dag
)

place_archive_and_delete = BashOperator(
    task_id="place_archive",
    bash_command=f"mv {currdir}/data.tar.gz {dest_dir} && rm -r {currdir}/temp/ && rm -r {currdir}/ncei_data_{year}.html",
    dag=dag
)

# Setting the task dependencies
fetch_data >> select_random >> zip_files >> place_archive_and_delete

