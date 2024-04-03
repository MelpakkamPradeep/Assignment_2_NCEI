# Assignment_2_NCEI

## Description
This repository consists of the codes required for Assignment 2 of the CS5830 course. The given task was to use Apache Beam and Airflow to process weather data from the NCEI Database. This repository consists of 2 pipelines. Each pipeline can be triggered in 2 ways. Using the `pipeline_1.py` and `pipeline_2.py` creates a data pipeline whose parameters are hard-coded. Use `pipeline_1_conf.py` and `pipeline_2_conf.py` if you want to give parameters as configuration files to the `airflow dags trigger` command.<br/><br/>
`pipline_1*` files perform the following functions:
* Fetch the page containing the location-wise datasets for a given/hardcoded year.
* Based on the required number of data files, select the data files randomly from the available
list of files.
* Fetch the individual data files.
* Zip them into an archive.
*  Place the archive at a required location. <br/>
<br/>

`pipline_2*` files perform the following functions:
* Wait for the archive to be available (with a timeout of 5 secs) at the destined location. If
the wait has timed out, stop the pipeline.
* Upon the availability (status=success), check if the file is a valid archive followed by unzip
the contents into individual CSV files.
* Extract the contents of the CSV into a data frame and filter the dataframe based on the
required fields such Windspeed or BulbTemperature, etc. Extract also the Lat/Long values
from the CSV to create a tuple of the form <Lat, Lon, [[ArrayOfHourlyDataOfTheReqFields]]>.
* Setup another PythonOperator over ApacheBeam to compute the monthly averages of the
required fields. The output will be of the form <Lat, Long, [[Avg_11, ..., Avg_1N] .. [Avg_M1, ...,
Avg_MN]]> for N fields and M months.
* Using ‘geopandas’ and ‘geodatasets’ packages, create a visualization where you plot the
required fields (one per field) using heatmaps at different lat/lon positions. Export the plots
to PNG. 
* Using a suitable tool like ‘ffmpeg’ or equivalent create a GIF animation using the 12 months
data in PNG image format 
* Upon successful completion, delete the CSV file from the destined location.

## Installation
1. Clone the Git Repository: `git clone https://github.com/MelpakkamPradeep/Assignment_2_NCEI.git`
2. Navigate to the downloaded repository.
3. Install the dependencies: `pip install requirements.txt `

## Usage and Notes
`pipeline_1.py` (triggers every 4 minutes) can be triggered using <br/>
                    <p align="center">`airflow dags trigger get_ncei_data`</p></br>
`pipeline_2.py` (triggers every 2 minutes) can be triggered using <br/>
                    <p align="center">`airflow dags trigger process_ncei_data`</p></br>
`pipeline_1_conf.py` can be triggered using <br/><br/>
                      `airflow dags trigger --conf '{"year":"1984", "dest_dir":"/home/melpradeep/Desktop/CS5830/Assignment_2/", "num_of_files": 3}' get_ncei_data_v1` <br/><br/>
`pipeline_2_conf.py` can be triggered using <br/><br/>
                      `airflow dags trigger --conf '{"reqloc":"/home/melpradeep/Desktop/CS5830/Assignment_2/", "required_fileds": "<list of required fields>"}' process_ncei_data_v1`




















 
