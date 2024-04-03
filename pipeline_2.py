from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
import apache_beam as beam
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import geopandas as gpd
import ast
import os
import seaborn as sns

# Define the directory location and required fields
reqloc = f"{os.getcwd()}/bdl/"  # Location of the data files
required_fields = "['HourlyDryBulbTemperature', 'HourlyPressureChange']"  # List of required fields to process

# Define the DAG configuration
dag = DAG(
    'process_ncei_data',  # Name of the DAG
    start_date=datetime(2024, 2, 25),  # Start date of the DAG
    description="Process all csv files that were pulled from the NCEI database.",  # Description of the DAG
    max_active_runs=1,
    schedule_interval='*/2 * * * *',
    catchup=False
)

def process_csv(input_file, required_fields):
    # Read CSV into Pandas DataFrame
    df = pd.read_csv(input_file)
    
    # Filter required fields + DATE, LATITUDE, LONGITUDE
    df = df[['DATE', 'LATITUDE', 'LONGITUDE'] + required_fields]
    
    # Add MONTH from DATE
    df['MONTH'] = pd.to_datetime(df['DATE']).dt.month
    df = df[['MONTH', 'LATITUDE', 'LONGITUDE'] + required_fields]
    for col in  df.columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    df = df.groupby(['MONTH'], as_index=False).mean()
    # Convert dataframe to list, so that it can be sent to Beam
    yield df.values.tolist()

# Function to process the CSV files using Apache Beam
def process_data_with_beam(input_dir, output_file, required_fields):
    # Convert required_fields from string to list
    required_fields = ast.literal_eval(required_fields)
    
    # Define the Apache Beam pipeline
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Create file list' >> beam.Create([os.path.join(input_dir, file) for file in os.listdir(input_dir) if file.endswith('.csv')])
            | 'Read and process CSV' >> beam.FlatMap(lambda file_path: process_csv(file_path, required_fields))
            | 'Write to text' >> beam.io.WriteToText(f'{output_file}', shard_name_template='')
        )

# Function to process the monthly averages output
def process_monthly_averages_output(line, required_fields):
    # Concatenate the line data into a string
    data = str()
    for x in line:
        data = data + x
    
    # Convert the data string to a list using literal_eval
    monthly_data = ast.literal_eval(data.replace('nan', 'None'))
    
    # Extract required fields from the data and create a GeoDataFrame
    entries = [{'month': monthly_data[index][0],
                'latitude': monthly_data[index][1],
                'longitude': monthly_data[index][2],
                **{field: monthly_data[index][i + 2] for i, field in enumerate(required_fields)}} for index in range(len(monthly_data))]
    gdf = gpd.GeoDataFrame(entries)
    gdf['geometry'] = gpd.points_from_xy(gdf.longitude, gdf.latitude)
    return gdf

# Function to combine GeoDataFrames by month
def combine_by_month(geodfs):
    # Extract DataFrames from GeoDataFrames and concatenate them
    geodfs_list = []
    for i in geodfs:
        geodfs_list.append(i)
    return pd.concat(geodfs_list, ignore_index=True)

# Function to group GeoDataFrames by month
def group_by_month(geodfs):
    # Group GeoDataFrames by month and aggregate data
    grouped_df = geodfs.groupby('month', as_index='False')
    return grouped_df

# Function to plot heatmaps for each required field
def plot_heatmap(grouped_df, required_fields, input_dir):
    # Iterate over each required field
    for field in required_fields:
        # Iterate over each element in the grouped DataFrame
        for element in grouped_df:
            month, data = element
            month = int(month)
            
            # Create a GeoDataFrame from the data
            gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.longitude, data.latitude))
            hue_attribute = data[field]

            # Set up the plot
            fig, ax = plt.subplots(figsize=(15, 10))

            # Plot the data points on a map
            world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
            world.plot(ax=ax, color='lightgrey', edgecolor='black')
            sns.scatterplot(x='longitude', y='latitude', data=data, hue=hue_attribute, palette='RdBu', legend=False, ax=ax)

            # Set plot title and labels
            ax.set_title(f'Heatmap for {field} - Month {month}')
            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')

            # Create a colorbar
            norm = plt.Normalize(hue_attribute.min(), hue_attribute.max())
            sm = plt.cm.ScalarMappable(cmap="RdBu", norm=norm)
            sm.set_array([])
            ax.figure.colorbar(sm, ax=ax, label=f'{field}')

            # Save the plot as a PNG file
            plt.savefig(f'{input_dir}heatmap_{field}_month_{month}.png')
            plt.close()

# Function to create heatmaps with Apache Beam
def create_heatmap_with_beam(input_dir, required_fields):
    # Convert required_fields from string to list
    required_fields = ast.literal_eval(required_fields)
    
    # Define the Apache Beam pipeline
    with beam.Pipeline() as pipeline:
        (
            pipeline
            | 'Read Monthly Average' >> beam.io.ReadFromText(f'{input_dir}monthly_averages_output.txt')
            | 'Create GeoDFs' >> beam.Map(lambda line: process_monthly_averages_output(line, required_fields))
            | 'Combine all GeoDFs' >> beam.CombineGlobally(combine_by_month)
            | 'Group by Month' >> beam.Map(group_by_month)
            | 'Plot Heatmaps' >> beam.Map(lambda geodfs: plot_heatmap(geodfs, required_fields, input_dir))
        )

# Function to combine PNG images into GIF animations
def combine_to_gif(required_fields, reqloc):
    # Convert required_fields from string to list
    required_fields = ast.literal_eval(required_fields)
    
    # Loop over each required field
    for field in required_fields:
        # Construct the shell command to combine PNG images into GIF
        command = f"convert -delay 30 -loop 0 {reqloc}heatmap_{field}_month_*.png {reqloc}{field}.gif"
        os.system(command)
    
    # Move PNG files to a subdirectory and remove unnecessary files
    os.system(f'mkdir {reqloc}pngs/ && mv {reqloc}*.png {reqloc}pngs/ && rm {reqloc}*.csv && rm {reqloc}monthly_averages_output.txt')

# Define DAG tasks

# File sensor task to check for the presence of the input data file
check_for_file = FileSensor(
    task_id="check_for_file",
    filepath=f"{reqloc}data.tar.gz",
    poke_interval=5,
    dag=dag
)

# Bash operator task to check for and unzip the input data archive
check_for_archive_and_unzip = BashOperator(
    task_id='check_for_archive_and_unzip',
    bash_command=f"cd {reqloc} && file data.tar.gz | grep -q 'gzip compressed data' && tar --strip-components 1 -xzf data.tar.gz && rm data.tar.gz",
    dag=dag
)

# Python operator task to process CSV files using Apache Beam
task_process_csv = PythonOperator(
    task_id='process_csv',
    python_callable=process_data_with_beam,
    op_kwargs={'input_dir': f'{reqloc}',
               'output_file': f'{reqloc}monthly_averages_output.txt',
               'required_fields': required_fields},
    dag=dag
)

# Python operator task to create heatmaps with Apache Beam
task_create_heatmap = PythonOperator(
    task_id='create_heatmap',
    python_callable=create_heatmap_with_beam,
    op_kwargs={'input_dir': f'{reqloc}',
               'required_fields': required_fields},
    dag=dag
)

# Python operator task to combine PNG images into GIF animations
combine_to_gif_task = PythonOperator(
    task_id='combine_to_gif',
    python_callable=combine_to_gif,
    op_kwargs={'required_fields': required_fields, 
               'reqloc': reqloc},
    dag=dag
)

# Define task dependencies
check_for_file >> check_for_archive_and_unzip >> task_process_csv >> task_create_heatmap >> combine_to_gif_task

