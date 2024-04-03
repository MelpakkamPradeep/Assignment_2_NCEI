There are 4 Python files in this submission:
	pipeline_1_conf.py	:	DAGs for Pipeline 1. Needs to be manually trigerred due to it requiring user input. Configuration is of form: {"year": "1992", "dest_dir": "/home/melpradeep/Desktop/bdl/", "num_of_files": 3}
	pipeline_2_conf.py :	DAGs for Pipeline 1. Needs to be manually trigerred due to it requiring user input. Configuration is of form: {"reqloc": "/home/melpradeep/Desktop/bdl/", "required_fields": "['HourlyDryBulbTemperature', 'HourlyPressureChange']"}
	pipeline_1.py	:	DAGs for Pipeline 1. User Inputs are hardcoded in the file. Triggers every 4 minutes.
	pipeline_2.py	:	DAGs for Pipeline 2. User Inputs are hardcoded in the file. Triggers every 2 minutes.
	
The librairies used can be found in the "requirements.txt" file.
