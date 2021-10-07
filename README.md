# MongoDB to PostgreSQL data migration

Python scripts for migrating user events data from `MongoDB` to `PostgreSQL`

### Scripts:

The data migration is divided into two scripts:
* `first_time_data_migration` - for the first time creation, set up and insertion of data into the PostgreSQL database
* `daily_data_migration` - for recurring (daily) data migration into a PostgreSQL database that has alreayd been set up using the above script

### Process of data migration:
* The data is fetched from `MongoDB` as user events. These user events are divided into three MongoDB collections (`production`, `staging` and `beta`). The events are currently fetched only from the `production` collection but this could be subject to change in the future
* The fetched data is then put into a `Pandas` dataframe to facilitate its cleaning and transformation
* The cleaning and transformation involves fixing things like column names and event type names, then breaking down the dataframe into four different dataframes (`share_events`, `users`, `organizations` and `countries`), which represent the four tables we want to have in the PostgreSQL database
* The data is then inserted into the PostgreSQL database in the four above mentioned tables. In the case of the `first_time_data_migration` script, the data inserted then primary and foreign keys are added. In the other case, the data is simply inserted

### Improvements:
* Modularize the scripts (extract common methods between the two scripts and call them from a separate file) - `in progress`
* Possibly implement using command line arguments to choose which of the two scripts to run
* Possibly improve the logic for fetching data for the current day (for now it's "date greater than yesterday")

### To do:
* Deploy this script (as part of an `Airflow` instance or an `AWS Lambda` function). This requires a deployed `Airflow` instance and a cloud hosted `PostgreSQL` database

## How to set up this project locally
* Clone this repository: `git clone https://github.com/NLSanyu/mongo_to_postgres.git`
* Change directory into the root folder of this project: `cd mongo_to_postgres`
* Create and activate a virtual environment: `python3 -m virtualenv my_venv`, then `source my_venv/bin/activate`
* Install the required dependencies from the requirements file: `pip freeze > requirements.txt`
* Create a PostgreSQL database called `mw_data` on your local machine (using `psql` or any other database tool)
* Run the first time data migration script: `python first_time_data_migration.py`
* Run the daily data migration script when needed `python daily_data_migration.py` (this script should ideally be run using a job/workflow scheduler)
* Check the `mw_data` PostgreSQL database for the updated data
