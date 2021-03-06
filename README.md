# MongoDB to PostgreSQL data migration

This repository contains Python scripts for migrating user events data from `MongoDB` to `PostgreSQL`. This is done to facilitate fetching these events in our custom analytics API.

### Data migration overview:

There are two ways the data migration can be done:
* `First time data migration` - for the first time set up and insertion of data into the PostgreSQL database
* `Daily data migration` - for recurring (daily) data migration into a PostgreSQL database that has already been set up using the above method

### Process of data migration:
* The data is fetched from `MongoDB` as user events. These user events are divided into three MongoDB collections (`production`, `staging` and `beta`). The events are currently fetched only from the `production` collection but this could be subject to change in the future
* The fetched data is then put into a `Pandas` dataframe to facilitate its cleaning and transformation
* The cleaning and transformation involves fixing things like column names and event type names, then breaking down the dataframe into four different dataframes (`share_events`, `users`, `organizations` and `countries`), which represent the four tables we want to have in the PostgreSQL database
* The data is then inserted into the PostgreSQL database in the four above mentioned tables. In the case of `first time data migration`, the data is inserted and then the primary and foreign keys are added. In the case of `daily data migration`, the data is simply inserted into the tables

## How to set up this project locally
* Clone this repository: `git clone https://github.com/Master-Wizr/masterwizr-mongo-to-postgres-lambda`
* Change directory into the root folder of this project: `cd masterwizr-mongo-to-postgres-lambda`
* Create a `.env` file and fill in your enviromnment variables as shown in the `.env.example` file included in this repository
* Create and activate a virtual environment: `python3 -m virtualenv my_venv`, then `source my_venv/bin/activate`
* Install the required dependencies from the requirements file: `pip freeze > requirements.txt`
* Create a PostgreSQL database on your local machine that has the same name as your `POSTGRES_DB_NAME` environment variable in your `.env` file. You can create this database using `psql` or any other Postgres database tool
* Run the first time data migration script: `python data_migration.py --first`
* Run the daily data migration script when needed `python data_migration.py --daily` (this script should ideally be run using a job/workflow scheduler)
* Check the PostgreSQL database for the updated data

### Improvements:
* Modularize the scripts (extract common methods between the two scripts and call them from a separate file) - `DONE`
* Implement using command line arguments to choose which data migration logic to run - `DONE`
* Possibly improve the logic for fetching data for the current day (for now it's "date greater than yesterday") - `TO DO`

### To do:
* Deploy this script as part of an `AWS Lambda` function (or later an `Apache Airflow` instance)