import pandas as pd
from decouple import config
import country_converter
import sqlalchemy
import pymongo

import logging
import traceback
from datetime import datetime

logging.basicConfig(filename='app.log', 
    format='%(asctime)s - %(message)s',
    level=logging.INFO
)

# Disable 'setting with copy' warning
pd.options.mode.chained_assignment = None

primary_keys = {
    "share_events": "insert_id",
    "users": "user_id",
    "organizations": "organization__id",
    "countries": "country_code"
}

username = config("POSTGRES_USERNAME")
password = config("POSTGRES_PASSWORD")
host = config("POSTGRES_HOST")
port = config("POSTGRES_PORT")
dbname = config("POSTGRES_DB_NAME")
engine = sqlalchemy.create_engine(f"postgresql://{username}:{password}@{host}:{port}/{dbname}")

def remove_prefix(df, prefix):
    df.rename(columns=lambda x: x[len(prefix) :] if x.startswith(prefix) else x, inplace=True)
    return df

def prepare_data(events):
    share_events_df = pd.json_normalize(events)

    # Extracting `user_properties` data
    users_df = share_events_df.loc[:, share_events_df.columns.str.startswith("user_properties")]

    # Add user_id to events data because we will need it as a foreign key to the ShareEvents table
    users_df["user_properties_user_id"] = share_events_df["user_id"]

    # Prepare columns from `user_properties`  that will later be dropped from the initial dataset
    user_cols_to_drop = list(users_df.columns)

    # Rename `user_properties` columns to remove prefix
    prefix = "user_properties_"
    users_df = remove_prefix(users_df, prefix)

    # Prepare to create user table - remove duplicate user ids
    users_df.drop_duplicates(subset=["user_id"], inplace=True)

    # Drop `user_properties` from initial dataframe, now that they have been extracted
    share_events_df.drop(columns=user_cols_to_drop, inplace=True, errors="ignore")
    share_events_df.drop(columns="_id", inplace=True)
    share_events_df.drop_duplicates(subset=["insert_id"], inplace=True)

    # Extract `organization` data
    organizations_df = users_df[["organization__id", "organization_name", "organization_type"]]
    organizations_df.dropna(subset=["organization__id"], inplace=True)
    organizations_df.drop_duplicates(subset=["organization__id"], inplace=True)
    org_cols_to_drop = ["organization_name", "organization_type", "organization___v",
        "organization_status", "organization_logo_url_url", "organization_owner_id",
        "organization_updated_at", "organization_code", "organization_created_at"]
    users_df.drop(columns=org_cols_to_drop, inplace=True)

    # Extract `location` data
    share_events_df["country_code"] = country_converter.convert(names=list(share_events_df["country"]), to="ISO3")
    countries_df = share_events_df[["country", "country_code"]]
    share_events_df.drop(columns="country", inplace=True)
    countries_df.drop_duplicates(subset=["country_code"], inplace=True)

    ### Cleaning up inconsistent share_events data
    # Add a name to event types that have no name and appear as links ("http...")
    share_events_df.loc[
        share_events_df["event_type"].str.startswith("http"), "event_type"
    ] = "Share Show Room:Studio"

    # rename Share Presentation:ContentAdmin to Share Presentation:Content Admin
    share_events_df.event_type.replace(
        "Share Presentation:ContentAdmin",
        "Share Presentation:Content Admin",
        inplace=True,
    )

    return [
        {"share_events": share_events_df}, 
        {"users": users_df},
        {"organizations": organizations_df},
        {"countries": countries_df}
    ]

def read_mongo_data(collection_name):
    user = config("MONGO_USER")
    password = config("MONGO_PASSWORD")

    try:
        client = pymongo.MongoClient(f"mongodb+srv://{user}:{password}@cluster0.yhpvw.mongodb.net/masterwizr-data-db?retryWrites=true&w=majority")
        db = client["masterwizr-data-db"]
        logging.info("Connected to Mongo")
        collection = db[collection_name]
        yesterday = (datetime.timestamp(datetime.now()) * 1000) - 86400000
        events = collection.find({"event_time": {"$gt": yesterday}})
    except Exception as e:
        logging.error(traceback.print_exc())
        return {"statusCode": 500, "body": {"message": "Error connecting to MongoDB"}}

    return events

def sql_insert(df, table_name):
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
    except sqlalchemy.exc.IntegrityError:
        logging.info(f"Duplicate key on {table_name} table")
    except Exception as e:
        logging.error(traceback.print_exc())
        return {"statusCode": 500, "body": {"message": "Error inserting into Postgres DB"}}

def migrate_data(environment):
    data = read_mongo_data(environment)
    data_dicts = prepare_data(data)
    for data in data_dicts:
        data_key = list(data.keys())[0]
        sql_insert(data[data_key], f"{environment}_{data_key}")


if __name__ == "__main__":
    migrate_data("production")
    # migrate_data("staging")
    # migrate_data("beta")
