import pandas as pd
from decouple import config
import pymongo

import logging
import traceback

from sqlalchemy import create_engine

username = config("POSTGRES_USERNAME")
password = config("POSTGRES_PASSWORD")
host = config("POSTGRES_HOST")
port = config("POSTGRES_PORT")
dbname = config("POSTGRES_DB_NAME")
engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')


def remove_prefix(df, prefix):
    df.rename(
        columns=lambda x: x[len(prefix) :] if x.startswith(prefix) else x, inplace=True
    )
    return df


def prepare_data(events):
    share_events_df = pd.json_normalize(events)

    # Extracting `user_properties` data
    users_df = share_events_df.loc[
        :, share_events_df.columns.str.startswith("user_properties")
    ]
    users_df["user_properties_user_id"] = share_events_df[
        "user_id"
    ]  # adding user_id to events data because we will need it as a foreign key to the ShareEvents table

    # Prepare columns from `user_properties`  that will later be dropped from the initial dataset
    user_cols_to_drop = list(users_df.columns)

    # Rename `user_properties` columns to remove prefix
    prefix = "user_properties_"
    users_df = remove_prefix(users_df, prefix)

    # Prepare to create user table
    users_df.reset_index(drop=True, inplace=True)
    users_df.drop_duplicates(subset=["user_id"], inplace=True)

    # Extract `organization` data
    organizations = users_df[["organization_name", "organization__id"]]

    # Extract `location` data
    # countries_df = share_events_df.group_by("city")
    # print(countries_df)


    # Drop `user_properties` from initial dataframe, now that they have been extracted
    new_cols_to_drop = user_cols_to_drop

    share_events_df.drop(columns=new_cols_to_drop, inplace=True, errors="ignore")
    share_events_df.drop_duplicates(subset=["insert_id"], inplace=True)


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

    users_df.to_csv("users.csv")
    share_events_df.to_csv("share_events.csv")

    return users_df, share_events_df



def read_mongo_data(collection_name):
    user = config("MONGO_USER")
    password = config("MONGO_PASSWORD")

    try:
        client = pymongo.MongoClient(f"mongodb+srv://{user}:{password}@cluster0.yhpvw.mongodb.net/masterwizr-data-db?retryWrites=true&w=majority")
        db = client["masterwizr-data-db"]
        logging.info("Connected to Mongo")
        collection = db[collection_name]
        events = list(collection.find())

    except Exception as e:
        logging.error(traceback.print_exc())
        return {"statusCode": 500, "body": {"message": "Error connecting to MongoDB"}}

    return events


def sql_insert(users_df, share_events_df):
    users_df.to_sql('users', engine) # test insert to local db
    # pass


if __name__ == "__main__":
    prod = read_mongo_data("production")
    users_df, share_events_df = prepare_data(prod)
    # sql_insert(users_df, share_events_df)

    # read_mongo_data("staging")
    # read_mongo_data("beta")