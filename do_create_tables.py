"""
This module provides methods to drop and crete tables for data etl, which should be run before etl.py
"""
from db_executor import create_session
import query_cql

def create_db():

    #create db and get the connection
    cluster, session = create_session()

    #create database names sparkify
    session(query_cql.DROP_KEYSPACE)
    session(query_cql.CREAT_KEYSPACE)

    #USE sparkify
    session.set_keyspace('sparkifydb')

    return cluster, session


def drop_tbls(session):
    #drop all tables, no exception
    for drop_query in query_cql.DROP_TABLES:
        session.execute(drop_query)

def create_tbls(session):
    #create all tables in keyspace
    for create_query in query_cql.CREATE_TABLES:
        session.execute(create_query)

def main():
    """
    run before etl.py
    1. create db
    2. drop tables
    3. create tables
    :return:
    """
    print("Establishing connection...")
    cluster, session = create_session()
    #create_db()
    session.set_keyspace('sparkifydb')

    print("Dropping old tables")
    drop_tbls(session)

    print("Creating new tables")
    create_tbls(session)

    print("Preparing done!..Closing connection...")
    session.shutdown()
    cluster.shutdown()
    print("All Done.")


if __name__ == "__main__":
    main()