import query_cql
from db_executor import create_session, insert
from utils import read_csv_file
from data_processor import PreparerQuery1, PreparerQuery2, PreparerQuery3

"""
connect cluster and load row data, then create tables and insert data
functions have been defined in pre_query*(), this module is used to execute those functions
"""
def load_data(session, filepath, functions):
    # CREATE/INSERT
    for func in functions:
        func(session, filepath)

def pre_query1(session, filepath):
    # Raw column formater
    data = read_csv_file(filepath)
    preparer = PreparerQuery1()
    valueToInsert = preparer.transform(data)
    insert(query_cql.INSERT_TABLE_QUERY1, valueToInsert, session)


def pre_query2(session, filepath):
    # Raw column formater
    data = read_csv_file(filepath)
    prep = PreparerQuery2()
    valueToInsert = prep.transform(data)
    insert(query_cql.INSERT_TABLE_QUERY2, valueToInsert, session)

def pre_query3(session, filepath):
    # Raw column formater
    data = read_csv_file(filepath)
    prep = PreparerQuery3()
    valueToInsert = prep.transform(data)
    insert(query_cql.INSERT_TABLE_QUERY3, valueToInsert, session)




def main():

    csv_file_path = "./event_data_new.csv"
    # 1st. prepare for each types of query, do data modeling
    functions = [pre_query1, pre_query2, pre_query3]
    # functions = [pre_query3]
    # 2nd. set keyspace
    print("Creating connection...")
    cluster, session = create_session();
    session.set_keyspace('sparkifydb')
    print("Inserting data...")
    load_data(session, csv_file_path, functions)
    print("Shut down connection...")
    session.shutdown()
    cluster.shutdown()

    print("Done.")

if __name__=="__main__":
    main()
