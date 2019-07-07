"""
This is the last module to be run.
"""

import query_cql
from db_executor import create_session
import pandas as pd

def select_query():
    """
    Do select query as the project asked.
    Put the result into pandas df and print them out
    """
    result = []
    print("Get connection...")
    cluster, session = create_session()
    session.set_keyspace("sparkifydb")
    print("Verifying.....")

    # for cql in query_cql.SELECT_TABLES:
    #     print("-------------")
    #     print(cql)
    #     try:
    #         rows = session.execute(cql)
    #         for row in rows:
    #             print(row)
    #     except Exception as e:
    #         print(e)

    for i,cql in enumerate(query_cql.SELECT_TABLES):
        print("--------------------------------")
        print(cql)
        # result.append(pd.DataFrame(list(session.execute(cql))))
        # print(pd.DataFrame(list(session.execute(cql))).to_string())
        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_rows', None)
        pd.set_option('display.max_colwidth', 100)
        pd.set_option('display.width', 1000)
        df=pd.DataFrame(list(session.execute(cql)))
        print(df)

    print("Closing connection....")
    session.shutdown()
    cluster.shutdown()
    return result


def main():
    result = select_query()
    # print (result)


if __name__ == "__main__":
    main()


