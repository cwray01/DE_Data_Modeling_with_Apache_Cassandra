from cassandra.cluster import Cluster

"""
create a session with the local host
"""
def create_session():
    # cluster = Cluster(['172.16.144.172'])
    # cluster = Cluster(['172.16.144.172'],port=9160)
    # cluster = Cluster(['172.16.144.172'],port=9160)
    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    return cluster, session

"""
do cql query with this module
"""
def insert(cql, data_lines, session):
    for line in data_lines:
        # print(cql)
        # print(line)
        try:
            session.execute(cql, line)
        except:
            pass
            #print(e)

def main():
    print("Creating connection...")
    cluster, session = create_session();
    session.set_keyspace('sparkifydb')
    print("Shut down connection...")

    try:
        rows = session.execute("select * from mubsic_library")
    except Exception as e:
        print(e)
    for row in rows:
        print(row.year, row.artist_name)


    session.shutdown()
    cluster.shutdown()

    print("Done.")

if __name__=="__main__":
    main()