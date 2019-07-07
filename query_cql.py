"""
This module provide all cql statements,
including creating & drop keyspace
creating & drop & insert for specific queries
Since 3 types of queries should be done at once, there should be 3 String arrays for creating/drop/insert CQL
Also, select could be execute at the same time, and should also put into an String array
"""

# FOR KEYSPACE
CREAT_KEYSPACE = ("""
CREATE KEYSPACE IF NOT EXISTS sparkifydb
WITH REPLICATION =
{'class' : 'SimpleStrategy','replication_factor' : 1 }""")

DROP_KEYSPACE = ("""
DROP KEYSPACE IF EXISTS sparkifydb;
""")


# FOR QUERY STATEMENTS
"""
1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
"""

# FIRST DROP IF EXISTS
DROP_TABLE_QUERY1 = "DROP TABLE IF EXISTS songs_by_session"
DROP_TABLE_QUERY2 = "DROP TABLE IF EXISTS song_by_user"
DROP_TABLE_QUERY3 = "DROP TABLE IF EXISTS user_by_song"

# THEN CREATE TABLES
CREATE_TABLE_QUERY1 = """
CREATE TABLE IF NOT EXISTS songs_by_session (
    sessionId INT, 
    itemInSession INT, 
    artist VARCHAR, 
    song VARCHAR, 
    length FLOAT, 
    PRIMARY KEY (sessionId, itemInSession)
    )
"""

"""
2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) 
for userid = 10, sessionid = 182
"""
CREATE_TABLE_QUERY2 = """
CREATE TABLE IF NOT EXISTS song_by_user (
    userId INT,
    sessionId INT,
    artist VARCHAR,
    song VARCHAR,
    itemInSession INT,
    firstName VARCHAR,
    lastName VARCHAR,
    PRIMARY KEY (userid, sessionId, itemInSession)
)
"""

"""
3. Give me every user name (first and last) in my music app history 
who listened to the song 'All Hands Against His Own'"""
CREATE_TABLE_QUERY3 = """
CREATE TABLE IF NOT EXISTS user_by_song(
    userId INT,
    song VARCHAR,
    firstName VARCHAR,
    lastName VARCHAR,
    PRIMARY KEY(userId, song)
)
"""


# INSERT data into tables
INSERT_TABLE_QUERY1 = """
INSERT INTO songs_by_session(
    sessionId, 
    itemInSession, 
    artist, 
    song, 
    length
)
VALUES(
    %(sessionId)s,
    %(itemInSession)s,
    %(artist)s,
    %(song)s,
    %(length)s
)
"""

INSERT_TABLE_QUERY2 = """
INSERT INTO song_by_user(
    userId,
    sessionId,
    artist,
    song,
    itemInSession,
    firstName,
    lastName
)
VALUES(
    %(userId)s,
    %(sessionId)s,
    %(artist)s,
    %(song)s,
    %(itemInSession)s,
    %(firstName)s,
    %(lastName)s
)
"""

INSERT_TABLE_QUERY3 = """
INSERT INTO user_by_song(
    userId,
    firstName,
    lastName,
    song
)
VALUES(
    %(userId)s,
    %(firstName)s,
    %(lastName)s,
    %(song)s
)
"""

# Used for verifying
SELECT_TABLE_QUERY1 = """
SELECT artist, song, length
FROM songs_by_session
WHERE
    sessionId = 338
AND
    itemInSession = 4
"""

SELECT_TABLE_QUERY2 = """
SELECT artist, song, firstName, lastName
FROM song_by_user
WHERE
    userId=10
AND
    sessionId = 182

"""

SELECT_TABLE_QUERY3 = """
SELECT firstName, lastName
FROM user_by_song
WHERE
    song = 'All Hands Against His Own' 
ALLOW FILTERING
"""

# Put all cql statement in array

DROP_TABLES = [DROP_TABLE_QUERY1, DROP_TABLE_QUERY2, DROP_TABLE_QUERY3]
CREATE_TABLES = [CREATE_TABLE_QUERY1, CREATE_TABLE_QUERY2, CREATE_TABLE_QUERY3]
INSERT_TABLES = [INSERT_TABLE_QUERY1, INSERT_TABLE_QUERY2, INSERT_TABLE_QUERY3]
SELECT_TABLES = [SELECT_TABLE_QUERY1, SELECT_TABLE_QUERY2, SELECT_TABLE_QUERY3]