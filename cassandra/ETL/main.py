from cassandra.cluster import Cluster
import oracledb as cx_Oracle
import os
from time import sleep

# Cassandra setup pt1
cluster = Cluster(os.environ.get("NODES").split(","))

# Oracle DB setup
dsn = cx_Oracle.makedsn(os.environ.get("URL"), os.environ.get("PORT"), sid=os.environ.get("ORACLE_SID"))
user = os.environ.get("ORACLE_USER") 
password = os.environ.get("ORACLE_PWD")

while True:
    try:
        # Once connected succesfully in both, go forward. Otherwise, keep trying.
        open("cache/.open", "r").close()
        cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", disable_oob=True)
        session = cluster.connect()
        break
    except Exception as e:
        print(e)
        sleep(10)
        continue

# Hacky function because apparently some values weren't being parsed correctly 
def to_num_or_null(value, is_float=False):
    if value == '\\N' or value == None:
        return None
    elif type(value) is float or type(value) is int:
        return value
    elif is_float:
        return float(value.strip('"').strip().strip('\''))
    else:
        return int(value.strip('"').strip().strip('\''))

with cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", disable_oob=True) as connection:
    cursor = connection.cursor()
    # Connect all data by their tconst (unique identifier)
    get_all_info = "SELECT * FROM title_basics full join \
        title_crew on title_basics.tconst = title_crew.tconst \
        full join title_episode on title_basics.tconst = title_episode.tconst \
        full join title_principals on title_basics.tconst = title_principals.tconst \
        full join title_ratings on title_basics.tconst = title_ratings.tconst"
    cursor.execute(get_all_info)
    all_info = cursor.fetchall()

    # Create Cassandra's "tables"
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS imdb
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
        """)
    session.set_keyspace('imdb')
    # Commented values are not being stored
    session.execute("CREATE TABLE IF NOT EXISTS imdb ( \
            tconst text PRIMARY KEY,\
            titleType text,\
            primaryTitle text,\
            originalTitle text,\
            isAdult int,\
            startYear int,\
            endYear int,\
            runtimeMinutes int,\
            genres text, " + \
            # id
            # tconst
            "directors text,\
            writers text," + \
            # id
            # tconst
            # parentTconst
            "seasonNumber int," + \
            # seasonNumber
            "numberOfEpisodes int," + \
            # episodeNumber
            # id
            # tconst
            # ordering
            # nconst
            # category
            # job
            # characters
            # tconst
            "averageRating float,\
            numVotes int \
    )")

    prepare_insert = session.prepare("""
        INSERT INTO imdb (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres, directors, writers, averageRating, numVotes)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    prepare_insert_episode = session.prepare("""
        UPDATE imdb SET seasonNumber = ?, numberOfEpisodes = ? WHERE tconst = ?
    """)

    number_of_seasons = {}
    number_of_episodes = {}

    for row in all_info:
        # Since we are using full join, we can check if most of the values are the NULL
        # If so, we can this is an episode from a series. Since the series have their
        # own row as well, we can save this row for later.
        if len(set(row))*2 > len(row):
            r = [None if v == '\\\\N' else v for v in row]
            print(r)
            session.execute(prepare_insert, (r[0], r[1], r[2], r[3], to_num_or_null(r[4]), to_num_or_null(r[5]), to_num_or_null(r[6]), to_num_or_null(r[7]), r[8], r[10], r[11], to_num_or_null(r[26], True), to_num_or_null(r[27])))
        else:
            # Here we work on the data so that instead of storing all the episodes individually,
            # we store the biggest number of seasons and episodes for each series. (I assume that episodes don't reset each season)
            
            # Check if it is present on the dictionary, if not, add it. If it is, check if the number of seasons is bigger than the one stored.
            if row[15] and row[15] not in number_of_seasons and row[16] != '\\\\N' and row[16] != None:
                number_of_seasons[row[15]] = row[16]
            else:
                if row[16] and row[16] > number_of_seasons[row[15]] and row[16] != '\\\\N' and row[16] != None:
                    number_of_seasons[row[15]] = row[16]

            # Same as above, but for the number of episodes.
            if row[15] and row[15] not in number_of_episodes and row[17] != '\\\\N' and row[17] != None:
                number_of_episodes[row[15]] = row[17] 
            else:
                if row[17] and row[17] > number_of_episodes[row[15]] and row[17] != '\\\\N' and row[17] != None:
                    number_of_episodes[row[15]] = row[17]

    # Append everything
    for key, value in number_of_seasons.items():
        session.execute(prepare_insert_episode, (int(value), int(number_of_episodes[key]), key))


session.shutdown()

# Lock database so it doesn't run again
open("cache/.locked", "w").close()
