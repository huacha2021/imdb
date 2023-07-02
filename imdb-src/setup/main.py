print("hello")
import requests
import gzip
import oracledb as cx_Oracle
import csv
import os
import shutil
from time import sleep

# Take numbers or nulls from IMDb dataset
def num_or_null(value):
    if value == '\\N':
        return None
    else:
        return float(value.strip('"').strip().strip('\''))

# Sometimes a table might already exist
# Oracle doesn't have a "IF NOT EXISTS" clause
# So we have to drop it first
def create_table(cursor, table_name, query):
    try:
        cursor.execute(query)
    except cx_Oracle.DatabaseError as e:
        print(e)
        cursor.execute(f"DROP TABLE {table_name} CASCADE CONSTRAINTS")
        cursor.execute(query)


# Download IMDb datasets
imdb_url = 'https://datasets.imdbws.com/'
sources = [
    "title.akas.tsv.gz",
    "title.basics.tsv.gz",
    "title.crew.tsv.gz",
    "title.episode.tsv.gz",
    "title.principals.tsv.gz",
    "title.ratings.tsv.gz",
    "name.basics.tsv.gz"
]

# Store files in a cache folder
cache = "cache"
for source in sources:
    print(f"Downloading {source}...")
    if os.path.exists(
        os.path.join("cache", source)
    ):
        print("Already downloaded")
        continue
    response = requests.get(imdb_url + source, stream=True)
    with open(os.path.join("cache", source), "wb") as f:
        f.write(response.content)
    print("Downloaded")


# Setup Oracle DB
dsn = cx_Oracle.makedsn(os.environ.get("URL"), os.environ.get("PORT"), sid=os.environ.get("ORACLE_SID"))
user = os.environ.get("ORACLE_USER") 
password = os.environ.get("ORACLE_PWD")


# Keep waiting for a connection
while True:
    try:
        cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", disable_oob=True)
        break
    except cx_Oracle.DatabaseError as e:
        sleep(10)
        print(e)
        continue

with cx_Oracle.connect(user=user, password=password, dsn=dsn, encoding="UTF-8", disable_oob=True) as connection:
    cursor = connection.cursor()

    # Create tables
    create_table(cursor, "name_basics", """
    CREATE TABLE name_basics (
    nconst VARCHAR2(20) PRIMARY KEY,
    primaryName VARCHAR2(255),
    birthYear NUMBER,
    deathYear NUMBER,
    primaryProfession VARCHAR2(255),
    knownForTitles VARCHAR2(255)
    )""")

    create_table(cursor, "title_basics", """
    CREATE TABLE title_basics (
    tconst VARCHAR2(20) PRIMARY KEY,
    titleType VARCHAR2(255),
    primaryTitle VARCHAR2(255),
    originalTitle VARCHAR2(255),
    isAdult NUMBER(1),
    startYear NUMBER(4),
    endYear NUMBER(4),
    runtimeMinutes NUMBER,
    genres VARCHAR2(255)
    )""")
    
    create_table(cursor, "title_akas", """
        CREATE TABLE title_akas (
        id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
        titleId VARCHAR2(20),
        ordering NUMBER,
        title VARCHAR2(255),
        region VARCHAR2(255),
        language VARCHAR2(255),
        types VARCHAR2(255),
        attributes VARCHAR2(255),
        isOriginalTitle NUMBER(1),
        CONSTRAINT fk_title_akas_tconst FOREIGN KEY (titleId) REFERENCES title_basics (tconst)
    )""")
    
    create_table(cursor, "title_crew", """
    CREATE TABLE title_crew (
    id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
    tconst VARCHAR2(20),
    directors VARCHAR2(255),
    writers VARCHAR2(255),
    CONSTRAINT fk_title_crew_tconst FOREIGN KEY (tconst) REFERENCES title_basics (tconst)
    )""")

    create_table(cursor, "title_episode", """
    CREATE TABLE title_episode (
    id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
    tconst VARCHAR2(20),
    parentTconst VARCHAR2(20),
    seasonNumber NUMBER,
    episodeNumber NUMBER,
    CONSTRAINT fk_title_episode_tconst FOREIGN KEY (tconst) REFERENCES title_basics (tconst) DISABLE NOVALIDATE
    )""")

    create_table(cursor, "title_principals", """
    CREATE TABLE title_principals (
    id NUMBER GENERATED ALWAYS as IDENTITY(START with 1 INCREMENT by 1),
    tconst VARCHAR2(20),
    ordering NUMBER,
    nconst VARCHAR2(20),
    category VARCHAR2(255),
    job VARCHAR2(255),
    characters VARCHAR2(255),
    CONSTRAINT fk_title_principals_tconst FOREIGN KEY (tconst) REFERENCES title_basics (tconst)
    )""")

    create_table(cursor, "title_ratings", """
    CREATE TABLE title_ratings (
    tconst VARCHAR2(20) PRIMARY KEY,
    averageRating NUMBER,
    numVotes NUMBER,
    CONSTRAINT fk_title_ratings_tconst FOREIGN KEY (tconst) REFERENCES title_basics (tconst)
    )""")

    connection.commit()
    db = {}
    # Insert data. Since I couldn't run the whole thing on my laptop, I'm only inserting the first 100 rows
    with gzip.open(os.path.join(cache, "name.basics.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        i = 1
        next(reader)
        for row in reader:
            cursor.execute("""
            INSERT INTO name_basics (nconst, primaryName, birthYear, deathYear, primaryProfession, knownForTitles)
            VALUES (:1, :2, :3, :4, :5, :6)
            """, (row[0], row[1], num_or_null(row[2]), num_or_null(row[3]), row[4], row[5]))
            if not row[0] in db:
                db[row[0]] = [None] * 5 + [i]
            else:
                db[row[0]][5] = i
            i += 1
            if i == 101:
                break
    connection.commit()
    print("name_basics filled")

    with gzip.open(os.path.join(cache, "title.basics.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        i = 1
        next(reader)
        for row in reader:
            cursor.execute("""
            INSERT INTO title_basics (tconst, titleType, primaryTitle, originalTitle, isAdult, startYear, endYear, runtimeMinutes, genres)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8, :9)
            """, (row[0], row[1], row[2], row[3], num_or_null(row[4]), num_or_null(row[5]), num_or_null(row[6]), num_or_null(row[7]), row[8]))
            i += 1
            if i == 101:
                break
    connection.commit()
    print("title_basics filled")

    with gzip.open(os.path.join(cache, "title.akas.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        i = 1
        for row in reader:
            cursor.execute("""
            INSERT INTO title_akas (titleId, ordering, title, region, language, types, attributes, isOriginalTitle)
            VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
            """, (row[0], row[1], row[2], row[3], row[4], row[5], row[6], num_or_null(row[7])))
            if not row[0] in db:
                db[row[0]] = [i] + [None] * 5
            else:
                db[row[0]][0] = i
            i += 1
            if i == 101:
                break
    print("title_akas filled")

    with gzip.open(os.path.join(cache, "title.crew.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        i = 1
        for row in reader:
            cursor.execute("""
            INSERT INTO title_crew (tconst, directors, writers)
            VALUES (:1, :2, :3)
            """, (row[0], row[1], row[2]))
            if not row[0] in db:
                db[row[0]] = [None] + [i] + [None] * 5
            else:
                db[row[0]][1] = i
            i += 1
            if i == 101:
                break
    connection.commit()
    print("title_crew filled")


    with gzip.open(os.path.join(cache, "title.episode.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        i = 1
        for row in reader:
            cursor.execute("""
            INSERT INTO title_episode (tconst, parentTconst, seasonNumber, episodeNumber)
            VALUES (:1, :2, :3, :4)
            """, (row[0], row[1], num_or_null(row[2]), num_or_null(row[3])))
            if not row[0] in db:
                db[row[0]] = [None] * 2 + [i] + [None] * 3
            else:
                db[row[0]][2] = i
            i += 1
            if  i == 101:
                break
    connection.commit()
    print("title_episode filled")

    with gzip.open(os.path.join(cache, "title.principals.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        i = 1
        next(reader)
        for row in reader:
            cursor.execute("""
            INSERT INTO title_principals (tconst, ordering, nconst, category, job, characters)
            VALUES (:1, :2, :3, :4, :5, :6)
            """, (row[0], num_or_null(row[1]), row[2], row[3], row[4], row[5]))
            if not row[0] in db:
                db[row[0]] = [None] * 3 + [i] + [None] * 2
            else:
                db[row[0]][3] = i
            i += 1
            if i == 101:
                break
    connection.commit()
    print("title_principals filled")

    with gzip.open(os.path.join(cache, "title.ratings.tsv.gz"), "rt", encoding="utf-8") as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        i = 1
        for row in reader:
            cursor.execute("""
            INSERT INTO title_ratings (tconst, averageRating, numVotes)
            VALUES (:1, :2, :3)
            """, (row[0], num_or_null(row[1]), num_or_null(row[2])))
            if not row[0] in db:
                db[row[0]] = [None] * 4 + [i] + [None]
            else:
                db[row[0]][4] = i
            i += 1
            if i == 101:
                break

    connection.commit()
    print("title_ratings filled")

# Clear cache if desired
if os.environ.get("KEEP_CACHE") is None:
    shutil.rmtree("cache")
    os.mkdir("cache")
                 
print("Database created")

# Lock database so it doesn't run again
open(os.path.join(cache, ".locked"), "w").close()
open(os.path.join("/cassandra/cache", ".open"), "w").close()