## Create graph dataset of open calgary and enmax data
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
import polars as pl
import json

from calgary_dashboard.config.paths import PROJECT_ROOT
from calgary_dashboard.config.settings import get_settings

settings = get_settings()

# Be sure to launch 
URI = settings.neo4j_uri
AUTH = (settings.neo4j_user, settings.neo4j_password)

# Get data catalog
data_catalog_dir = str(PROJECT_ROOT / "ontology" / "processed_data_catalog_20260327.json")
with open(data_catalog_dir, "r") as f:
    data_catalog = json.load(f)

# Create database
database_name = settings.neo4j_database


with GraphDatabase.driver(URI, auth=AUTH) as driver:
    driver.verify_connectivity()
    print("Connection established.")

    try:
        # Create some nodes
        for person in people:
            records, summary, keys = driver.execute_query(
                "MERGE (p:Person {name: $person.name, age: $person.age})",
                person=person,
                database_=database_name,
            )

        # Create some relationships
        for person in people:
            if person.get("friends"):
                records, summary, keys = driver.execute_query("""
                    MATCH (p:Person {name: $person.name})
                    UNWIND $person.friends AS friend_name
                    MATCH (friend:Person {name: friend_name})
                    MERGE (p)-[:KNOWS]->(friend)
                    """, person=person,
                    database_=database_name,
                )

        # Retrieve Alice's friends who are under 40
        records, summary, keys = driver.execute_query("""
            MATCH (p:Person {name: $name})-[:KNOWS]-(friend:Person)
            WHERE friend.age < $age
            RETURN friend
            """, name="Alice", age=40,
            routing_="r",
            database_=database_name,
        )
        # Loop through results and do something with them
        for record in records:
            print(record)
        # Summary information
        print("The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query, records_count=len(records),
            time=summary.result_available_after
        ))

    except Neo4jError as e:
        print(e)
        # further logging/processing