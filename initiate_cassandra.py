from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(['localhost'])
session = cluster.connect()

session.execute("""
        CREATE KEYSPACE IF NOT EXISTS interaction_data
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """)

session.set_keyspace('interaction_data')

create_table_query = """
    CREATE TABLE IF NOT EXISTS interaction_logs (
        student_id TEXT,
        course_code TEXT,
        interaction_type TEXT,
        timestamp TIMESTAMP,
        duration INT,  
        location_country TEXT, 
        location_city TEXT,
        PRIMARY KEY (student_id, timestamp)
        );
"""

session.execute(create_table_query)

session.execute("""
    CREATE TABLE IF NOT EXISTS course_popularity (
    course_code TEXT,
    timestamp TIMESTAMP,
    active_users INT,
    engagement_score INT,
    total_popularity INT,
    PRIMARY KEY (course_code)
);
    """)

session.execute("""
    CREATE TABLE IF NOT EXISTS student_recommendations (
    student_id TEXT,
    course_code TEXT,
    rating FLOAT,
    recommendation_time TIMESTAMP,
    PRIMARY KEY (student_id, course_code, recommendation_time)
    );
""")


cluster.shutdown()

print("Keyspace and tables created successfully!")