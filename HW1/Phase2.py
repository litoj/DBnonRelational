#!/usr/bin/env python
# coding: utf-8

# In[32]:


import psycopg2
import time
import random


# In[33]:


try:
    conn =  psycopg2.connect(
        dbname="Sparsity",
        user="postgres",
        password="",
        host="localhost",
        port="5432",
    )
    cursor = conn.cursor()
    print("Connected to database")
except Exception as e:
    print("Error connecting to database")
    print(e)


# In[34]:


def h2v(cursor, table_name, v_table_name):
    cursor.execute(f"DROP TABLE IF EXISTS {v_table_name} CASCADE;")
    cursor.execute(f"""
                CREATE TABLE {v_table_name} (
                    oid INTEGER,
                    key VARCHAR(50),
                    value VARCHAR(50)
                );
            """)

    cursor.execute(f"""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = '{table_name}' AND column_name != 'oid';
                """)

    columns = [row[0] for row in cursor.fetchall()]

    for column in columns:
        cursor.execute(f"""
                    INSERT INTO {v_table_name} (oid, key, value)
                    SELECT oid, '{column}', {column}
                    FROM {table_name}
                    WHERE {column} IS NOT NULL
                    ORDER BY oid ASC;
                """)
    conn.commit()



# In[35]:


h2v(cursor, "h", "v")



# In[36]:


def v2h(cursor, v_table_name, h_view_name, stop):
    cursor.execute(f"DROP VIEW IF EXISTS {h_view_name}")

    select_statements = []
    for i in range(1, stop):
        if i % 2 == 0:  # Gerade Spaltennummer -> INTEGER
            select_statements.append(f"CAST(v{i}.value AS INTEGER) AS a{i}")
        else:  # Ungerade Spaltennummer -> VARCHAR
            select_statements.append(f"v{i}.value AS a{i}")

    select_clause = ",\n ".join(select_statements)

    join_statement = "\n ".join(
        f"LEFT JOIN {v_table_name} AS v{i} ON base.oid = v{i}.oid AND v{i}.key = 'a{i}'" 
        for i in range(1, stop))

    cursor.execute(f"""
            CREATE VIEW {h_view_name} AS
            SELECT base.oid, {select_clause}
            FROM (SELECT DISTINCT oid FROM {v_table_name}) AS base
            {join_statement};
        """)
    conn.commit()


# In[37]:


v2h(cursor, "v", "h_transform", 6)


# In[38]:


def benchmark(cursor, h_view_name, num_queries=1000):
    cursor.execute(f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name = '{h_view_name}';")
    attr_anz = cursor.fetchone()[0]-1

    start_time = time.time()
    for _ in range(num_queries):
        query_type = random.choice(["single_oid", "attribute_query"])

        if query_type == "single_oid":
            oid = random.randint(1, 10000)
            cursor.execute(f"SELECT * FROM {h_view_name} WHERE oid = {oid};")
        else:
            attr_num = random.randint(1, attr_anz) 
            if attr_num % 2 == 0:
                attr_val = random.randint(1, 100)
            else:
                attr_val = random.choice(["a", "b", "c"]) 
            cursor.execute(
                f"SELECT oid FROM {h_view_name} WHERE a{attr_num} = '{attr_val}';"
            )

    elapsed_time = time.time() - start_time
    print(f"Executed {num_queries} queries in {elapsed_time:.2f} seconds")


# In[39]:


def get_storage_size(cursor, table_name):
    cursor.execute(f"SELECT pg_total_relation_size('{table_name}');")
    size_bytes = cursor.fetchone()[0]
    size_mb = size_bytes / (1024 * 1024)  # Umrechnung in MB
    print(f"Speicherverbrauch von {table_name}: {size_mb:.2f} MB")
    return size_mb


# In[40]:


benchmark(cursor, "h", 100000)
# Speicherverbrauch für H und V bestimmen
get_storage_size(cursor, "h")
get_storage_size(cursor, "v")


# In[41]:


def optimize_v(cursor, v_table_name):
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_key ON {v_table_name}(key);"
    )
    cursor.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_value ON {v_table_name}(value);"
    )
    conn.commit()
    print(f"Indexe für {v_table_name} erstellt.")


# Indexe auf der vertikalen Tabelle setzen
optimize_v(cursor, "v")

