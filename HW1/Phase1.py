# %%
import psycopg2
import random
from collections import defaultdict


# %%
def print_table(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    print(f"Table {table_name}")
    for row in rows:
        print(row)
    print()


# %%


# a: Verbindung zur Datenbank herstellen
try:
    connection = psycopg2.connect(
        dbname="Sparsity", user="postgres", password="", host="localhost", port="5432"
    )
    cursor = connection.cursor()
    cursor.execute("SELECT version();")
    print(cursor.fetchone())
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

# %%
# Tabellen löschen
cursor.execute(
    """
    DROP TABLE IF EXISTS H_toy CASCADE;
    DROP TABLE IF EXISTS V_toy CASCADE;
    DROP TABLE IF EXISTS V_toy_string CASCADE;
    DROP TABLE IF EXISTS V_toy_int CASCADE;
    DROP TABLE IF EXISTS H CASCADE;
    DROP VIEW IF EXISTS h2v_toy CASCADE;
    DROP VIEW IF EXISTS V_toy_all CASCADE;
    DROP VIEW IF EXISTS H_non_null CASCADE;
    DROP VIEW IF EXISTS H_null_count CASCADE;
"""
)
connection.commit()


# %%

# b: Tabelle H_toy erstellen und befüllen
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS H_toy (
        oid SERIAL PRIMARY KEY,
        a1 VARCHAR(50),
        a2 VARCHAR(50),
        a3 INTEGER
    );
"""
)
cursor.execute(
    """
    INSERT INTO H_toy (a1, a2, a3) VALUES
    ('a', 'b', NULL),
    (NULL, 'c', 2),
    (NULL, NULL, 3),
    (NULL, NULL, NULL);
"""
)
connection.commit()

# Vertikale Darstellung V_toy erstellen
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS V_toy (
        oid SERIAL PRIMARY KEY,
        key VARCHAR(50),
        val VARCHAR(50)
    );
"""
)
cursor.execute(
    """
    INSERT INTO V_toy (oid, key, val) VALUES
    (11, 'a1', 'a'),
    (12, 'a2', 'b'),
    (21, 'a2', 'c'),
    (22, 'a3', '2'),
    (31, 'a3', '3');
"""
)
connection.commit()

print_table(cursor, "H_toy")
print_table(cursor, "V_toy")

# %%

# Sicht h2v_toy erstellen
cursor.execute(
    """
    CREATE VIEW h2v_toy AS
    SELECT 
        base.oid AS oid,
        v1.val AS a1,
        v2.val AS a2,
        v3.val AS a3
    FROM 
        (SELECT DISTINCT oid FROM V_toy) AS base
    LEFT JOIN 
        V_toy AS v1 ON base.oid = v1.oid AND v1.key = 'a1'
    LEFT JOIN 
        V_toy AS v2 ON base.oid = v2.oid AND v2.key = 'a2'
    LEFT JOIN 
        V_toy AS v3 ON base.oid = v3.oid AND v3.key = 'a3';
"""
)
connection.commit()

print_table(cursor, "h2v_toy")

# %%

# Partitionen erstellen
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS V_toy_string (
        oid INTEGER,
        key VARCHAR(50),
        val VARCHAR(50)
    );
"""
)
cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS V_toy_int (
        oid INTEGER,
        key VARCHAR(50),
        val INTEGER
    );
"""
)
connection.commit()

# Daten in Partitionen einfügen
cursor.execute(
    """
    INSERT INTO V_toy_string (oid, key, val) VALUES
    (1, 'a1', 'a'),
    (1, 'a2', 'b'),
    (2, 'a2', 'c');
"""
)
cursor.execute(
    """
    INSERT INTO V_toy_int (oid, key, val) VALUES
    (2, 'a3', 2),
    (3, 'a3', 3);
"""
)
connection.commit()

print_table(cursor, "V_toy_string")
print_table(cursor, "V_toy_int")

# %%

# Sicht V_toy_all erstellen
cursor.execute(
    """
    CREATE VIEW V_toy_all AS
    SELECT oid, key, val::VARCHAR(50) AS val FROM V_toy_string
    UNION ALL
    SELECT oid, key, val::VARCHAR(50) FROM V_toy_int;
"""
)
connection.commit()

print_table(cursor, "V_toy_all")

# %%


# c: Korrigierte Funktion generate()
def generate(num_tuples, sparsity, num_attributes):
    try:
        # Alte Tabelle löschen, falls vorhanden
        cursor.execute("DROP TABLE IF EXISTS H;")

        # Tabelle H erstellen
        columns = ["oid SERIAL PRIMARY KEY"]
        for i in range(1, num_attributes + 1):
            if i % 2 == 0:
                columns.append(f"a{i} INTEGER")
            else:
                columns.append(f"a{i} VARCHAR(50)")
        create_table_query = f"CREATE TABLE H ({', '.join(columns)});"
        cursor.execute(create_table_query)

        # Begrenzung für Attributwerte (maximal 5-mal vorkommen)
        value_counts = defaultdict(int)
        allowed_values = ["a", "b", "c"]

        # Daten einfügen
        for oid in range(1, num_tuples + 1):
            values = []
            for i in range(1, num_attributes + 1):
                if random.random() < sparsity:
                    values.append("NULL")
                else:
                    if i % 2 == 0:
                        values.append(str(random.randint(1, 100)))
                    else:
                        # Begrenzung auf max. 5 Vorkommen pro Wert
                        valid_choices = [
                            v for v in allowed_values if value_counts[v] < 5
                        ]
                        if valid_choices:
                            chosen_value = random.choice(valid_choices)
                            values.append(f"'{chosen_value}'")
                            value_counts[chosen_value] += 1
                        else:
                            values.append("NULL")
            insert_query = f"INSERT INTO H ({', '.join([f'a{i}' for i in range(1, num_attributes + 1)] )}) VALUES ({', '.join(values)});"
            cursor.execute(insert_query)

        connection.commit()
        print(
            f"Tabelle H mit {num_tuples} Tupeln und {num_attributes} Attributen erstellt."
        )
    except (Exception, psycopg2.Error) as error:
        print("Fehler in generate():", error)


# %%


# Beispielaufruf der Funktion generate()
generate(num_tuples=10, sparsity=0.3, num_attributes=5)

# Zusätzliche Sichten zur Prüfung der Korrektheit erstellen
# Vorhandene Sichten löschen, falls sie existieren
cursor.execute("DROP VIEW IF EXISTS H_non_null;")
cursor.execute("DROP VIEW IF EXISTS H_null_count;")
connection.commit()

# Sichten zur Überprüfung der Korrektheit erstellen
cursor.execute(
    """
    CREATE VIEW H_null_count AS
    SELECT COUNT(*) FROM H WHERE a1 IS NULL OR a2 IS NULL OR a3 IS NULL OR a4 IS NULL OR a5 IS NULL;
"""
)
connection.commit()

print_table(cursor, "H")
print_table(cursor, "H_null_count")
