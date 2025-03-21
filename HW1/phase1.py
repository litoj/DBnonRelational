#!/usr/bin/env python
# coding: utf-8

# In[165]:

import psycopg2
import random
import string
import math

# In[166]:


def print_table(cursor, table_name, max_rows=10):
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY oid")
    rows = cursor.fetchall()
    print(f"Table {table_name}")
    for row in rows[:max_rows]:
        print(row)
    print()


# In[167]:

# a: Verbindung zur Datenbank herstellen
try:
    conn = psycopg2.connect(
        dbname="Sparsity", user="postgres", password="", host="localhost", port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print(cursor.fetchone())
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL")
    raise error

# In[168]:
allowed_strings = string.ascii_lowercase
allowed_integer = 2**31


def toy_example():
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
    conn.commit()

    # In[169]:

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
    conn.commit()

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
        (31, 'a3', '3'),
        (41, '_', '');
    """
    )
    conn.commit()

    print_table(cursor, "H_toy")
    print_table(cursor, "V_toy")

    # In[170]:

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
    conn.commit()

    print_table(cursor, "h2v_toy")

    # In[171]:

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
    conn.commit()

    # Daten in Partitionen einfügen
    cursor.execute(
        """
        INSERT INTO V_toy_string (oid, key, val) VALUES
        (1, 'a1', 'a'),
        (1, 'a2', 'b'),
        (2, 'a2', 'c'),
        (4, '_', '');
    """
    )
    cursor.execute(
        """
        INSERT INTO V_toy_int (oid, key, val) VALUES
        (2, 'a3', 2),
        (3, 'a3', 3),
        (4, '_', -1);
    """
    )
    conn.commit()

    print_table(cursor, "V_toy_string")
    print_table(cursor, "V_toy_int")

    # In[172]:

    # Sicht V_toy_all erstellen
    cursor.execute(
        """
        CREATE VIEW V_toy_all AS
        SELECT oid, key, val::VARCHAR(50) AS val FROM V_toy_string
        UNION ALL
        SELECT oid, key, val::VARCHAR(50) FROM V_toy_int;
    """
    )
    conn.commit()

    print_table(cursor, "V_toy_all")


def create_table(cursor, table_name, num_attributes):
    # Alte Tabelle löschen, falls vorhanden
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    # Tabelle H erstellen
    columns = ["oid SERIAL PRIMARY KEY"]
    for i in range(1, num_attributes + 1):
        if i % 2 == 0:
            columns.append(f"a{i} INTEGER")
        else:
            columns.append(f"a{i} VARCHAR(50)")
    create_table_query = f"CREATE TABLE {table_name} ({', '.join(columns)});"
    cursor.execute(create_table_query)


def get_max_column_num(cursor, table_name):
    # cursor.execute(f"""
    #     SELECT column_name AS cn FROM information_schema.columns
    #     WHERE table_name = '{table_name}' AND cn != 'oid'
    #     ORDER BY length(cn::varchar(10)) DESC, cn DESC LIMIT 1;
    # """)
    cursor.execute(
        f"""
        SELECT COUNT(column_name) AS cn FROM information_schema.columns
        WHERE table_name = '{table_name}'
    """
    )
    return cursor.fetchone()[0] - 1  # -1 for oid


def get_column_seq(max_column):
    return [f"a{i}" for i in range(1, max_column + 1)]


# direct approach
def get_columns(cursor, table_name):
    cursor.execute(
        f"""
        SELECT column_name AS cn FROM information_schema.columns
        WHERE table_name = '{table_name}' AND cn != 'oid';
    """
    )
    return [row[0] for row in cursor.fetchall()]


def null_count(cursor, table_name):
    null_count = 0
    for col_num in range(1, get_max_column_num(cursor, table_name) + 1):
        cursor.execute(
            f"SELECT SUM(CASE WHEN a{col_num} IS NULL THEN 1 ELSE 0 END) a{col_num} FROM {table_name};"
        )
        null_count += cursor.fetchone()[0]
    return null_count


# Begrenzung so, dass alle Werte gleichmöglich sind (Halb sind Integers -> /2)
def prepare_string_pool(num_tuples, sparsity, num_attributes):
    max_value_count = num_attributes * (1 - sparsity) * num_tuples / 2
    return {c: max_value_count for c in allowed_strings}


def generate_column_value(sparsity, col_num, str_pool):
    if random.random() < sparsity:
        return "NULL"
    elif col_num % 2 == 0:
        return str(random.randint(1, allowed_integer))
    else:
        chosen_value = random.choice(list(str_pool.keys()))
        str_pool[chosen_value] -= 1
        if str_pool[chosen_value] == 0:
            str_pool.pop(chosen_value)
        return f"'{chosen_value}'"


# c: Korrigierte Funktion generate()
def generate_table(cursor, table_name, num_tuples, sparsity, num_attributes):
    try:
        create_table(cursor, table_name, num_attributes)

        str_pool = prepare_string_pool(num_tuples, sparsity, num_attributes)

        conn.commit()
        # Daten einfügen
        for r in range(1, num_tuples + 1):
            values = []
            for i in range(1, num_attributes + 1):
                values.append(generate_column_value(sparsity, i, str_pool))
            insert_query = f"INSERT INTO {table_name} ({', '.join([f'a{i}' for i in range(1, num_attributes + 1)] )}) VALUES ({', '.join(values)});"
            cursor.execute(insert_query)
            if r % 1024 == 0:
                conn.commit()

        conn.commit()
        print(
            f"Generated '{table_name}': tpl={num_tuples}, spars={sparsity:.6}, attr={num_attributes}"
        )
    except (Exception, psycopg2.Error) as error:
        print("Fehler in generate():", error)


def generate(num_tuples, sparsity, num_attributes):
    generate_table(cursor, "H", num_tuples, sparsity, num_attributes)


def generate_randomized(
    table_name="rnd_h", tuple_limit=10, attr_limit=10, value_preview=10
):
    num_tuples = random.randint(1, tuple_limit)
    sparsity = random.random()
    num_attributes = random.randint(1, attr_limit)
    generate_table(cursor, table_name, num_tuples, sparsity, num_attributes)
    if value_preview != 0:
        print_table(
            cursor,
            table_name,
            max_rows=(
                num_tuples
                if value_preview < 0
                else math.ceil(value_preview / num_attributes)
            ),
        )
    return sparsity


def test_sparsity(cursor, table_name, sparsity):
    nulls = null_count(cursor, table_name)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    num_tuples = cursor.fetchone()[0]
    num_attributes = get_max_column_num(cursor, table_name)
    n = num_tuples * num_attributes
    # primitive 95% deviation check - should use z_0.5 range from statistics instead
    variance = math.sqrt(n * sparsity * (1 - sparsity)) * 2
    ok = abs(nulls - sparsity * n) < variance
    print(
        f"Sparsity {"OK" if ok else f"outside {variance/n:.4} deviation"}: target={sparsity:.4}; real={nulls/n:.4};"
    )
    return ok


def test_generator_randomized(
    size, table_name="rnd_h", tuple_limit=10, attr_limit=10, value_preview=10
):
    ok_count = 0
    for _ in range(size):
        sparsity = generate_randomized(
            table_name, tuple_limit, attr_limit, value_preview
        )
        ok = test_sparsity(cursor, table_name, sparsity)
        if ok:
            ok_count += 1
    print(f"Generations within formidable deviation: {ok_count}/{size}")


# randomized_generator_test(10)
