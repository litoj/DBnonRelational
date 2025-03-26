#!/usr/bin/env python
# coding: utf-8

# In[1]:

from math import log, sqrt, floor
import time
import random
import phase1

# In[2]:

conn = phase1.conn
cursor = phase1.cursor

# In[3]:


def h2v(cursor, h_table_name, v_table_name, indexing=False):
    cursor.execute(f"DROP TABLE IF EXISTS {v_table_name} CASCADE;")
    cursor.execute(f"DROP VIEW IF EXISTS {v_table_name}_column_type CASCADE;")
    cursor.execute(
        f"""
        CREATE TABLE {v_table_name} (
            oid INTEGER,
            key VARCHAR(50),
            value VARCHAR(50)
        );
    """
    )

    cursor.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{h_table_name}' AND column_name != 'oid';
    """
    )

    columns = cursor.fetchall()

    for [column] in columns:
        cursor.execute(
            f"""
            INSERT INTO {v_table_name} (oid, key, value)
            SELECT oid, '{column}', {column}
            FROM {h_table_name}
            WHERE {column} IS NOT NULL
        """
        )

    # add '_'='' for all rows without a single value
    cursor.execute(
        f"""
        INSERT INTO {v_table_name} (oid, key, value)
        SELECT oid, '_', '' FROM {h_table_name}
        WHERE oid NOT IN (SELECT DISTINCT oid FROM {v_table_name})
    """
    )

    if indexing:
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_oid ON {v_table_name}(oid);"
        )
        cursor.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_key ON {v_table_name}(key);"
        )

    cursor.execute(
        f"""
        CREATE VIEW {v_table_name}_column_type AS
        SELECT column_name AS key, data_type AS value FROM information_schema.columns
        WHERE table_name = '{h_table_name}' AND column_name != 'oid'
    """
    )
    conn.commit()


def v2h(cursor, v_table_name, h_view_name, sacrifice="memory"):
    try:
        cursor.execute(f"DROP VIEW IF EXISTS {h_view_name}")
    except:
        conn.commit()
        cursor.execute(f"DROP TABLE IF EXISTS {h_view_name}")

    cursor.execute(f"SELECT key, value FROM {v_table_name}_column_type")
    columns = cursor.fetchall()
    col_type = {c: d for c, d in columns}
    columns = [(f"a{i}", col_type[f"a{i}"]) for i in range(1, len(columns) + 1)]

    if sacrifice == "memory":  # approach1: create everything at once - memory heavy
        select_statements = ",\n ".join([f"v{c}.value::{d} AS {c}" for c, d in columns])
        join_statement = "\n ".join(
            f"LEFT JOIN {v_table_name} AS v{c} ON b.oid = v{c}.oid AND v{c}.key = '{c}'"
            for c, _ in columns
        )
        cursor.execute(
            f"""
            CREATE VIEW {h_view_name} AS
            SELECT b.oid, {select_statements}
            FROM (SELECT DISTINCT oid FROM {v_table_name}) AS b
            {join_statement}
            ORDER BY b.oid ASC;
        """
        )
    else:  # approach2: cpu and disk intensive - iteratively populate columns
        cursor.execute(
            f"""
            CREATE TABLE {h_view_name} (
                oid SERIAL PRIMARY KEY,
                {",\n".join([f"{key} {value}" for key, value in columns])}
            );
        """
        )
        cursor.execute(
            f"INSERT INTO {h_view_name} (oid) SELECT DISTINCT oid FROM {v_table_name} ORDER BY oid ASC;"
        )
        conn.commit()  # without partial commits the transaction never ends
        for column, data_type in columns:
            cursor.execute(
                f"""
                UPDATE {h_view_name} dst
                SET {column} = v.value
                FROM (SELECT oid, value::{data_type} FROM {v_table_name} WHERE key = '{column}') AS v
                WHERE dst.oid = v.oid;
            """
            )
            conn.commit()

    conn.commit()


def test_identity(cursor, table1, table2):
    # for whatever reason, the order of the original table breaks at 80+
    cursor.execute(f"SELECT * FROM {table1} ORDER BY oid")
    rows1 = cursor.fetchall()
    # update changes order so there is also no other way
    cursor.execute(f"SELECT * FROM {table2} ORDER BY oid")
    rows2 = cursor.fetchall()
    for r1, r2 in zip(rows1, rows2):
        if r1 != r2:
            print(f"Different rows:\n{r1}\n{r2}")
            return


def test_transform_randomized(cursor):
    phase1.generate_randomized("h", value_preview=-1)
    h2v(cursor, "h", "v")
    phase1.print_table(cursor, "v")
    v2h(cursor, "v", "h_view")
    phase1.print_table(cursor, "h_view")


def test_transform(cursor, size=5, sacrifice="memory"):
    num_tuples = size
    num_attributes = size - 1
    sparsity = 0.5
    table_name = "h"
    phase1.create_table(cursor, table_name, num_attributes)

    str_pool = phase1.prepare_string_pool(num_tuples, sparsity, num_attributes)
    for r in range(1, num_tuples + 1):
        values = []
        for i in range(1, num_attributes + 1):
            values.append(phase1.generate_column_value(1 if i < r else 0, i, str_pool))
        insert_query = f"INSERT INTO {table_name} (oid, {', '.join([f'a{i}' for i in range(1, num_attributes + 1)] )}) VALUES ({r}, {', '.join(values)});"
        cursor.execute(insert_query)

    conn.commit()
    # phase1.print_table(cursor, table_name)

    h2v(cursor, table_name, "v")
    # phase1.print_table(cursor, "v_transform")

    v2h(cursor, "v", "h_view", sacrifice)
    # phase1.print_table(cursor, "h_transform")

    test_identity(cursor, table_name, "h_view")


# test_transform(cursor, 100, "cpu")


def bench_table(
    cursor,
    table_name,
    num_tuples,
    num_attributes,
    num_queries=1000,
    oid_test_preference=0.5,
    max_time=5,
):
    start_time = time.perf_counter()
    end_time = time.perf_counter()
    measure_loss = end_time - start_time
    i = 0
    while i < num_queries:
        i += 1
        if random.random() < oid_test_preference:
            cursor.execute(
                f"SELECT * FROM {table_name} WHERE oid = {random.randint(1, num_tuples)}"
            )
        else:
            i = random.randint(1, num_attributes)
            if i % 2 == 0:
                cursor.execute(
                    f"SELECT * FROM {table_name} WHERE a{i} = {random.randint(1, phase1.allowed_integer)}"
                )
            else:
                cursor.execute(
                    f"SELECT * FROM {table_name} WHERE a{i} = '{random.choice(phase1.allowed_strings)}'"
                )
        end_time = time.perf_counter()
        if end_time - start_time > max_time:
            break
    return i / (end_time - start_time - i * measure_loss)


def bench_compare(
    cursor,
    num_tuples,
    sparsity,
    num_attributes,
    indexing=False,
    # partition?
    num_queries=1000,
    oid_test_preference=0.5,
    max_time=5,
):
    phase1.generate_table(cursor, "h", num_tuples, sparsity, num_attributes)
    h2v(cursor, "h", "v", indexing)
    v2h(cursor, "v", "h_view")

    return {
        "h": bench_table(
            cursor,
            "h",
            num_tuples,
            num_attributes,
            num_queries,
            oid_test_preference,
            max_time,
        ),
        "v": bench_table(
            cursor,
            "h_view",
            num_tuples,
            num_attributes,
            num_queries,
            oid_test_preference,
            max_time,
        ),
    }


def normalized_diff(a, b):
    return floor((a - b) * 10000 / (a + b)) / 100


def benchmark(
    t_range=range(10, 16, 2),  # base sqrt(2)
    s_range=range(2, 8, 4),  # base 0.5
    a_range=range(5, 9, 4),
    num_queries=10,
    oid_test_preference=0.5,
    max_time=10,
):
    results = []

    for indexing in False, True:
        for tuple_factor in t_range:
            for sparsity_factor in s_range:
                for num_attributes in a_range:
                    t = floor(2**tuple_factor)
                    s = 1 - 0.5**sparsity_factor

                    result = bench_compare(
                        cursor,
                        t,
                        s,
                        num_attributes,
                        indexing,
                        num_queries,
                        oid_test_preference,
                        max_time,
                    )
                    cursor.execute("SELECT pg_total_relation_size('h')")
                    memory_h = cursor.fetchall()[0][0]
                    cursor.execute("SELECT pg_total_relation_size('v')")
                    memory_v = cursor.fetchall()[0][0]

                    results.append(
                        {
                            "tf": tuple_factor,
                            "t": t,
                            "sf": sparsity_factor,
                            "s": s,
                            "a": num_attributes,
                            "i": indexing,
                            "p_h": floor(result["h"]),
                            "p_v": floor(result["v"]),
                            "m_h": memory_h,
                            "m_v": memory_v,
                            "pdf": normalized_diff(result["h"], result["v"]),
                            "mdf": normalized_diff(memory_h, memory_v),
                        }
                    )
                    print(
                        f"mem_rating_diff: {results[-1]["mdf"]}; perf_rating_diff: {results[-1]["pdf"]}"
                    )

    perf_results = sorted(results, key=lambda x: x["pdf"])
    mem_results = sorted(list(results), key=lambda x: x["mdf"])
    print("Performance Results:")
    for r in perf_results:
        print(r)
    print("Memory Results:")
    for r in mem_results:
        print(r)


phase1.allowed_strings = ["a", "b", "c", "d", "e"]
benchmark(
    # t_range=range(10, 16, 1),
    s_range=range(4, 20, 2),
    a_range=range(5, 11, 4),
    num_queries=10,
    oid_test_preference=0.5,
    max_time=10,
)
