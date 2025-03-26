#!/usr/bin/env python
# coding: utf-8

# In[1]:

from math import log, sqrt, floor
import time
import random
import phase1
import json

# In[2]:

conn = phase1.conn
cursor = phase1.cursor

# In[3]:


def h2v(cursor, h_table_name, v_table_name, indexing=False):
    for suffix in "str", "int", "null", "col":
        cursor.execute(f"DROP TABLE IF EXISTS {v_table_name}_{suffix} CASCADE;")
    cursor.execute(
        f"""
        CREATE TABLE {v_table_name}_str (oid INTEGER, key VARCHAR(50), value VARCHAR(50));
        CREATE TABLE {v_table_name}_int (oid INTEGER, key VARCHAR(50), value INTEGER);
        CREATE TABLE {v_table_name}_null (oid INTEGER);
        CREATE TABLE {v_table_name}_col (column_name VARCHAR(50), data_type CHAR(3));
    """
    )

    cursor.execute(
        f"""
        INSERT INTO {v_table_name}_col
        SELECT
            column_name,
            CASE WHEN data_type = 'integer' THEN 'int' ELSE 'str' END
        FROM information_schema.columns
        WHERE table_name = '{h_table_name}' AND column_name != 'oid'
    """
    )

    cursor.execute(f"SELECT * FROM {v_table_name}_col")

    columns = cursor.fetchall()

    for [column, data_type] in columns:
        if data_type != "int":
            cursor.execute(
                f"""
                INSERT INTO {v_table_name}_str (oid, key, value)
                SELECT oid, '{column}', {column}
                FROM {h_table_name}
                WHERE {column} IS NOT NULL
            """
            )
        else:
            cursor.execute(
                f"""
                INSERT INTO {v_table_name}_int (oid, key, value)
                SELECT oid, '{column}', {column}
                FROM {h_table_name}
                WHERE {column} IS NOT NULL
            """
            )

    cursor.execute(
        f"""
        INSERT INTO {v_table_name}_null (oid)
        SELECT oid FROM {h_table_name}
        WHERE oid NOT IN (
            SELECT DISTINCT oid FROM {v_table_name}_int
            FULL OUTER JOIN {v_table_name}_str USING (oid)
        )"""
    )

    if indexing:
        for suffix in "str", "int", "null":
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_oid ON {v_table_name}_{suffix}(oid);"
            )
        for suffix in "str", "int":
            cursor.execute(
                f"CREATE INDEX IF NOT EXISTS idx_{v_table_name}_key ON {v_table_name}_{suffix}(key);"
            )

    conn.commit()


def v2h(cursor, v_table_name, h_view_name):
    cursor.execute(f"DROP VIEW IF EXISTS {h_view_name}")

    cursor.execute(f"SELECT column_name, data_type FROM {v_table_name}_col")
    typed_cols = cursor.fetchall()
    typed_cols = sorted(typed_cols, key=lambda x: x[0])

    cursor.execute(  # how do we not loose the empty column?
        f"""
        CREATE VIEW {h_view_name} AS
        SELECT b.oid, {
            ", ".join([f"v{c}.value AS {c}" for c,_ in typed_cols])
        } FROM (
            (SELECT oid FROM {v_table_name}_str)
            UNION
            (SELECT oid FROM {v_table_name}_int)
            UNION ALL -- null rows are not stored in other tables -> still unique
            (SELECT oid FROM {v_table_name}_null)
        ) AS b {"\n".join(
            f"LEFT JOIN {v_table_name}_{t} AS v{c} ON b.oid = v{c}.oid AND v{c}.key = '{c}'"
            for c,t in typed_cols
        )}"""
    )

    conn.commit()


def test_identity(cursor, table1, table2):
    # for whatever reason, the order of the original table breaks at 80+
    cursor.execute(f"SELECT * FROM {table1} ORDER BY oid")
    rows1 = cursor.fetchall()
    # update changes order so there is also no other way
    cursor.execute(f"SELECT * FROM {table2} ORDER BY oid")
    rows2 = cursor.fetchall()
    print(len(rows1), len(rows2))
    assert len(rows1) == len(rows2)
    for r1, r2 in zip(rows1, rows2):
        if r1 != r2:
            print(f"Different rows:\n{r1}\n{r2}")
            return


def test_transform_randomized(cursor):
    phase1.generate_randomized("h", value_preview=-1)
    h2v(cursor, "h", "v")
    phase1.print_table(cursor, "v_str")
    v2h(cursor, "v", "h_view")
    phase1.print_table(cursor, "h_view")


def test_transform_identity(cursor, size=3):
    num_tuples = size
    num_attributes = size
    sparsity = 0.5
    table_name = "h"
    phase1.create_table(cursor, table_name, num_attributes)

    str_pool = phase1.prepare_string_pool(num_tuples, sparsity, num_attributes)
    for r in range(1, num_tuples + 1):
        values = []
        for i in range(1, num_attributes + 1):
            values.append(phase1.generate_column_value(1 if i <= r else 0, i, str_pool))
        insert_query = f"INSERT INTO {table_name} (oid, {', '.join([f'a{i}' for i in range(1, num_attributes + 1)] )}) VALUES ({r}, {', '.join(values)});"
        cursor.execute(insert_query)

    conn.commit()
    phase1.print_table(cursor, table_name)

    h2v(cursor, table_name, "v")
    phase1.print_table(cursor, "v_str")

    v2h(cursor, "v", "h_view")
    phase1.print_table(cursor, "h_view")

    test_identity(cursor, table_name, "h_view")


test_transform_identity(cursor)


def bench_oid(cursor, table_name, num_tuples):
    cursor.execute(
        f"SELECT * FROM {table_name} WHERE oid = {random.randint(1, num_tuples)}"
    )


def bench_vals(cursor, table_name, num_attributes):
    i = random.randint(1, num_attributes)
    if i % 2 == 0:
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE a{i} = {random.randint(1, phase1.allowed_integer)}"
        )
    else:
        cursor.execute(
            f"SELECT * FROM {table_name} WHERE a{i} = '{random.choice(phase1.allowed_strings)}'"
        )


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
    i = 0
    end_time = time.perf_counter()
    measure_loss = end_time - start_time
    if oid_test_preference < 0:  # to ensure both tests are run at least once
        while i < num_queries:
            i += 2
            bench_oid(cursor, table_name, num_tuples)
            bench_vals(cursor, table_name, num_attributes)
            end_time = time.perf_counter()
            if end_time - start_time > max_time:
                break
    else:
        while i < num_queries:
            i += 1
            if random.random() < oid_test_preference:
                bench_oid(cursor, table_name, num_tuples)
            else:
                bench_vals(cursor, table_name, num_attributes)
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
    t_range=range(10, 16, 2),  # base 2
    s_range=range(2, 8, 4),  # base 0.5
    a_range=range(5, 9, 4),
    num_queries=10,
    oid_test_preference=0.5,
    max_time=10,
):
    start_time = time.perf_counter()
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
                    cursor.execute(
                        f"SELECT {' + '.join(
                            [f"pg_total_relation_size('v_{s}')" for s in ('str','int','null')]
                        )}"
                    )
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

    with open("results.json", "w") as file:
        perf_results = sorted(results, key=lambda x: x["pdf"])
        mem_results = sorted(results, key=lambda x: x["mdf"])
        json.dump({"perf": perf_results, "mem": mem_results}, file, indent=4)

    print(
        f"Results saved to results.json in {time.perf_counter() - start_time}s, used {time.process_time()} CPU seconds"
    )


# phase1.allowed_strings = ['a', 'b', 'c', 'd', 'e']
benchmark(
    t_range=range(10, 17, 2),
    s_range=range(2, 22, 3),
    a_range=range(5, 11, 4),
    num_queries=100,
    oid_test_preference=0.5,
    max_time=100,
)
