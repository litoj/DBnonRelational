#!/usr/bin/env python
# coding: utf-8
import random
import psycopg2
import random


def print_table(cursor, table_name, max_rows=10):
    cursor.execute(f"SELECT * FROM {table_name} ORDER BY i, j")
    rows = cursor.fetchall()
    print(f"Table {table_name}")
    for row in rows[:max_rows]:
        print(row)
    print()


try:
    conn = psycopg2.connect(
        dbname="matmul", user="postgres", password="", host="localhost", port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    print(cursor.fetchone())
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL")
    raise error

allowed_integer = 2**10


def create_table(table_name):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")

    cursor.execute(
        f"""
        CREATE TABLE {table_name} (
            i INTEGER NOT NULL,
            j INTEGER NOT NULL,
            val INTEGER NOT NULL
        );
        CREATE INDEX idx_pos ON {table_name}(i, j);
    """
    )


def get_mat_size(table_name):
    cursor.execute(f"SELECT max(i), max(j) FROM {table_name}")
    return cursor.fetchone()


def generate(table_name, width, height, zeros_ratio):
    try:
        create_table(table_name)

        conn.commit()
        for i in range(1, width + 1):
            for j in range(1, height + 1):
                if random.random() >= zeros_ratio:
                    query = f"INSERT INTO {table_name} VALUES ({i}, {j}, {random.randint(1,allowed_integer)});"
                    cursor.execute(query)

        [max_i, max_j] = get_mat_size(table_name)
        if max_i != width or max_j != height:  # ensure size is preserved
            query = f"INSERT INTO {table_name} VALUES ({width}, {height}, 0)"
            cursor.execute(query)

        conn.commit()
        print(
            f"Generated '{table_name}': width={width}, height={height}, spars={zeros_ratio:.6}"
        )
    except (Exception, psycopg2.Error) as error:
        print("Fehler in generate():", error)


def sparsity_check(table_name):
    [width, height] = get_mat_size(table_name)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    value_count = cursor.fetchone()[0]

    cursor.execute(f"SELECT val FROM {table_name} WHERE i={width} AND j={height}")
    size_anchor_value = cursor.fetchone()[0]

    if size_anchor_value == 0:
        value_count -= 1
    return 1 - (value_count / (width * height))


def generate_randomized(
    table_name="rnd", width_limit=50, height_limit=50, value_preview=0
):
    width = random.randint(1, width_limit + 1)
    height = random.randint(1, height_limit + 1)
    sparsity = random.random()
    generate(table_name, width, height, sparsity)
    if value_preview != 0:
        print_table(cursor, table_name, max_rows=value_preview)
    return sparsity


generate_randomized()
print(sparsity_check("rnd"))
