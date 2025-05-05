#!/usr/bin/env python
# coding: utf-8
import random
import psycopg2
import random


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
        CREATE INDEX idx_{table_name}_pos ON {table_name}(i, j);
    """
    )


def get_mat_size(table_name):
    cursor.execute(f"SELECT max(i), max(j) FROM {table_name}")
    return cursor.fetchone()


def generate_table(table_name, width, height, zeros_ratio):
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


def generate(base_name="rnd", size=10, sparsity=0.5):
    generate_table(f"{base_name}_h", size + 1, size, sparsity)
    generate_table(f"{base_name}_v", size, size + 1, sparsity)


def sparsity_check(table_name):
    [width, height] = get_mat_size(table_name)
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    value_count = cursor.fetchone()[0]

    cursor.execute(f"SELECT val FROM {table_name} WHERE i={width} AND j={height}")
    size_anchor_value = cursor.fetchone()

    if size_anchor_value and size_anchor_value[0] == 0:
        value_count -= 1
    return 1 - (value_count / (width * height))


if __name__ == "__main__":
    # Example usage
    generate("rnd", 5, 0.5)
    print(sparsity_check("rnd_h"))
    print(sparsity_check("rnd_v"))

