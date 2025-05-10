#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *


def fetch_matrix(table_name, rows, cols):
    """Fetch a matrix from the database and return it as a dictionary"""
    cursor.execute(f"SELECT i, j, val FROM {table_name} ORDER BY i, j")
    matrix = [[0 for _ in range(cols)] for _ in range(rows)]
    for row in cursor.fetchall():
        i, j, val = row
        matrix[i - 1][j - 1] = val
    return matrix


def get_data(tbl_name) -> dict:
    rows, cols = get_mat_size(tbl_name)
    return {
        "rows": rows,
        "cols": cols,
        "values": fetch_matrix(tbl_name, rows, cols),
    }


# A: {rows, cols, values}; B: -||-
def client_side_matmul(A_data: dict, B_data: dict, C_name):
    """Perform matrix multiplication on the client side"""
    A, a_rows, a_cols = A_data["values"], A_data["rows"], A_data["cols"]
    B, b_cols = B_data["values"], B_data["cols"]

    C = {}

    # Perform multiplication
    C = [
        [sum([A[i][k] * B[k][j] for k in range(a_cols)]) for j in range(b_cols)]
        for i in range(a_rows)
    ]

    # in eigene Methode auslagern

    # Store result in database
    i = 1
    for row in C:
        j = 1
        for val in row:
            cursor.execute(f"INSERT INTO {C_name} VALUES ({i}, {j}, {val})")
            j += 1
        i += 1
    conn.commit()
    # print(f"Client-side multiplication completed. Result stored in {C_name}")


if __name__ == "__main__":
    try:

        # Test with toy example
        client_side_matmul(get_data("A_toy"), get_data("B_toy"), "C_client_toy")

        # Test with random matrices
        generate("rnd", 5, 0.5)  # Generate random 5x6 and 6x5 matrices
        client_side_matmul(get_data("rnd_h"), get_data("rnd_v"), "rnd_result_client")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
