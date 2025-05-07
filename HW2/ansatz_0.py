#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *


def fill_zeros(matrix, rows, cols):
    """Fill missing entries in the matrix with zeros"""
    for i in range(1, rows + 1):
        if i not in matrix:
            matrix[i] = {}
        for j in range(1, cols + 1):
            if j not in matrix[i]:
                matrix[i][j] = 0
    return matrix


def fetch_matrix(table_name):
    """Fetch a matrix from the database and return it as a dictionary"""
    cursor.execute(f"SELECT i, j, val FROM {table_name} ORDER BY i, j")
    matrix = {}
    for row in cursor.fetchall():
        i, j, val = row
        if i not in matrix:
            matrix[i] = {}
        matrix[i][j] = val
    return matrix


def get_data(tbl_name) -> dict:
    values = fetch_matrix(tbl_name)
    rows, cols = get_mat_size(tbl_name)
    return {
        "name": tbl_name,
        "rows": rows,
        "cols": cols,
        "values": fill_zeros(values, rows, cols),
    }


# A: {rows, cols, values}; B: -||-
def client_side_matmul(A_data: dict, B_data: dict, C_name):
    """Perform matrix multiplication on the client side"""
    # Fetch matrices from database
    A, a_rows, a_cols = A_data["values"], A_data["rows"], A_data["cols"]
    B, b_cols = B_data["values"], B_data["cols"]

    C = {}

    # Perform multiplication
    for i in range(1, a_rows + 1):
        C[i] = {}
        for j in range(1, b_cols + 1):
            sum_val = 0
            for k in range(1, a_cols + 1):
                # Get A[i][k], default to 0 if not present
                a_val = A.get(i, {}).get(k, 0)
                # Get B[k][j], default to 0 if not present
                b_val = B.get(k, {}).get(j, 0)
                sum_val += a_val * b_val
            C[i][j] = sum_val

    # Store result in database
    create_table(C_name)
    for i in C:
        for j in C[i]:
            cursor.execute(f"INSERT INTO {C_name} VALUES ({i}, {j}, {C[i][j]})")
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
