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


def client_side_matmul(A_name, B_name, C_name):
    """Perform matrix multiplication on the client side"""
    # Fetch matrices from database
    A = fetch_matrix(A_name)
    B = fetch_matrix(B_name)

    a_rows, a_cols = get_mat_size(A_name)
    b_rows, b_cols = get_mat_size(B_name)
    
    # fill zeros in A and B
    A = fill_zeros(A, a_rows, a_cols)
    B = fill_zeros(B, b_rows, b_cols)
    #B = fill_zeros(B, b_rows, b_cols)
    # Initialize result matrix
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
    print(f"Client-side multiplication completed. Result stored in {C_name}")

if __name__ == "__main__":
    try:

        # Test with toy example
        client_side_matmul("A_toy", "B_toy", "C_toy_client")

        # Test with random matrices
        generate("rnd", 5, 0.5)  # Generate random 5x6 and 6x5 matrices
        client_side_matmul("rnd_h", "rnd_v", "rnd_result_client")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
