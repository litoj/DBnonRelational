#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *


def sql_side_matmul(A_name, B_name):
    """Perform matrix multiplication on the SQL side using sparse representation"""
    try:
        # Execute the sparse matrix multiplication query
        cursor.execute(
            f"""
            SELECT A.i, B.j, SUM(A.val * B.val) AS val
            FROM {A_name} A, {B_name} B
            WHERE A.j = B.i
            GROUP BY A.i, B.j
            ORDER BY A.i, B.j
        """
        )

        # Fetch and return the result
        result = cursor.fetchall()
        print(f"SQL-side multiplication completed for {A_name} * {B_name}")
        return result

    except (Exception, psycopg2.Error) as error:
        print("Error during SQL-side multiplication:", error)
        raise


if __name__ == "__main__":
    try:
        # Test with toy example
        toy_result = sql_side_matmul("A_toy", "B_toy")
        print("Toy example result:", toy_result)

        # Test with random matrices
        generate("rnd", 5, 0.5) 
        rnd_result = sql_side_matmul("rnd_h", "rnd_v")
        print("Random matrices result sample:", rnd_result[:5])  # Print first 5 rows

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
