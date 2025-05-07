#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *

def sql_side_matmul(A_name, B_name, C_name):
    """Perform matrix multiplication on the SQL side using sparse representation"""
    try:

        # Store results in table C
        cursor.execute(
            f"""
            INSERT INTO {C_name} (i, j, val)
            SELECT A.i, B.j, SUM(A.val * B.val) AS val
            FROM {A_name} A, {B_name} B
            WHERE A.j = B.i
            GROUP BY A.i, B.j;
        """
        )
        conn.commit()
        # print(f"Result stored in {C_name}")

    except (Exception, psycopg2.Error) as error:
        print("Error during SQL-side multiplication:", error)
        raise


if __name__ == "__main__":
    try:
        create_table("C_sql_toy")
        # Test with toy example
        sql_side_matmul("A_toy", "B_toy", "C_sql_toy")


        # Test with random matrices
        generate("rnd", 5, 0.5) 
        create_table("rnd_result_sql")
        sql_side_matmul("rnd_h", "rnd_v", "rnd_result_sql")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
