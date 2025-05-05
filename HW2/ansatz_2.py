#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *

### Teil 1 ###
'''
def create_vector_tables(A_name, B_name):
    """Create tables for vector representation of matrices"""
    try:
        # Create table for row-wise matrix (A)
        cursor.execute(
            f"""
            DROP TABLE IF EXISTS {A_name};
            CREATE TABLE {A_name} (
                i INTEGER NOT NULL,
                row INTEGER[] NOT NULL
            );
        """
        )

        # Create table for column-wise matrix (B)
        cursor.execute(
            f"""
            DROP TABLE IF EXISTS {B_name};
            CREATE TABLE {B_name} (
                j INTEGER NOT NULL,
                col INTEGER[] NOT NULL
            );
        """
        )

        conn.commit()
        print("Created vector representation tables")
    except (Exception, psycopg2.Error) as error:
        print("Error creating vector tables:", error)
        raise
'''
'''
def import_toy_example_vector():
    """Import the toy example in vector representation"""
    try:
        create_vector_tables("a_vector_toy", "b_vector_toy")

        # Matrix A row-wise
        # A = [[1, 0], [2, 3]] becomes:
        cursor.execute("INSERT INTO A_vector_toy VALUES (1, ARRAY[1, 0]);")
        cursor.execute("INSERT INTO A_vector_toy VALUES (2, ARRAY[2, 3]);")

        # Matrix B column-wise
        # B = [[4, 1], [0, 2]] becomes:
        cursor.execute("INSERT INTO B_vector_toy VALUES (1, ARRAY[4, 0]);")
        cursor.execute("INSERT INTO B_vector_toy VALUES (2, ARRAY[1, 2]);")

        conn.commit()
        print("Toy example imported in vector representation")
    except (Exception, psycopg2.Error) as error:
        print("Error importing toy example:", error)
        raise'''


def convert_to_vector(table_name, orientation="row"):
    """
    Convert a sparse matrix table (i,j,val) to vector format (row-wise or column-wise).
    orientation: "row" for row vectors (A), "col" for column vectors (B)
    """
    try:
        # Determine matrix dimensions
        cursor.execute(f"SELECT max(i), max(j) FROM {table_name}")
        max_i, max_j = cursor.fetchone()

        # Create target table
        col_def = "row" if orientation == "row" else "col"
        key_column = "i" if orientation == "row" else "j"
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}_vector;")
        cursor.execute(
            f"""
            CREATE TABLE {table_name}_vector (
                {key_column} INTEGER NOT NULL,
                {col_def} INTEGER[] NOT NULL
            );
        """
        )

        # For row-wise: One row per i with all j values as array
        if orientation == "row":
            for i in range(1, max_i + 1):
                # Build array for current row
                cursor.execute(
                    f"""
                    SELECT array_agg(val ORDER BY j) 
                    FROM (
                        SELECT j, COALESCE(
                            (SELECT val FROM {table_name} WHERE i = {i} AND j = js.j), 
                            0
                        ) AS val
                        FROM generate_series(1, {max_j}) AS js(j)
                    ) AS subq;
                """
                )
                row_array = cursor.fetchone()[0]
                cursor.execute(
                    f"INSERT INTO {table_name}_vector VALUES ({i}, %s);", (row_array,)
                )

        # For column-wise: One row per j with all i values as array
        else:
            for j in range(1, max_j + 1):
                # Build array for current column
                cursor.execute(
                    f"""
                    SELECT array_agg(val ORDER BY i) 
                    FROM (
                        SELECT i, COALESCE(
                            (SELECT val FROM {table_name} WHERE i = idx.i AND j = {j}), 
                            0
                        ) AS val
                        FROM generate_series(1, {max_i}) AS idx(i)
                    ) AS subq;
                """
                )
                col_array = cursor.fetchone()[0]
                cursor.execute(
                    f"INSERT INTO {table_name}_vector VALUES ({j}, %s);", (col_array,)
                )

        conn.commit()
        print(f"Converted {table_name} to {table_name}_vector ({orientation}-wise)")

    except (Exception, psycopg2.Error) as error:
        print(f"Error converting {table_name} to vector format:", error)
        raise


### Teil 2 ###

def create_dotprod_function():
    """Create the UDF for dot product calculation in PostgreSQL"""
    try:
        cursor.execute(
            """
            CREATE OR REPLACE FUNCTION dotprod(v1 INTEGER[], v2 INTEGER[]) 
            RETURNS INTEGER AS $$
            DECLARE
                result INTEGER := 0;
                len INTEGER;
            BEGIN
                len := array_length(v1, 1);
                FOR i IN 1..len LOOP
                    result := result + v1[i] * v2[i];
                END LOOP;
                RETURN result;
            END;
            $$ LANGUAGE plpgsql;
        """
        )
        conn.commit()
        print("Created UDF 'dotprod' for vector dot product.")
    except (Exception, psycopg2.Error) as error:
        print("Error creating dotprod function:", error)
        raise


def vector_matmul(A_name, B_name, C_name):
    """Perform matrix multiplication using vector representation (Example 2.2)"""
    try:
        # Create result table C (NOT vector)
        cursor.execute(f"DROP TABLE IF EXISTS {C_name};")
        cursor.execute(
            f"""
            CREATE TABLE {C_name} (
                i INTEGER NOT NULL,
                j INTEGER NOT NULL,
                val INTEGER NOT NULL
            );
        """
        )

        # Execute the multiplication query and store results in C
        cursor.execute(
            f"""
            INSERT INTO {C_name} (i, j, val)
            SELECT A.i, B.j, dotprod(A.row, B.col) AS val
            FROM {A_name} A, {B_name} B
            ORDER BY A.i, B.j;
        """
        )
        conn.commit()
        print(f"Vector-based multiplication completed. Result stored in {C_name}")

    except (Exception, psycopg2.Error) as error:
        print("Error during vector-based multiplication:", error)
        raise


if __name__ == "__main__":
    try:
        # Setup: Create tables and UDF
        create_dotprod_function()  
        convert_to_vector("A_toy", "row")
        convert_to_vector("B_toy", "col")

        # Test with toy example
        vector_matmul("A_toy_vector", "B_toy_vector", "C_toy_vector")

    except (Exception, psycopg2.Error) as error:
        print("Error:", error)
    finally:
        if conn:
            cursor.close()
            conn.close()
