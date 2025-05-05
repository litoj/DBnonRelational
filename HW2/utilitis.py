import psycopg2
from generator import *

def get_table_names():
    """Get all table names in the database"""
    cursor.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE';
        """
    )
    return [row[0] for row in cursor.fetchall()]    

def get_functions_names():
    """Get all function names in the database"""
    cursor.execute(
        """
        SELECT routine_name
        FROM information_schema.routines
        WHERE routine_schema = 'public'
        AND routine_type = 'FUNCTION';
        """
    )
    return [row[0] for row in cursor.fetchall()]

def delete_tables():
    """Delete all tables in the database"""
    table_names = get_table_names()
    for table_name in table_names:
        cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    conn.commit()
    print("All tables deleted")

def delete_functions():
    """Delete all functions in the database"""
    function_names = get_functions_names()
    for function_name in function_names:
        cursor.execute(f"DROP FUNCTION IF EXISTS {function_name} CASCADE;")
    conn.commit()
    print("All functions deleted")

def delete_all():
    """Delete all tables and functions in the database"""
    delete_tables()
    delete_functions()
    print("All tables and functions deleted")
    conn.commit()

delete_all()