#!/usr/bin/env python
# coding: utf-8
import psycopg2
from generator import *

def import_toy_example():
    # Matrix A (2x2 mit Null-Eintrag)
    create_table("A_toy")  # Tabelle A erstellen
    conn.commit()
    cursor.execute("INSERT INTO A_toy VALUES (1, 1, 1);")  # A[1][1] = 1
    cursor.execute("INSERT INTO A_toy VALUES (2, 1, 2);")  # A[2][1] = 2
    cursor.execute("INSERT INTO A_toy VALUES (2, 2, 3);")  # A[2][2] = 3
    # A[1][2] = 0 (wird nicht eingefügt)

    # Matrix B (2x2 mit Null-Eintrag)
    create_table("B_toy")  # Tabelle B erstellen
    conn.commit()
    cursor.execute("INSERT INTO B_toy VALUES (1, 1, 4);")  # B[1][1] = 4
    cursor.execute("INSERT INTO B_toy VALUES (1, 2, 1);")  # B[1][2] = 1
    cursor.execute("INSERT INTO B_toy VALUES (2, 2, 2);")  # B[2][2] = 2
    # B[2][1] = 0 (wird nicht eingefügt)

    conn.commit()
    print("Toy-Beispiel erfolgreich in die Datenbank importiert")


import_toy_example()  # Toy-Beispiel importieren
conn.commit()


#matmul_toy

def matmul_toy():
    # Matrix C (2x2 mit Null-Eintrag)
    create_table("C_toy") 
    conn.commit()
    cursor.execute("INSERT INTO C_toy VALUES (1, 1, 4);")  # C[1][1] = 4
    cursor.execute("INSERT INTO C_toy VALUES (1, 2, 1);")  # C[1][2] = 1
    cursor.execute("INSERT INTO C_toy VALUES (2, 1, 8);")  # C[2][1] = 8
    cursor.execute("INSERT INTO C_toy VALUES (2, 2, 8);")  # C[2][2] = 8

    conn.commit()
    print("Toy-Beispiel erfolgreich in die Datenbank importiert")

matmul_toy()

cursor.close()
conn.close()
