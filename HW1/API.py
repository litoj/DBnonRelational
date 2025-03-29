import psycopg2
import time
import random
from collections import defaultdict
from Phase2 import h2v, v2h, generate, benchmark
import pandas as pd
import matplotlib.pyplot as plt

try:
    conn = psycopg2.connect(
        dbname="Sparsity",
        user="postgres",
        password="",
        host="localhost",
        port="5432",
    )
    cursor = conn.cursor()
    print("Connected to database")
except Exception as e:
    print("Error connecting to database")
    print(e)


def drop_existing_functions(cursor):
    """Bestehende Funktionen löschen, falls vorhanden"""
    try:
        cursor.execute("DROP FUNCTION IF EXISTS q_i(integer);")
        cursor.execute("DROP FUNCTION IF EXISTS q_ii_string(varchar, varchar);")
        cursor.execute("DROP FUNCTION IF EXISTS q_ii_int(varchar, integer);")
        conn.commit()
        print("Bestehende Funktionen gelöscht")
    except Exception as e:
        print("Fehler beim Löschen bestehender Funktionen:", e)
        conn.rollback()


def create_query_i_function(cursor):
    """Erstellt die Funktion für Einzel-Tupel-Abfragen"""
    try:
        cursor.execute(
            """
        CREATE OR REPLACE FUNCTION q_i(oid_param INTEGER)
        RETURNS TABLE(
            oid INTEGER,
            key VARCHAR(50),
            value TEXT
        ) AS $$
        BEGIN
            RETURN QUERY
            SELECT v.oid, v.key, v.value::TEXT
            FROM v_all v
            WHERE v.oid = oid_param;
        END;
        $$ LANGUAGE plpgsql;
        """
        )
        conn.commit()
        print("Funktion q_i erfolgreich erstellt")
    except Exception as e:
        print("Fehler beim Erstellen von q_i:", e)
        conn.rollback()


def create_query_ii_functions(cursor):
    """Erstellt die Funktionen für Attribut-basierte Abfragen"""
    try:
        # Für String-Attribute
        cursor.execute(
            """
        CREATE OR REPLACE FUNCTION q_ii_string(attr_name VARCHAR(50), attr_value VARCHAR(50))
        RETURNS TABLE(oid INTEGER) AS $$
        BEGIN
            RETURN QUERY
            SELECT v.oid
            FROM v_string v
            WHERE v.key = attr_name AND v.value = attr_value
            LIMIT 5;
        END;
        $$ LANGUAGE plpgsql;
        """
        )

        # Für Integer-Attribute
        cursor.execute(
            """
        CREATE OR REPLACE FUNCTION q_ii_int(attr_name VARCHAR(50), attr_value INTEGER)
        RETURNS TABLE(oid INTEGER) AS $$
        BEGIN
            RETURN QUERY
            SELECT v.oid
            FROM v_int v
            WHERE v.key = attr_name AND v.value = attr_value
            LIMIT 5;
        END;
        $$ LANGUAGE plpgsql;
        """
        )

        conn.commit()
        print("Funktionen q_ii_string und q_ii_int erfolgreich erstellt")
    except Exception as e:
        print("Fehler beim Erstellen der q_ii-Funktionen:", e)
        conn.rollback()


def benchmark_api(cursor, num_queries=1000):
    """Benchmark für die API-Funktionen"""
    start_time = time.time()

    for _ in range(num_queries):
        query_type = random.choice(["single_oid", "attribute_query"])

        if query_type == "single_oid":
            oid = random.randint(1, 10000)
            cursor.execute("SELECT * FROM q_i(%s);", (oid,))
        else:
            attr_num = random.randint(1, 20)  # Angenommene maximale Attributanzahl
            if attr_num % 2 == 0:
                attr_val = random.randint(1, 100)
                cursor.execute(
                    "SELECT * FROM q_ii_int(%s, %s);", (f"a{attr_num}", attr_val)
                )
            else:
                attr_val = random.choice(["a", "b", "c"])
                cursor.execute(
                    "SELECT * FROM q_ii_string(%s, %s);", (f"a{attr_num}", attr_val)
                )

    elapsed_time = time.time() - start_time
    print(f"API: {num_queries} queries in {elapsed_time:.2f} seconds")
    return elapsed_time


def compare_query_plans(cursor):
    """Vergleicht die Anfragepläne der verschiedenen Implementierungen"""
    print("\nVergleich der Anfragepläne:")

    # Beispiel-OID für Vergleich
    oid = random.randint(1, 10000)

    print("\n1. Originale horizontale Abfrage:")
    cursor.execute(f"EXPLAIN ANALYZE SELECT * FROM h WHERE oid = {oid};")
    print(cursor.fetchone()[0])

    print("\n2. Transformierte vertikale Abfrage (Phase 2):")
    cursor.execute(f"EXPLAIN ANALYZE SELECT * FROM h_view WHERE oid = {oid};")
    print(cursor.fetchone()[0])

    print("\n3. API-Funktion (Phase 3):")
    cursor.execute("EXPLAIN ANALYZE SELECT * FROM q_i(%s);", (oid,))
    print(cursor.fetchone()[0])


def apply_final_optimizations(cursor):
    try:
        # 1. Partitionierung nach OID-Bereichen
        cursor.execute("DROP TABLE IF EXISTS v_partitioned;")
        print("Optimierung 1: Partitionierung nach OID-Bereichen")
        cursor.execute(
            """
        CREATE TABLE v_partitioned (
            oid INTEGER,
            key VARCHAR(50),
            value TEXT
        ) PARTITION BY RANGE (oid);
        
        CREATE TABLE v_part_1 PARTITION OF v_partitioned
            FOR VALUES FROM (1) TO (5000);
        CREATE TABLE v_part_2 PARTITION OF v_partitioned
            FOR VALUES FROM (5000) TO (10000);
        CREATE TABLE v_part_3 PARTITION OF v_partitioned
            FOR VALUES FROM (10000) TO (MAXVALUE);
        
        INSERT INTO v_partitioned
        SELECT * FROM v_all;
        """
        )

        # 2. Materialisierte Sicht für häufig abgefragte Attribute
        print("Optimierung 2: Materialisierte Sicht für häufige Abfragen")
        cursor.execute(
            """
        CREATE MATERIALIZED VIEW v_frequent_attrs AS
        SELECT oid, key, value
        FROM v_all
        WHERE key IN ('a1', 'a2', 'a3', 'a4', 'a5')
        WITH DATA;
        
        CREATE INDEX idx_v_frequent_attrs_key_value ON v_frequent_attrs(key, value);
        """
        )

        conn.commit()
        print("Finale Optimierungen erfolgreich angewendet")
    except Exception as e:
        print("Fehler bei finalen Optimierungen:", e)


def run_extended_benchmark(cursor):
    """Führt den erweiterten Benchmark mit allen Varianten durch"""
    results = []

    # Testkonfigurationen
    configs = [(1000, 0.5, 5), (5000, 0.75, 10), (10000, 0.9, 20)]

    for config in configs:
        print(f"\nBenchmark für Konfiguration: {config}")
        generate(*config)
        h2v(cursor, "h", "v")
        v2h(cursor, "v", "h_view", config[2])

        # Phase 2 Implementierung
        h_time = benchmark(cursor, "h", 1000, silent=True)
        v_time = benchmark(cursor, "h_view", 1000, silent=True)

        # API-Funktionen erstellen
        create_query_i_function(cursor)
        create_query_ii_functions(cursor, config[2])

        # Phase 3 API
        api_time = benchmark_api(cursor, 1000)

        # Finale Optimierungen
        apply_final_optimizations(cursor)
        optimized_api_time = benchmark_api(cursor, 1000)

        results.append(
            {
                "config": config,
                "H_time": h_time,
                "V_time": v_time,
                "API_time": api_time,
                "Optimized_API_time": optimized_api_time,
            }
        )

    return pd.DataFrame(results)


def visualize_results(df):
    """Visualisiert die Benchmark-Ergebnisse"""
    fig, axes = plt.subplots(1, 2, figsize=(15, 5))

    # Performance-Vergleich
    df.plot(
        x="config",
        y=["H_time", "V_time", "API_time", "Optimized_API_time"],
        kind="bar",
        ax=axes[0],
        title="Query Performance (lower is better)",
        logy=True,
    )

    # Speedup-Vergleich
    df["Speedup_API_vs_H"] = df["H_time"] / df["API_time"]
    df["Speedup_Opt_vs_H"] = df["H_time"] / df["Optimized_API_time"]

    df.plot(
        x="config",
        y=["Speedup_API_vs_H", "Speedup_Opt_vs_H"],
        kind="bar",
        ax=axes[1],
        title="Speedup compared to horizontal (higher is better)",
    )

    plt.tight_layout()
    plt.show()

drop_existing_functions(cursor)
create_query_i_function(cursor)
create_query_ii_functions(cursor)
generate(1000, 0.5, 10)
h2v(cursor, "h", "v")


# 4. Funktionen testen
print("\nTest der API-Funktionen:")

# Test q_i
test_oid = random.randint(1, 1000)
cursor.execute("SELECT * FROM q_i(%s);", (test_oid,))
print(f"\nErgebnis von q_i({test_oid}):")
print(cursor.fetchone())

# Test q_ii_string
cursor.execute("SELECT * FROM q_ii_string('a1', 'a');")
print("\nErgebnis von q_ii_string('a1', 'a'):")
print(cursor.fetchall())

# Test q_ii_int
cursor.execute("SELECT * FROM q_ii_int('a2', 42);")
print("\nErgebnis von q_ii_int('a2', 42):")
print(cursor.fetchall())


# 5. Benchmark durchführen
print("\nBenchmark der API-Funktionen:")
api_time = benchmark_api(cursor, 1000)
print(f"API benötigte {api_time:.2f} Sekunden für 1000 Abfragen")

# 6. Finale Optimierungen anwenden
apply_final_optimizations(cursor)


cursor.close()
conn.close()
