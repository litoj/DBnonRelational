from edge_modell import *


def create_accelerator_schema():
    print("=== Erstelle Accelerator-Schema ===")
    cursor.execute("DROP TABLE IF EXISTS attribute CASCADE")
    cursor.execute("DROP TABLE IF EXISTS accel_attr CASCADE")
    cursor.execute("DROP TABLE IF EXISTS content CASCADE")
    cursor.execute("DROP TABLE IF EXISTS accel_content CASCADE")
    cursor.execute("DROP TABLE IF EXISTS accel CASCADE")

    cursor.execute(
        """
        CREATE TABLE accel (
            pre INTEGER NOT NULL,
            post INTEGER NOT NULL,
            parent INTEGER,
            tag TEXT NOT NULL,
            content TEXT
        );
        CREATE TABLE accel_content (
            pre INTEGER NOT NULL,
            text TEXT NOT NULL
        );
        CREATE TABLE accel_attr (
            pre INTEGER NOT NULL,
            key TEXT NOT NULL,
            value TEXT NOT NULL
        );
    """
    )
    conn.commit()


def complete_accellerator_schema():
    print("=== Vervollständige Accelerator-Schema ===")
    cursor.execute("ALTER TABLE accel DROP CONSTRAINT IF EXISTS pk_accel_pre")
    cursor.execute(
        "ALTER TABLE accel_content DROP CONSTRAINT IF EXISTS fk_accel_content_pre"
    )
    cursor.execute("ALTER TABLE accel_attr DROP CONSTRAINT IF EXISTS fk_accel_attr_pre")

    cursor.execute(
        """
        ALTER TABLE accel ADD CONSTRAINT pk_accel_pre PRIMARY KEY (pre);
        ALTER TABLE accel_content ADD CONSTRAINT fk_accel_content_pre
            FOREIGN KEY (pre) REFERENCES accel(pre) ON DELETE CASCADE;
        ALTER TABLE accel ADD CONSTRAINT fk_attr_pre
            FOREIGN KEY (parent) REFERENCES accel(pre) ON DELETE CASCADE;
        ALTER TABLE accel_attr ADD CONSTRAINT fk_accel_attr_pre
            FOREIGN KEY (pre) REFERENCES accel(pre) ON DELETE CASCADE;
    """
    )
    conn.commit()


def populate_accelerator():
    print("=== Suchen für den Wurzelknoten ===")
    cursor.execute(
        """
        SELECT id_node FROM node WHERE id_node NOT IN (
            SELECT id_to FROM edge
        ) LIMIT 1
    """
    )
    root_node_id = cursor.fetchone()[0]

    print(f"=== Starte Population des Accelerators von Knoten={root_node_id} ===")

    # retrieve the entire tree, idx=id_node
    cursor.execute(
        """
        SELECT tag, content, COALESCE(child_ids, ARRAY[]::int[]), attrs, values
        FROM node
        LEFT JOIN (
            SELECT
                id_from AS id_node,
                array_agg(id_to) AS child_ids
            FROM edge
            GROUP BY id_from
        ) USING (id_node)
        LEFT JOIN (
            SELECT
                id_node,
                array_agg(key) AS attrs,
                array_agg(value) AS values
            FROM attr
            GROUP BY id_node
        ) USING (id_node)
        ORDER BY id_node ASC
    """
    )
    nodes = cursor.fetchall()
    # [0: tag, 1: content, 2: child_ids, 3: attrs, 4: values]
    print(f"  Gefunden: {len(nodes)} Knoten")

    # update all the nodes with pre and post order numbers
    print("  Berechne Pre- und Post-Order Werte...")
    # [0: pre_num, 1: post_num, 2: parent]
    vectors = [[0, 0, 0] for _ in range(len(nodes))]

    def update_pre_post_order(idx, pre_num, post_num, parent_pre) -> tuple[int, int]:
        vector = vectors[idx]

        vector[0] = pre_num
        node_pre_num = pre_num
        pre_num += 1

        vector[2] = parent_pre

        for child_id in nodes[idx][2]:
            pre_num, post_num = update_pre_post_order(
                child_id - 1, pre_num, post_num, node_pre_num
            )

        vector[1] = post_num
        return pre_num, post_num + 1

    update_pre_post_order(root_node_id - 1, 0, 0, None)

    # save updated structure to the accel tables
    print("  Speichere Knoten in Accelerator-Tabellen...")
    to_save = [
        (vector[0], vector[1], vector[2], node[0])
        for node, vector in zip(nodes, vectors)
    ]
    insert_bulk(to_save, "accel", ("pre", "post", "parent", "tag"))
    to_save = [(vector[0], node[1]) for node, vector in zip(nodes, vectors) if node[1]]
    insert_bulk(to_save, "accel_content", ("pre", "text"))
    to_save = [
        (v[0], key, val)
        for v, n in zip(vectors, nodes)
        for key, val in zip(n[3] or [], n[4] or [])
    ]
    insert_bulk(to_save, "accel_attr", ("pre", "key", "value"))
    conn.commit()
    print("=== Accelerator erfolgreich befüllt ===")


def test_accelerator():
    """Testet die Accelerator-Implementierung mit den XPath-Achsen"""
    print("\n=== Testing Accelerator Implementation ===")

    # 1. Ancestor-Achse testen
    cursor.execute(
        """
        SELECT a.pre FROM accel a JOIN accel_content c USING (pre)
        WHERE a.tag = 'author' AND c.text LIKE 'Daniel%'
        LIMIT 1
    """
    )
    author_pre = cursor.fetchone()[0]
    print(f"Ancestors of author (pre={author_pre}):")

    cursor.execute(
        """
        SELECT a2.pre, a2.tag FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.pre < a1.pre AND a2.post > a1.post
        ORDER BY a2.pre
    """,
        (author_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)

    # 2. Descendant-Achse testen
    cursor.execute(
        """
        SELECT pre FROM accel WHERE tag = 'year' LIMIT 1
    """
    )
    year_pre = cursor.fetchone()[0]
    print(f"Descendants of year (pre={year_pre})")

    cursor.execute(
        """
        SELECT a2.pre, a2.tag FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.pre > a1.pre AND a2.post < a1.post
        ORDER BY a2.pre
    """,
        (year_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)

    # 3. Sibling-Achsen testen
    cursor.execute(
        """
        SELECT pre FROM accel WHERE tag IN ('article', 'inproceedings') LIMIT 1
    """
    )
    article_pre = cursor.fetchone()[0]
    print(f"Following siblings of article (pre={article_pre})")

    cursor.execute(
        """
        SELECT a2.pre, a2.tag FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.parent = a1.parent AND a2.pre > a1.pre
        ORDER BY a2.pre
    """,
        (article_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)

    print(f"Preceding siblings of article (pre={article_pre})")
    cursor.execute(
        """
        SELECT a2.pre, a2.tag FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.parent = a1.parent AND a2.pre < a1.pre
        ORDER BY a2.pre DESC
    """,
        (article_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)


def get_accelerator_ancestors(node_key: str):
    cursor.execute(
        """
        WITH context AS (
            SELECT pre, post FROM accel 
            WHERE pre = (SELECT pre FROM accel_attr WHERE key = 'key' AND value = %s LIMIT 1)
        )
        SELECT a.pre, a.tag, a.content 
        FROM accel a, context c
        WHERE a.pre < c.pre AND a.post > c.post
        ORDER BY a.pre
        """,
        (node_key,),
    )
    return cursor.fetchall()


def get_accelerator_descendants(node_key: str):
    cursor.execute(
        """
        WITH context AS (
            SELECT pre, post FROM accel 
            WHERE pre = (SELECT pre FROM accel_attr WHERE key = 'key' AND value = %s LIMIT 1)
        )
        SELECT a.pre, a.tag, a.content 
        FROM accel a, context c
        WHERE a.pre > c.pre AND a.post < c.post
        ORDER BY a.pre
        """,
        (node_key,),
    )
    return cursor.fetchall()


def get_accelerator_following_siblings(
    node_key: str,
):
    cursor.execute(
        """
        WITH context AS (
            SELECT pre, parent FROM accel 
            WHERE pre = (SELECT pre FROM accel_attr WHERE key = 'key' AND value = %s LIMIT 1)
        )
        SELECT a.pre, a.tag, a.content 
        FROM accel a, context c
        WHERE a.parent = c.parent AND a.pre > c.pre
        ORDER BY a.pre
        """,
        (node_key,),
    )
    return cursor.fetchall()


def get_accelerator_preceding_siblings(
    node_key: str,
):
    cursor.execute(
        """
        WITH context AS (
            SELECT pre, parent FROM accel 
            WHERE pre = (SELECT pre FROM accel_attr WHERE key = 'key' AND value = %s LIMIT 1)
        )
        SELECT a.pre, a.tag, a.content 
        FROM accel a, context c
        WHERE a.parent = c.parent AND a.pre < c.pre
        ORDER BY a.pre DESC
        """,
        (node_key,),
    )
    return cursor.fetchall()


def test_toy_example():
    # 1. Test Ancestors für "Daniel Ulrich Schmitt"
    ancestors = get_accelerator_ancestors("Daniel Ulrich Schmitt")
    assert len(ancestors) == 3  # article, year, venue

    # 2. Test Descendants für VLDB 2023
    descendants = get_accelerator_descendants("vldb_2023")
    assert any(d[1] == "SchmittKAMM23" for d in descendants)

    # 3. Test Siblings für spezifische Artikel
    following = get_accelerator_following_siblings("SchmittKAMM23")
    assert following[0][1] == "SchalerHS23"


if __name__ == "__main__":
    # Datenbank initialisieren (falls nicht vorhanden)
    try:
        cursor.execute("SELECT COUNT(*) FROM node")
        if cursor.fetchone()[0] < 70:
            raise Exception("Datenbank ist leer, muss befüllt werden")
        else:
            print("Datenbank existiert bereits - Überspringe Import")
    except Exception as e:
        conn.rollback()
        create_generic_schema()
        create_accelerator_schema()
        root_node = xml_to_db_iterative_2nd_level("./HW3/dblp.xml")

    try:
        cursor.execute("SELECT COUNT(*) FROM node")
        node_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM accel")
        accelerator_count = cursor.fetchone()[0]

        if node_count != accelerator_count:
            raise Exception("Knoten fehlen in Accelerator-Tabelle, muss befüllt werden")
        else:
            print("Accellerator schon vorhanden - Überspringe Befüllung")
    except Exception as e:
        conn.rollback()
        create_accelerator_schema()
        populate_accelerator()
        complete_accellerator_schema()

    # Teste Accelerator mit dem Toy-Beispiel
    test_toy_example()

    # Accelerator testen
    #test_accelerator()

    conn.close()
