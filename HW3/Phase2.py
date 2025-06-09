from edge_modell import *


def create_accelerator_schema():
    """Erstellt das Schema für den XPath-Accelerator"""
    cursor.execute("DROP TABLE IF EXISTS accel CASCADE")
    cursor.execute("DROP TABLE IF EXISTS content CASCADE")
    cursor.execute("DROP TABLE IF EXISTS attribute CASCADE")

    cursor.execute(
        """
        CREATE TABLE accel (
            pre INTEGER PRIMARY KEY,
            post INTEGER,
            parent INTEGER,
            kind TEXT,
            name TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE content (
            pre INTEGER REFERENCES accel(pre),
            text TEXT
        )
    """
    )

    cursor.execute(
        """
        CREATE TABLE attribute (
            pre INTEGER REFERENCES accel(pre),
            name TEXT,
            value TEXT
        )
    """
    )
    conn.commit()


def calculate_pre_post_orders_iterative(root_node_id):
    cursor.execute("SELECT id_from, id_to FROM edge")
    edges = cursor.fetchall()
    children = defaultdict(list)
    for frm, to in edges:
        children[frm].append(to)

    pre_order = {}
    post_order = {}
    pre_counter = 0
    post_counter = 0
    stack = [(root_node_id, False)]

    while stack:
        node_id, processed = stack.pop()
        if processed:
            post_order[node_id] = post_counter
            post_counter += 1
        else:
            pre_order[node_id] = pre_counter
            pre_counter += 1
            stack.append((node_id, True))
            # Push children in reverse order to process them in order
            for child in reversed(children.get(node_id, [])):
                stack.append((child, False))
    return pre_order, post_order


def populate_accelerator(root_node_id):

    print("\n=== Starte Population des Accelerators ===")

    cursor.execute("SELECT id_node, tag, content FROM node")
    nodes = cursor.fetchall()
    node_ids = [n[0] for n in nodes]
    total_nodes = len(node_ids)
    
    try:
        print("Erfasse alle Knoten...")
        nodes = get_descendant_nodes(root_node_id)
        node_ids = [n[0] for n in nodes]
        total_nodes = len(node_ids)
        print(f"  Gefunden: {total_nodes} Knoten")

        print("Berechne Pre- und Post-Order Werte...")
        pre_order, post_order = calculate_pre_post_orders_iterative(root_node_id)

        print("Lade Elternbeziehungen...")
        cursor.execute("SELECT id_from, id_to FROM edge")
        edges = cursor.fetchall()
        parents = {to: frm for frm, to in edges}

        print("Lade Inhalte aller Knoten...")
        cursor.execute(
            "SELECT id_node, content FROM node WHERE id_node = ANY(%s)", (node_ids,)
        )
        contents = dict(cursor.fetchall())

        print("Lade Attribute aller Knoten...")
        cursor.execute(
            "SELECT id_node, key, value FROM attr WHERE id_node = ANY(%s)", (node_ids,)
        )
        attr_map = {}
        for id_node, key, value in cursor.fetchall():
            attr_map.setdefault(id_node, []).append((key, value))

        print("Vorbereiten der Bulk-Daten...")
        accel_data = []
        content_data = []
        attribute_data = []

        for i, node_id in enumerate(node_ids, 1):
            pre = pre_order[node_id]
            post = post_order[node_id]

            # Fortschritt anzeigen
            if i % 10000 == 0 or i == total_nodes:
                print(f"  Fortschritt: {i}/{total_nodes}")

            # Tag laden
            cursor.execute("SELECT tag FROM node WHERE id_node = %s", (node_id,))
            tag = cursor.fetchone()[0]

            accel_data.append((pre, post, parents.get(node_id), tag, None))

            content = contents.get(node_id)
            if content:
                content_data.append((pre, content))

            for key, value in attr_map.get(node_id, []):
                attribute_data.append((pre, key, value))

        print("Führe Bulk-Inserts aus...")

        print(f"  Übertrage {len(accel_data)} Zeilen in accel-Tabelle")
        insert_bulk(accel_data, "accel", ["pre", "post", "parent", "kind", "name"])

        if content_data:
            print(f"  Übertrage {len(content_data)} Zeilen in content-Tabelle")
            insert_bulk(content_data, "content", ["pre", "text"])

        if attribute_data:
            print(f"  Übertrage {len(attribute_data)} Zeilen in attribute-Tabelle")
            insert_bulk(attribute_data, "attribute", ["pre", "name", "value"])

        conn.commit()
        print("=== Accelerator erfolgreich befüllt ===")

    except Exception as e:
        conn.rollback()
        print("Fehler während der Population:", str(e))


def test_accelerator():
    """Testet die Accelerator-Implementierung mit den XPath-Achsen"""
    print("\n=== Testing Accelerator Implementation ===")

    # 1. Ancestor-Achse testen
    cursor.execute(
        """
        SELECT a.pre FROM accel a JOIN content c ON a.pre = c.pre
        WHERE a.kind = 'author' AND c.text LIKE 'Daniel%'
        LIMIT 1
    """
    )
    author_pre = cursor.fetchone()[0]

    print("\nAncestors of author (pre=%d):" % author_pre)
    cursor.execute(
        """
        SELECT a2.pre, a2.kind FROM accel a1, accel a2
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
        SELECT pre FROM accel WHERE kind = 'year' LIMIT 1
    """
    )
    year_pre = cursor.fetchone()[0]

    print("\nDescendants of year (pre=%d):" % year_pre)
    cursor.execute(
        """
        SELECT a2.pre, a2.kind FROM accel a1, accel a2
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
        SELECT pre FROM accel WHERE kind IN ('article', 'inproceedings') LIMIT 1
    """
    )
    article_pre = cursor.fetchone()[0]

    print("\nFollowing siblings of article (pre=%d):" % article_pre)
    cursor.execute(
        """
        SELECT a2.pre, a2.kind FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.parent = a1.parent AND a2.pre > a1.pre
        ORDER BY a2.pre
    """,
        (article_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)

    print("\nPreceding siblings of article (pre=%d):" % article_pre)
    cursor.execute(
        """
        SELECT a2.pre, a2.kind FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.parent = a1.parent AND a2.pre < a1.pre
        ORDER BY a2.pre DESC
    """,
        (article_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)


def test_toy_example():
    print("\n=== Teste Accelerator am Toy-Beispiel ===")
    vldb_pre = 2

    print(f"\nDescendants of VLDB (pre={vldb_pre}):")
    cursor.execute(
        """
        SELECT a2.pre, a2.kind FROM accel a1, accel a2
        WHERE a1.pre = %s AND a2.pre > a1.pre AND a2.post < a1.post
        ORDER BY a2.pre
        """,
        (vldb_pre,),
    )
    for row in cursor.fetchall():
        print("  ", row)


if __name__ == "__main__":
    # Datenbank initialisieren (falls nicht vorhanden)
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'node'
        ) AND EXISTS (
            SELECT 1 FROM node LIMIT 1
        )
    """
    )
    if not cursor.fetchone()[0]:
        create_generic_schema()
        root_node = xml_to_db_iterative_2nd_level("DBnonRela    create_generic_schema()tional/HW3/dblp.xml")
    else:
        print("Datenbank existiert bereits - Überspringe Import")
    ###create_accelerator_schema()

    # Root-Node finden
    ###cursor.execute("SELECT id_node FROM node")
    ###root_node_id = cursor.fetchone()[0]

    # Accelerator-Tabellen befüllen
    ###populate_accelerator(root_node_id)

    # Teste Accelerator mit dem Toy-Beispiel
    test_toy_example()
    
    # Accelerator testen
    ###test_accelerator()

    conn.close()
