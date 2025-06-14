from edge_modell import *
import os


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
            tag TEXT NOT NULL
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
    # [0: tag, 1: content, 2: child_ids, 3: attrs, 4: values]
    root_node_id, nodes = get_edge_model()

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


def find_accel(
    pre=-1,
    post=-1,
    par_pre=-1,
    tag=None,
    content=None,
    attr: str | tuple[str, str] | None = None,
) -> tuple[int, int, int, str]:
    conditions = []
    params = []
    if pre != -1:
        conditions.append("pre = %s")
        params.append(pre)
    if post != -1:
        conditions.append("post = %s")
        params.append(post)
    if par_pre != -1:
        conditions.append("parent = %s")
        params.append(par_pre)
    if tag:
        conditions.append("tag = %s")
        params.append(tag)
    if content:
        conditions.append("pre IN (SELECT pre FROM accel_content WHERE text LIKE %s)")
        params.append(content)
    if attr:
        if isinstance(attr, tuple):
            conditions.append(
                "pre IN (SELECT pre FROM accel_attr WHERE key = %s AND value LIKE %s)"
            )
            params.append(attr[0])
            params.append(attr[1])
        else:
            conditions.append(
                "pre IN (SELECT pre FROM accel_attr WHERE key = 'key' AND value LIKE %s)"
            )
            params.append(attr)
    if not conditions:
        raise ValueError("At least one condition must be specified")

    cursor.execute(
        "SELECT pre, post, parent, tag FROM accel WHERE "
        + " AND ".join(conditions)
        + " LIMIT 1",
        params,
    )
    return cursor.fetchone()


def get_accel_attrs(pre: int):
    cursor.execute(
        """
        SELECT a.tag, c.text, attrs, values
        FROM accel a, (
            SELECT
                array_agg(key) AS attrs,
                array_agg(value) AS values
            FROM accel_attr
            WHERE pre = %s
        )
        LEFT JOIN accel_content c ON c.pre = %s
        WHERE a.pre = %s
        """,
        (pre, pre, pre),
    )
    return cursor.fetchone()


def print_xml_accel(pre: int, indent=0):
    node = get_accel_attrs(pre) or ["error"]
    indent_str = " " * indent
    print(f"{indent_str}<{node[0]}", end="")
    if node[2]:
        for attr, value in zip(node[2], node[3]):
            print(f' {attr}="{value}"', end="")
    print(">", end="")
    if node[1]:
        print(node[1], end="")
    print(f"</{node[0]}>")


def get_accel_ancestors(pre: int, post: int):
    cursor.execute(
        """
        SELECT a.pre, a.tag, a.parent, a.tag
        FROM accel a
        WHERE a.pre < %s AND a.post > %s
        ORDER BY a.pre
        """,
        (pre, post),
    )
    return cursor.fetchall()


def get_accel_descendants(pre: int, post: int):
    cursor.execute(
        """
        SELECT a.pre, a.tag, a.parent, a.tag
        FROM accel a
        WHERE a.pre > %s AND a.post < %s
        ORDER BY a.pre
        """,
        (pre, post),
    )
    return cursor.fetchall()


def get_accel_following_siblings(pre: int, post: int, parent_pre: int):
    cursor.execute(
        """
        SELECT a.pre, a.tag, a.parent, a.tag
        FROM accel a
        WHERE a.parent = %s AND a.pre > %s
        ORDER BY a.pre
        """,
        (parent_pre, pre),
    )
    return cursor.fetchall()


def get_accel_preceding_siblings(pre: int, post: int, parent_pre: int):
    cursor.execute(
        """
        SELECT a.pre, a.tag, a.parent, a.tag
        FROM accel a
        WHERE a.parent = %s AND a.pre < %s
        ORDER BY a.pre DESC
        """,
        (parent_pre, pre),
    )
    return cursor.fetchall()


def test_toy_example():
    print("# 1. Test Ancestors für 'Daniel Ulrich Schmitt'")
    result = get_accel_ancestors(*find_accel(content="Daniel Ulrich Schmitt")[:2])
    print(len(result))
    for n in result:
        print_xml_accel(n[0], 2)

    print("# 2. Test Descendants für VLDB 2023")
    venue = find_accel(attr="VLDB")[0]
    result = get_accel_descendants(*find_accel(par_pre=venue, attr="2023")[:2])
    print(len(result))

    print("# 3. Test Siblings für spezifische Artikel")
    result = get_accel_following_siblings(*find_accel(attr="%SchmittKAMM23")[:3])
    print(len(result))


if __name__ == "__main__":
    # Datenbank initialisieren (falls nicht vorhanden)
    try:
        cursor.execute("SELECT COUNT(*) FROM node")
        if cursor.fetchone()[0] < 70:
            raise Exception("Datenbank ist leer, muss befüllt werden")
        else:
            print("Datenbank existiert bereits - Überspringe Import")
    except Exception as e:
        conn, cursor = connect()
        print("=== Dateiimport läuft durch ===")
        create_generic_schema()
        exported = "./HW3/my_small_bib.xml"
        if not os.path.exists(exported):
            xml_to_db("./HW3/dblp.xml")
            export_tree_to_xml(exported)
        else:
            xml_to_db(exported, filter=False)
            # export_tree_to_xml("./HW3/identity-check-reexport.xml")

        print("=== Dateiimport fertig ===")


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
        conn, cursor = connect()
        create_accelerator_schema()
        populate_accelerator()
        complete_accellerator_schema()

    # Teste Accelerator mit dem Toy-Beispiel
    # print(get_ancestor_nodes(3))
    test_toy_example()

    # Accelerator testen
    # test_accelerator()

    conn.close()
