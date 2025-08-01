from collections.abc import Iterable
from lxml import etree
import psycopg2
from typing import Optional, List, Tuple
from collections import defaultdict

CHUNK_SIZE = 1024


conn = None


def connect():
    global conn, cursor
    if conn:
        try:
            conn.rollback()
            conn.close()
        except psycopg2.Error:
            pass
    try:
        conn = psycopg2.connect(
            dbname="xmldb", user="postgres", password="", host="localhost", port="5432"
        )
        cursor = conn.cursor()
        return conn, cursor
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL")
        raise error


conn, cursor = connect()


def create_generic_schema():
    cursor.execute("DROP TABLE IF EXISTS attr")
    cursor.execute("DROP TABLE IF EXISTS edge")
    cursor.execute("DROP TABLE IF EXISTS node")

    cursor.execute(
        """
        CREATE TABLE node (
            id_node SERIAL PRIMARY KEY,
            tag TEXT,
            content TEXT
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE edge (
            id_from INTEGER REFERENCES node(id_node),
            id_to INTEGER REFERENCES node(id_node)
        )
    """
    )
    cursor.execute(
        """
        CREATE TABLE attr (
            id_node INTEGER REFERENCES node(id_node),
            key TEXT,
            value TEXT
        )
    """
    )
    conn.commit()


def insert_bulk(entries: List[Tuple], table: str, colNames: Iterable):
    format = f"({",".join("%s"for _ in colNames)})"
    insertDef = f"INSERT INTO {table} ({",".join(attrName for attrName in colNames)})"

    i = 0
    for j in range(0, len(entries), CHUNK_SIZE):
        values_str = ", ".join(
            cursor.mogrify(format, row).decode("utf-8")
            for row in entries[j : j + CHUNK_SIZE]
        )

        cursor.execute(f"{insertDef} VALUES {values_str}")


class Node:
    def __init__(self, tag: str, text: Optional[str] = None):
        self.tag = tag
        self.text = text
        self.attrib = {}
        self.children: List["Node"] = []
        self.db_id: Optional[int] = None

    def collect_unsaved(self):
        nodes = []
        edges = []
        if not self.db_id:
            nodes.append(self)

            for child in self.children:
                edges.append((self, child))
                if child.db_id:
                    continue
                child_objs, child_edges = child.collect_unsaved()
                edges.extend(child_edges)
                nodes.extend(child_objs)

        return nodes, edges

    def insert_all(self):  # perform bulk insertions to slightly improve perf
        nodes, edges = self.collect_unsaved()

        i = 0
        for obj in nodes:
            if obj.db_id != None:
                raise ValueError("Objects cannot be saved twice")
            obj.db_id = i
            i += 1

        node_values = [(n.db_id, n.tag, n.text) for n in nodes]
        insert_bulk(node_values, "node", ["id_node", "tag", "content"])

        edge_values = [(p.db_id, c.db_id) for p, c in edges]
        insert_bulk(edge_values, "edge", ["id_from", "id_to"])

        attr_values = [(n.db_id, k, v) for n in nodes for k, v in n.attrib.items()]
        insert_bulk(attr_values, "attr", ["id_node", "key", "value"])

        conn.commit()


category_map = {
    "pvldb": "VLDB",
    "vldb": "VLDB",
    "pacmmod": "SIGMOD",
    "sigmod": "SIGMOD",
    "icde": "ICDE",
}


def categorize_node(pub: Node) -> tuple[str | None, str]:

    key = pub.attrib.get("key", "")
    key_parts = key.split("/")
    venue = category_map.get(key_parts[1], None) if len(key_parts) > 1 else None
    year = "unknown"
    for child in pub.children:
        if child.tag == "year" and child.text:
            year = child.text
            break
    return venue, year


def xml_to_db(xml_file: str, filter=True) -> Node:
    parser = etree.iterparse(xml_file, events=("start", "end"), load_dtd=True)
    event, elem = next(parser)
    root_node = Node(tag=elem.tag)
    root_node.attrib = dict(elem.attrib)

    venue_map = defaultdict(lambda: defaultdict(list))
    stack = []
    if not filter:  # add children to root node only when not filtering
        stack.append(root_node)
    discarded_cnt = 0

    for event, elem in parser:
        if event == "start":
            node = Node(tag=elem.tag)
            node.attrib = dict(elem.attrib)
            if stack:
                stack[-1].children.append(node)
            stack.append(node)
        elif event == "end" and stack:
            stack[-1].text = elem.text and elem.text.strip() or None
            elem.clear()
            node: Node = stack.pop()

            if not stack:  # save publication and clean children for memory savings
                venue, year = categorize_node(node)
                if not venue:  # ignore unknown categories
                    discarded_cnt += 1
                    continue

                venue_map[venue][year].append(node)
                print(f"Found piece in:v:{venue}, y:{year}")

    if filter:
        for venue, years in venue_map.items():
            venue_node = Node(tag="venue")
            venue_node.attrib["key"] = venue
            root_node.children.append(venue_node)
            for year, pubs in years.items():
                year_node = Node(tag="year")
                year_node.attrib["key"] = year
                venue_node.children.append(year_node)
                for pub in pubs:
                    year_node.children.append(pub)
        print(f"Filtered out {discarded_cnt} publications of foreign venues.")

    root_node.insert_all()
    return root_node


# root_node_id (id-1 for its index); [0: tag, 1: content, children_ids, attr names, attr values]
def get_edge_model() -> (
    tuple[int, list[tuple[str, str, list[int], list[str], list[str]]]]
):
    print("=== Erstellung des Edge-Modells im Hauptspeicher ===")
    print("  Suche Wurzelknoten...")
    cursor.execute(
        """
        SELECT id_node FROM node 
        WHERE id_node IN (
            SELECT id_from FROM edge 
            EXCEPT 
            SELECT id_to FROM edge
        ) 
        LIMIT 1
    """
    )
    root_node_id = cursor.fetchone()[0]

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
    print(f"  Gefunden: {len(nodes)} Knoten")
    return root_node_id, nodes


def export_tree_to_xml(output_file):
    # [0: tag, 1: content, 2: child_ids, 3: attrs, 4: values]
    root_node_id, nodes = get_edge_model()
    print("=== Speichern des Edge-Modells in XML ===")

    # save updated structure to the file
    print(f"  Speichere Knoten im '{output_file}'.")
    with open(output_file, "wb") as f:

        def write(string: str):
            f.write(string.replace("&", "&amp;").encode("utf-8"))

        write(f'<!DOCTYPE dblp SYSTEM "dblp.dtd">\n')

        def write_node(idx, level=0):
            # type, content or children, key of attr, values of attr
            tag, content, child_ids, attrs, values = nodes[idx]

            indent = "  " * level
            write(f"{indent}<{tag}")
            if attrs:
                for attr, value in zip(attrs, values):
                    write(f' {attr}="{value}"')

            write(">")

            if content:
                write(content)

            if child_ids:
                write("\n")
                child_ids.sort()
                for child in child_ids:
                    write_node(child, level + 1)
                write(indent)

            write(f"</{tag}>\n")

        write_node(root_node_id)


def find_node(
    id_node=-1,
    par_id=-1,
    tag=None,
    content=None,
    attr: str | tuple[str, str] | None = None,
) -> tuple[int, int, int, str]:
    query = "SELECT id_node, tag FROM node WHERE "
    conditions = []
    params = []
    if id_node != -1:
        conditions.append("id_node = %s")
        params.append(id_node)
    if par_id != -1:
        conditions.append("id_node IN (SELECT id_to FROM edge WHERE id_from = %s)")
        params.append(par_id)
    if tag:
        conditions.append("tag = %s")
        params.append(tag)
    if content:
        conditions.append("content LIKE %s")
        params.append(content)
    if attr:
        if isinstance(attr, tuple):
            conditions.append(
                "id_node IN (SELECT id_node FROM attr WHERE key = %s AND value LIKE %s)"
            )
            params.extend(attr)
        else:
            conditions.append(
                "id_node IN (SELECT id_node FROM attr WHERE key = 'key' AND value LIKE %s)"
            )
            params.append(attr)

    if not conditions:
        raise ValueError("At least one condition must be specified")

    cursor.execute(
        query + " AND ".join(conditions) + " LIMIT 1",
        params,
    )
    return cursor.fetchone()


def get_node_ancestors(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    cursor.execute(
        """
        WITH RECURSIVE ancestors(id_node, tag, content) AS (
            SELECT n.id_node, n.tag, n.content
            FROM node n
            WHERE n.id_node = %s
            UNION
            SELECT n.id_node, n.tag, n.content
            FROM edge e
            JOIN ancestors a ON e.id_to = a.id_node
            JOIN node n ON e.id_from = n.id_node
        )
        SELECT id_node, tag, content FROM ancestors WHERE id_node != %s
    """,
        (node_id, node_id),
    )
    return cursor.fetchall()


def get_node_descendants(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    cursor.execute(
        """
        WITH RECURSIVE descendants(id_node, tag, content) AS (
            SELECT n.id_node, n.tag, n.content
            FROM node n
            WHERE n.id_node = %s
            UNION
            SELECT n.id_node, n.tag, n.content
            FROM edge e
            JOIN descendants d ON e.id_from = d.id_node
            JOIN node n ON e.id_to = n.id_node
        )
        SELECT id_node, tag, content FROM descendants WHERE id_node != %s
    """,
        (node_id, node_id),
    )
    return cursor.fetchall()


def get_node_following_siblings(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    cursor.execute(
        """
        SELECT e1.id_to, n.tag, n.content
        FROM edge e1
        JOIN edge e2 ON e1.id_from = e2.id_from
        JOIN node n ON e1.id_to = n.id_node
        WHERE e2.id_to = %s AND e1.id_to > e2.id_to
    """,
        (node_id,),
    )
    return cursor.fetchall()


def get_node_preceding_siblings(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
    cursor.execute(
        """
        SELECT e1.id_to, n.tag, n.content
        FROM edge e1
        JOIN edge e2 ON e1.id_from = e2.id_from
        JOIN node n ON e1.id_to = n.id_node
        WHERE e2.id_to = %s AND e1.id_to < e2.id_to
    """,
        (node_id,),
    )
    return cursor.fetchall()


def print_tree_by_edges(nodes: List[Tuple[int, str, Optional[str]]]):
    node_map: dict[int, Tuple[str, Optional[str]]] = {n[0]: (n[1], n[2]) for n in nodes}

    cursor.execute("SELECT id_node, key, value FROM attr")
    attr_map: dict[int, List[Tuple[str, str]]] = defaultdict(list)
    for node_id, key, value in cursor.fetchall():
        attr_map[node_id].append((key, value))

    cursor.execute("SELECT id_from, id_to FROM edge")
    edges = cursor.fetchall()

    children_map: dict[int, List[int]] = defaultdict(list)
    parent_map: dict[int, int] = {}
    for frm, to in edges:
        if frm in node_map and to in node_map:
            children_map[frm].append(to)
            parent_map[to] = frm

    roots = [n for n in node_map if n not in parent_map]

    def print_subtree(node_id: int, indent: int = 0):
        tag, content = node_map[node_id]
        attrs = attr_map.get(node_id, [])
        attr_str = " ".join(f'{k}="{v}"' for k, v in attrs)
        line = f"<{tag}{' ' + attr_str if attr_str else ''}>{' ' + content if content else ''}"
        print("  " * indent + line)
        for child_id in children_map.get(node_id, []):
            print_subtree(child_id, indent + 1)

    for r in roots:
        print_subtree(r)


def toy_xpath_examples():
    print("# 1. Test Ancestors für 'Daniel Ulrich Schmitt'")
    result = get_node_ancestors(find_node(content="Daniel Ulrich Schmitt")[0])
    print(len(result))
    print_tree_by_edges(result)

    print("# 2. Test Descendants für VLDB 2023")
    venue = find_node(attr="VLDB")[0]
    result = get_node_descendants(find_node(par_id=venue, attr="2023")[0])
    print(len(result))

    print("# 3. Test Siblings für spezifische Artikel")
    result = get_node_following_siblings(find_node(attr="%SchmittKAMM23")[0])
    print(len(result))


if __name__ == "__main__":
    try:
        cursor.execute("SELECT COUNT(*) FROM node")
        if cursor.fetchone()[0]:
            print("Datenbank existiert bereits - Überspringe Import")
    except Exception as e:
        conn, cursor = connect()
        print("=== Dateiimport läuft durch ===")
        create_generic_schema()
        xml_to_db("HW3/toy_example.xml")

    toy_xpath_examples()

    conn.close()
