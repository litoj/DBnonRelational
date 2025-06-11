from lxml import etree
import psycopg2
from typing import Optional, List, Tuple
from collections import defaultdict

CHUNK_SIZE = 1024

try:
    conn = psycopg2.connect(
        dbname="xmldb", user="postgres", password="", host="localhost", port="5432"
    )
    cursor = conn.cursor()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL")
    raise error


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


def insert_bulk(
    entries: List[Tuple], table: str, attrNames: List, retAttr: Optional[str] = None
) -> List:
    format = f"({",".join("%s"for _ in attrNames)})"
    insertDef = f"INSERT INTO {table} ({",".join(attrName for attrName in attrNames)})"

    if retAttr:
        retAttr = f"RETURNING {retAttr}"
    else:
        retAttr = ""

    returnList = [None] * len(entries) if retAttr else []

    i = 0
    for j in range(0, len(entries), CHUNK_SIZE):
        values_str = ", ".join(
            cursor.mogrify(format, row).decode("utf-8")
            for row in entries[j : j + CHUNK_SIZE]
        )

        cursor.execute(f"{insertDef} VALUES {values_str} {retAttr}")

        if retAttr:
            for row in cursor.fetchall():
                returnList[i] = row[0]
                i += 1

    return returnList


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
        attrs = []
        node_objs = []
        if not self.db_id:
            nodes.append((self.tag, self.text))
            attrs = [(self.tag, k, v) for k, v in self.attrib.items()]
            node_objs.append(self)

            for child in self.children:
                edges.append((self, child))
                if child.db_id:
                    continue
                child_nodes, child_edges, child_attrs, child_objs = (
                    child.collect_unsaved()
                )
                nodes.extend(child_nodes)
                edges.extend(child_edges)
                attrs.extend(child_attrs)
                node_objs.extend(child_objs)

        return nodes, edges, attrs, node_objs

    def insert_all(self):  # perform bulk insertions to slightly improve perf
        nodes, edges, attrs, node_objs = self.collect_unsaved()

        node_values = [(n.tag, n.text) for n in node_objs]
        ids = insert_bulk(node_values, "node", ["tag", "content"], "id_node")

        for obj, db_id in zip(node_objs, ids):
            obj.db_id = db_id

        edge_values = [(p.db_id, c.db_id) for p, c in edges]
        insert_bulk(edge_values, "edge", ["id_from", "id_to"])

        attr_values = [(n.db_id, k, v) for n in node_objs for k, v in n.attrib.items()]
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


def xml_to_db_iterative_2nd_level(xml_file: str) -> Node:
    parser = etree.iterparse(xml_file, events=("start", "end"), load_dtd=True)
    event, elem = next(parser)
    root_node = Node(tag=elem.tag)
    root_node.attrib = dict(elem.attrib)

    venue_map = defaultdict(lambda: defaultdict(list))
    stack = []
    discarded_cnt = 0

    for event, elem in parser:
        if event == "start":
            node = Node(tag=elem.tag)
            node.attrib = dict(elem.attrib)
            if stack:  # no saving to root node
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
                print(f"Found piece in: v:{venue}, y:{year}")

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

    root_node.insert_all()
    print(f"Filtered out {discarded_cnt} publications of foreign venues.")
    return root_node


def get_ancestor_nodes(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
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


def get_descendant_nodes(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
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


def get_following_siblings(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
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


def get_preceding_siblings(node_id: int) -> List[Tuple[int, str, Optional[str]]]:
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
    print("\nAncestors of 'Daniel Ulrich Schmitt':")
    cursor.execute(
        """SELECT id_node FROM node WHERE content = 'Daniel Ulrich Schmitt'"""
    )
    id = cursor.fetchone()
    if id:
        print_tree_by_edges(get_ancestor_nodes(id[0]))

    print("\nDescendants of VLDB 2023:")
    cursor.execute("""SELECT id_node FROM attr WHERE key = 'key' and value = 'VLDB'""")
    id = cursor.fetchone()
    if id:
        print_tree_by_edges(get_descendant_nodes(id[0]))

    for name in ["SchmittKAMM23", "SchalerHS23"]:
        cursor.execute(
            """SELECT id_node FROM attr WHERE key = "key" and value LIKE %s""",
            (f"%{name}",),
        )
        id = (cursor.fetchone() or [])[0]
        if id:
            print(f"\nFollowing siblings of {name}, id={id}:")
            print(get_following_siblings(id))
            print(f"\nPreceding siblings of {name}, id={id}:")
            print(get_preceding_siblings(id))


if __name__ == "__main__":
    create_generic_schema()
    root_node = xml_to_db_iterative_2nd_level("HW3/toy_example.xml")
    # root_node = xml_to_db_iterative_2nd_level("./dblp.xml")

    toy_xpath_examples()

    # verify all data was saved
    print_tree_by_edges(get_descendant_nodes(root_node.db_id))

    conn.close()
