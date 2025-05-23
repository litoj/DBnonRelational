# file: xml_to_edge_model.py

from lxml import etree
import psycopg2
from typing import Optional, List, Tuple

CHUNK_SIZE = 1000

try:
    conn = psycopg2.connect(
        dbname="xmldb", user="postgres", password="", host="localhost", port="5432"
    )
    cursor = conn.cursor()
except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL")
    raise error


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
        self.parent: Optional["Node"] = None
        self.db_id: Optional[int] = None

    def add_child(self, child: "Node"):
        child.parent = self
        self.children.append(child)

    def collect_all(self):
        nodes = [(self.tag, self.text)]
        edges = []
        attrs = [(self.tag, k, v) for k, v in self.attrib.items()]
        node_objs = [self]

        for child in self.children:
            child_nodes, child_edges, child_attrs, child_objs = child.collect_all()
            nodes.extend(child_nodes)
            edges.append((self, child))
            edges.extend(child_edges)
            attrs.extend(child_attrs)
            node_objs.extend(child_objs)

        return nodes, edges, attrs, node_objs

    def insert_all(self):  # perform bulk insertions to slightly improve perf
        nodes, edges, attrs, node_objs = self.collect_all()

        node_values = [(n.tag, n.text) for n in node_objs]
        ids = insert_bulk(node_values, "node", ["tag", "content"], "id_node")

        for obj, db_id in zip(node_objs, ids):
            obj.db_id = db_id

        edge_values = [(p.db_id, c.db_id) for p, c in edges]
        insert_bulk(edge_values, "edge", ["id_from", "id_to"])

        attr_values = [(n.db_id, k, v) for n in node_objs for k, v in n.attrib.items()]
        insert_bulk(attr_values, "attr", ["id_node", "key", "value"])

        conn.commit()


def parse_generic_xml(xml_file: str) -> Node:
    root_node = None
    node_stack = []

    for event, elem in etree.iterparse(
        xml_file, events=("start", "end"), load_dtd=True
    ):
        if event == "start":
            node = Node(tag=elem.tag, text=None)
            node.attrib = dict(elem.attrib)
            if node_stack:
                node_stack[-1].add_child(node)
            node_stack.append(node)
            if root_node is None:
                root_node = node
        elif event == "end":
            node_stack[-1].text = elem.text.strip() if elem.text else None
            elem.clear()
            node_stack.pop()

    return root_node


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


def main():
    create_generic_schema()
    root_node = parse_generic_xml("toy_example.xml")
    root_node.insert_all()
    conn.close()


main()
