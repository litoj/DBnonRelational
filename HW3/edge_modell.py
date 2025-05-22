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

    def insert_all(self): # perform bulk insertions to slightly improve perf
        nodes, edges, attrs, node_objs = self.collect_all()

        node_values = [(n.tag, n.text) for n in node_objs]
        ids = []
        for i in range(0, len(node_values), CHUNK_SIZE):
            chunk = node_values[i : i + CHUNK_SIZE]
            values_str = ", ".join(
                cursor.mogrify("(%s, %s)", row).decode("utf-8") for row in chunk
            )
            cursor.execute(
                f"INSERT INTO node (tag, content) VALUES {values_str} RETURNING id_node"
            )
            ids.extend([row[0] for row in cursor.fetchall()])

        for obj, db_id in zip(node_objs, ids):
            obj.db_id = db_id

        edge_values = [(p.db_id, c.db_id) for p, c in edges]
        for i in range(0, len(edge_values), CHUNK_SIZE):
            chunk = edge_values[i : i + CHUNK_SIZE]
            values_str = ", ".join(
                cursor.mogrify("(%s, %s)", row).decode("utf-8") for row in chunk
            )
            cursor.execute(f"INSERT INTO edge (id_from, id_to) VALUES {values_str}")

        for i in range(0, len(node_objs), CHUNK_SIZE):
            attr_chunk = []
            for obj in node_objs[i : i + CHUNK_SIZE]:
                for k, v in obj.attrib.items():
                    attr_chunk.append((obj.db_id, k, v))
            if attr_chunk:
                values_str = ", ".join(
                    cursor.mogrify("(%s, %s, %s)", row).decode("utf-8")
                    for row in attr_chunk
                )
                cursor.execute(
                    f"INSERT INTO attr (id_node, key, value) VALUES {values_str}"
                )

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
