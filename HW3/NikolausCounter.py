from edge_modell import *
from Phase2 import *

from lxml import etree

# Lade die XML-Datei
tree = etree.parse("HW3/my_small_bib.xml") 
root = tree.getroot()

def NikolausCounter():
    venue_list = ["ICDE", "SIGMOD", "VLDB"]
    author_name = "Nikolaus Augsten"
    for venue in venue_list:

        count = root.xpath(
            f"count(//venue[@key='{venue}']//*/author[text()='{author_name}'])"
        )
        print(f"Anzahl der Autoren '{author_name}' in {venue}: {int(count)}")

def print_pre_post_notation():
    print("\n=== Pre-Post-Notation für toy_example.xml ===")
    cursor.execute(
    """
    SELECT a.pre, a.post, a.tag, ac.text, aa.key, aa.value
    FROM accel a
    LEFT JOIN accel_content ac ON a.pre = ac.pre
    LEFT JOIN accel_attr aa ON a.pre = aa.pre
    WHERE a.tag IN ('article', 'inproceedings', 'author', 'year', 'venue')
    ORDER BY a.pre
    """
)
    rows = cursor.fetchall()
    for row in rows:
        pre, post, tag, text, key, value = row
        print(f"Pre: {pre}, Post: {post}, Tag: {tag}")
        if text:
            print(f"  Content: {text}")
        if key and value:
            print(f"  Attribute: {key} = {value}")

def knoten_ids():
    # Ancestors von SchmittKAMM23
    schmitt_id = find_node(attr="%SchmittKAMM23")[0]
    ancestors = get_node_ancestors(schmitt_id)
    print(f"Ancestors von SchmittKAMM23: {len(ancestors)} Ergebnisse")
    print(
        f"  Erste ID: {ancestors[0] if ancestors else 'N/A'}, Letzte ID: {ancestors[-1] if ancestors else 'N/A'}"
    )

    # Descendants von SchmittKAMM23
    descendants = get_node_descendants(schmitt_id)
    print(f"Descendants von SchmittKAMM23: {len(descendants)} Ergebnisse")
    print(
        f"  Erste ID: {descendants[0] if descendants else 'N/A'}, Letzte ID: {descendants[-1] if descendants else 'N/A'}"
    )

    # Following Siblings von SchmittKAMM23
    following_siblings = get_node_following_siblings(schmitt_id)
    print(f"Following Siblings von SchmittKAMM23: {len(following_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {following_siblings[0] if following_siblings else 'N/A'}, Letzte ID: {following_siblings[-1] if following_siblings else 'N/A'}"
    )

    # Preceding Siblings von SchmittKAMM23
    preceding_siblings = get_node_preceding_siblings(schmitt_id)
    print(f"Preceding Siblings von SchmittKAMM23: {len(preceding_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {preceding_siblings[0] if preceding_siblings else 'N/A'}, Letzte ID: {preceding_siblings[-1] if preceding_siblings else 'N/A'}"
    )

    # Following Siblings von SchalerHS23
    schaler_id = find_node(attr="%SchalerHS23")[0]
    following_siblings = get_node_following_siblings(schaler_id)
    print(f"Following Siblings von SchalerHS23: {len(following_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {following_siblings[0] if following_siblings else 'N/A'}, Letzte ID: {following_siblings[-1] if following_siblings else 'N/A'}"
    )

    # Preceding Siblings von SchalerHS23
    preceding_siblings = get_node_preceding_siblings(schaler_id)
    print(f"Preceding Siblings von SchalerHS23: {len(preceding_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {preceding_siblings[0] if preceding_siblings else 'N/A'}, Letzte ID: {preceding_siblings[-1] if preceding_siblings else 'N/A'}"
    )

    ##XAccel##

    # Ancestors von SchmittKAMM23
    pre, post, parent, tag = find_accel(attr="%SchmittKAMM23")
    ancestors = get_accel_ancestors(pre, post)
    print(f"Ancestors von SchmittKAMM23: {len(ancestors)} Ergebnisse")
    print(
        f"  Erste ID: {ancestors[0] if ancestors else 'N/A'}, Letzte ID: {ancestors[-1] if ancestors else 'N/A'}"
    )

    # Descendants von SchmittKAMM23
    descendants = get_accel_descendants(pre, post)
    print(f"Descendants von SchmittKAMM23: {len(descendants)} Ergebnisse")
    print(
        f"  Erste ID: {descendants[0] if descendants else 'N/A'}, Letzte ID: {descendants[-1] if descendants else 'N/A'}"
    )

    # Following Siblings von SchmittKAMM23
    following_siblings = get_accel_following_siblings(pre, post, parent)
    print(f"Following Siblings von SchmittKAMM23: {len(following_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {following_siblings[0] if following_siblings else 'N/A'}, Letzte ID: {following_siblings[-1] if following_siblings else 'N/A'}"
    )

    # Preceding Siblings von SchmittKAMM23
    preceding_siblings = get_accel_preceding_siblings(pre, post, parent)
    print(f"Preceding Siblings von SchmittKAMM23: {len(preceding_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {preceding_siblings[0] if preceding_siblings else 'N/A'}, Letzte ID: {preceding_siblings[-1] if preceding_siblings else 'N/A'}"
    )

    # Following Siblings von SchalerHS23
    pre, post, parent, tag = find_accel(attr="%SchalerHS23")
    following_siblings = get_accel_following_siblings(pre, post, parent)
    print(f"Following Siblings von SchalerHS23: {len(following_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {following_siblings[0] if following_siblings else 'N/A'}, Letzte ID: {following_siblings[-1] if following_siblings else 'N/A'}"
    )

    # Preceding Siblings von SchalerHS23
    preceding_siblings = get_accel_preceding_siblings(pre, post, parent)
    print(f"Preceding Siblings von SchalerHS23: {len(preceding_siblings)} Ergebnisse")
    print(
        f"  Erste ID: {preceding_siblings[0] if preceding_siblings else 'N/A'}, Letzte ID: {preceding_siblings[-1] if preceding_siblings else 'N/A'}"
    )

if __name__ == "__main__":
    # Teste Accelerator mit dem Toy-Beispiel
    #test_toy_example()

    # Nikolaus Augsten Zähler
    NikolausCounter()

    #Knoten IDs
    knoten_ids()
    
    conn.close()
