from edge_modell import *

from lxml import etree

# Lade die XML-Datei
tree = etree.parse("HW3/my_small_bib.xml") 
root = tree.getroot()

venue_list = ["ICDE", "SIGMOD", "VLDB"]
author_name = "Nikolaus Augsten"
for venue in venue_list:

    count = root.xpath(
        f"count(//venue[@key='{venue}']//*/author[text()='{author_name}'])"
    )
    print(f"Anzahl der Autoren '{author_name}' in {venue}: {int(count)}")
