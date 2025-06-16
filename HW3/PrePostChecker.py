from edge_modell import *


def print_pre_post_notation():
    print("\n=== Pre-Post-Notation für toy_example.xml ===")
    cursor.execute(
            """
    SELECT DISTINCT a.pre, a.post, a.tag, ac.text
    FROM accel a
    LEFT JOIN accel_content ac ON a.pre = ac.pre
    ORDER BY a.pre
    """ 
    )
    rows = cursor.fetchall()
    for row in rows:
        pre, post, tag, text = row
        print(f"Pre: {pre}, Post: {post}, Tag: {tag}")
        if text:
            print(f"  Content: {text}")


if __name__ == "__main__":
    create_generic_schema() 
    xml_to_db("HW3/toy_example.xml") 
    print_pre_post_notation()  
    conn.close()
