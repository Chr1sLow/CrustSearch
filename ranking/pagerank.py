import math
import sqlite3

def db_connect():
    connection = sqlite3.connect("search.db", check_same_thread=False)
    cursor = connection.cursor()

    return connection, cursor

def ranking(connection, cursor, damping=0.85, iterations=20, tolerance=1.0e-6):
    cursor.execute("SELECT id FROM URLs WHERE crawled = 1")
    nodes = [row[0] for row in cursor.fetchall()]
    num_nodes = len(nodes)

    if num_nodes == 0:
        print("No urls to calculate")
        return

    pagerank = {url: 1.0 / num_nodes for url in nodes}

    num_outgoing_links = {}
    incoming_links = {}

    cursor.execute("SELECT source_id, COUNT(*) FROM CONNECTIONS GROUP BY source_id")
    # Get count of how many links that source url points to
    for source_id, count in cursor:
        num_outgoing_links[source_id] = count

    cursor.execute("SELECT target_id, source_id FROM CONNECTIONS")
    # Get what urls point to a url
    for target_id, source_id in cursor:
        if incoming_links.get(target_id) is None:
            incoming_links[target_id] = []
        incoming_links[target_id].append(source_id)
    dangling_nodes = set(link for link in nodes if num_outgoing_links.get(link, 0) == 0)

    updated_pagerank = {}
    dangling_sum = damping * sum(pagerank[node] for node in dangling_nodes) / num_nodes

    # Calculate the pagerank score for each crawled url
    for _ in range(iterations):
        for node in nodes:
            rank = (1.0 - damping) / num_nodes
            rank += dangling_sum
            
            if node in incoming_links:
                for source in incoming_links[node]:
                    if num_outgoing_links.get(source, 0) > 0:
                        rank += damping * pagerank[node] / num_outgoing_links[source]

            updated_pagerank[node] = rank

        change = sum(abs(updated_pagerank[node] - pagerank[node]) for node in nodes)
        if change < tolerance:
            break
        pagerank = updated_pagerank
    for node in nodes:
        pagerank[node] = round(pagerank[node], 6)
        cursor.execute("INSERT OR REPLACE INTO RANKS (url_id, rank) VALUES (?, ?)", (node, pagerank[node]))

    connection.commit()
    print("Pagerank scores calculated")
    
    return pagerank


# Calculate the tf-idf score for each word in the url
def tf_idf(connection, cursor):
    try:
        cursor.execute("SELECT COUNT(*) FROM URLs WHERE crawled = 1")
        count = cursor.fetchone()[0]

        if count == 0 :
            print("No urls to calculate")
            return
        
        cursor.execute("""
            SELECT i.word_id, i.page_id, i.frequency, u.word_count 
            FROM INVERTED_INDEX i
            JOIN URLs u ON i.page_id = u.id
            WHERE u.crawled = 1 AND u.word_count > 0
        """)

        BATCH_SIZE = 5000
        buffer = []

        for word_id, page_id, freq, word_count in cursor.fetchall():
            term_freq = freq / word_count
            cursor.execute("SELECT COUNT(*) FROM INVERTED_INDEX WHERE word_id = ?", (word_id, ))
            docs_containing_word = cursor.fetchone()[0]

            inverse_doc_freq = math.log(count / docs_containing_word)

            tf_idf = round(term_freq * inverse_doc_freq, 6)

            buffer.append((tf_idf, word_id, page_id))

            if len(buffer) > BATCH_SIZE:
                cursor.executemany("UPDATE INVERTED_INDEX SET score = ? WHERE word_id = ? AND page_id = ?", buffer)
                buffer = []
                connection.commit()

        if buffer:
            cursor.executemany("UPDATE INVERTED_INDEX SET score = ? WHERE word_id = ? AND page_id = ?", buffer)

        print("tf-idf scores calculated")

        connection.commit()
    except Exception as e:
        print(f"Error: {e}")


# Combine the tf-idf score and pagerank score into one for each crawled url
def combine_scores(connection, cursor, alpha=0.7):
    try:
        cursor.execute('''SELECT page_id, SUM(score) FROM INVERTED_INDEX WHERE page_id IN (
                            SELECT id FROM URLs WHERE crawled = 1 AND word_count > 0) GROUP BY page_id''')
        tf_idf_scores = {row[0]: row[1] for row in cursor.fetchall()}
        max_score = max(tf_idf_scores.values())
        min_score = min(tf_idf_scores.values())

        if max_score == min_score:
            normalized_tf_idf_scores = {page_id: 1.0 for page_id in tf_idf_scores}
        else:
            normalized_tf_idf_scores = {page_id: (score - min_score) / (max_score - min_score) for page_id, score in tf_idf_scores.items()}

        ids = list(tf_idf_scores.keys())
        bindings = ", ".join(["?"] * len(ids))
        cursor.execute(f"SELECT url_id, rank FROM RANKS WHERE url_id IN ({bindings})", ids)

        BATCH_SIZE = 1000
        buffer = []

        for url_id, rank in cursor.fetchall():
            final_score = alpha * normalized_tf_idf_scores[url_id] + (1 - alpha) * rank
            buffer.append((final_score, url_id))

            if len(buffer) > BATCH_SIZE:
                cursor.executemany("UPDATE URLs SET final_rank = ? WHERE id = ?", buffer)
                buffer = []

        if buffer:
            cursor.executemany("UPDATE URLs SET final_rank = ? WHERE id = ?", buffer)

        print("Pagerank scores and tf-idf scores combined successfully")
        connection.commit()
    except Exception as e:
        print(f"Error: {e}")


def main():
    connection, cursor = db_connect()
    ranking(connection, cursor)


if __name__ == "__main__":
    main()