from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse
from queue import Queue
import random
import requests
import sqlite3
import time
import threading

from indexing.indexer import index, index_images
from ranking.pagerank import ranking, tf_idf, combine_scores

HEADERS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
]

# Connect to db and create tables if they do not exist yet
def db_connect():
    connection = sqlite3.connect("search.db", check_same_thread=False)
    cursor = connection.cursor()

    # Create SQL tables if they do not exist yet
    cursor.execute('''CREATE TABLE IF NOT EXISTS URLs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL,
                        title TEXT,
                        description TEXT,
                        word_count INTEGER,
                        final_rank REAL DEFAULT NULL,
                        crawled BOOLEAN DEFAULT 0
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS WORDS (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        word TEXT UNIQUE NOT NULL
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS IMAGES (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT,
                        alt TEXT,
                        context TEXT,
                        source_url INTEGER NOT NULL,
                        image TEXT UNIQUE NOT NULL,
                        FOREIGN KEY (source_url) REFERENCES URLs(id)
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS INVERTED_INDEX (
                        word_id INTEGER NOT NULL,
                        page_id INTEGER NOT NULL,
                        frequency INTEGER DEFAULT 1,
                        score DEFAULT NULL,
                        UNIQUE (word_id, page_id),
                        FOREIGN KEY (word_id) REFERENCES WORDS(id),
                        FOREIGN KEY (page_id) REFERENCES URLs(id)
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS BLOCKED_URLs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        url TEXT UNIQUE NOT NULL
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS CONNECTIONS (
                        source_id INTEGER NOT NULL,
                        target_id INTEGER NOT NULL,
                        FOREIGN KEY (source_id) REFERENCES URLs(id),
                        FOREIGN KEY (target_id) REFERENCES URLs(id)
                    )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS RANKS (
                        url_id INTEGER UNIQUE NOT NULL,
                        rank REAL NOT NULL,
                        FOREIGN KEY (url_id) REFERENCES URLs(id)
                   )''')

    return connection, cursor

# Check robots.txt on each site to prevent going to unauthorized site and getting blocked
def can_parse(args, url):
    lock = args["lock"]
    parsed_url = urlparse(url)

    if not parsed_url.scheme or not parsed_url.netloc:
        print(f"Invalid URL format: {url}")
        return False

    # Only process HTTP/HTTPS URLs
    if parsed_url.scheme not in ('http', 'https'):
        print(f"Skipping non-HTTP URL: {url}")
        return False

    robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
    try:
        response = requests.get(robots_url, timeout=10)
        disallowed = []

        for line in response.text.splitlines():
            if line.lower().startswith("user-agent:"):
                parts = line.split()
                current_agent = parts[1]
            if line.startswith("disallow"):
                parts = line.split()
                if current_agent == "*" and len(parts) > 1:
                    disallowed.append(parts[1])
        
        for path in disallowed:
            if urlparse(url).path.startswith(path):
                print(f"DISALLOWED: {robots_url}")
                return False
        return True
    except requests.RequestException as e:
        print(f"Failed to access robots.txt: {robots_url}")
        print(f"Error: {e}")
        return False
    except Exception as e:
        print(f"Error with Robots.txt: {e}")
        return False

def parse_links(args, current_url, hyperlinks):
    queue = args["queue"]
    lock = args["lock"]
    connection = args["connection"]
    cursor = args["cursor"]
    connections = set()

    # Buffer to prevent too many sql injections
    BATCH_SIZE = 500
    buffer = []

    with lock:
        cursor.execute("SELECT id FROM URLs WHERE url = ?", (current_url, ))
        row = cursor.fetchone()
        if row:
            current_id = row[0]
        else:
            return

    for hyperlink in hyperlinks:
        try:
            url = hyperlink["href"]
            if url.startswith("#"):
                continue
            elif url.startswith("//"):
                url = "https:" + url
            elif url.startswith("/"):
                base = "{0.scheme}://{0.netloc}".format(requests.utils.urlparse(current_url))
                url = base + url
            elif not url.startswith("http"):
                continue

            with lock:
                # Check if the url is blocked
                cursor.execute("SELECT * FROM BLOCKED_URLs WHERE url = ?", (url, ))
                if not cursor.fetchone():
                    buffer.append((url,))
            if len(buffer) > BATCH_SIZE:
                with lock:
                    cursor.executemany("INSERT OR IGNORE INTO URLs (url) VALUES (?)", buffer)
                    connection.commit()

                    urls = tuple(url for (url,) in buffer)
                    bindings = ", ".join(["?"] * len(urls))
                    cursor.execute(f"SELECT id FROM URLs WHERE url IN ({bindings})", urls)
                    target_ids = cursor.fetchall()

                    connection_data = []
                    for target_id in target_ids:
                        connection_data.append((current_id, target_id[0]))

                    cursor.executemany("INSERT INTO CONNECTIONS (source_id, target_id) VALUES (?, ?)", connection_data)
                    connection.commit()

                    for url in buffer:
                        queue.put(url[0])
                    buffer = []
            connections.add(url)
            
        except Exception as e:
            print(f"Error processing hyperlink: {e}")
            continue
    if buffer:
        with lock:
            cursor.executemany("INSERT OR IGNORE INTO URLs (url) VALUES (?)", buffer)
            connection.commit()
            urls = tuple(url for (url,) in buffer)
            bindings = ", ".join(["?"] * len(urls))
            cursor.execute(f"SELECT id FROM URLs WHERE url IN ({bindings})", urls)
            target_ids = cursor.fetchall()

            connection_data = []
            for target_id in target_ids:
                connection_data.append((current_id, target_id[0]))
                
            cursor.executemany("INSERT OR IGNORE INTO CONNECTIONS (source_id, target_id) VALUES (?, ?)", connection_data)
            connection.commit()

            for url in buffer:
                queue.put(url[0])
    return connections
    
def crawl(args):
    queue = args["queue"]
    lock = args["lock"]
    count = args["count"]
    stop_crawl = args["stop_crawl"]
    connection = args["connection"]
    cursor = args["cursor"]
    errors = args["errors"]
    domain_delay = 2.0
    domain_last_request = {}
    
    while not stop_crawl.is_set():
        try:
            url = queue.get(timeout=1.0)
        except queue.Empty:
            # Make sure that there are urls that need to be crawled
            cursor.execute("SELECT COUNT(*) FROM URLs WHERE crawled = 0")
            remaining = cursor.fetchone()[0]
            if remaining == 0:
                stop_crawl.set()
                break
            else:
                continue

        try:
            with lock:
                # Get the first url that has not been crawled
                cursor.execute("SELECT id, url, crawled FROM URLs WHERE url = ? AND crawled = 0 LIMIT 1", (url, ))
                row = cursor.fetchone()
                if row is None:
                    continue        
                url_id = row[0]
                current_url = row[1]
                crawled = row[2]

                # Check robots.txt
                if not can_parse(args, current_url):
                    cursor.execute("INSERT OR IGNORE INTO BLOCKED_URLs (url) VALUES (?)", (url, ))
                    cursor.execute("DELETE FROM URLs WHERE url = ?", (url, ))
                    connection.commit()
                    continue

            parsed = urlparse(current_url)
            domain = parsed.netloc

            # Delay to prevent issues in browser
            with lock:
                last_time = domain_last_request.get(domain, 0)
                current_time = time.time()
                elapsed = current_time - last_time
                
                # Sleep only if needed for this domain
                if elapsed < domain_delay:
                    sleep_time = domain_delay - elapsed
                    time.sleep(sleep_time)
                
                # Update last request time for this domain
                domain_last_request[domain] = time.time()

            try:
                response = requests.get(current_url, headers={'User-Agent': random.choice(HEADERS)}, timeout=20)
                response.raise_for_status() 
            except requests.RequestException as e:
                print(f"Failed to retrieve {current_url}: {e}")
                with lock:
                    cursor.execute("INSERT OR IGNORE INTO BLOCKED_URLs (url) VALUES (?)", (current_url, ))
                    cursor.execute("DELETE FROM URLs WHERE id = ?", (url_id, ))
                    connection.commit()
                continue
            
            # Make sure that the content of url is in HTML
            content_type = response.headers.get('Content-Type', '').lower()
            # Else insert into urls as Non-HTML and continue
            if not content_type.startswith('text/html'):
                with lock:
                    cursor.execute("SELECT id, crawled FROM URLs WHERE url = ?", (current_url, ))
                    row = cursor.fetchone()
                    url_id = row[0]
                    # If non-HTML url is not already in URLs, insert it
                    if crawled == 0:
                        cursor.execute("UPDATE URLs SET title = ?, description = ?, crawled = 1 WHERE id = ?", ("Non-HTML", content_type, url_id, ))
                        connection.commit()
                    continue

            page = BeautifulSoup(response.content, "html.parser", from_encoding="iso-8859-1")
            hyperlinks = page.select("a[href]")
            images = page.find_all("img")
            parse_links(args, current_url, hyperlinks)
            
            # Create graph for urls that current url connects to
            with lock:
                cursor.execute("SELECT id FROM URLs WHERE url = ? and crawled = 1", (current_url, ))
                # Check if url has already been crawled
                if cursor.fetchone() is not None:
                    # Skip url if already crawled
                    continue
                count[0] += 1
                if count[0] > args["max_urls"]:
                    # Clear remaining urls to stop processing them
                    queue.queue.clear()
                    print(f"Errors: {errors[0]}")
                    print("Crawl limit reached. Exiting...")
                    stop_crawl.set()
                    break
                
            # Get information on each page
            # Title, description, and word count
            indexed_page = index(args, page, current_url)
            valid_images = index_images(args, current_url, images)
            with lock:
                # Save images to db
                cursor.executemany("INSERT OR IGNORE INTO IMAGES (image, title, alt, source_url, context) VALUES (?, ?, ?, ?, ?)", valid_images)

                # Set url to have been crawled
                cursor.execute("SELECT id FROM URLs WHERE url = ?", (current_url, ))
                row = cursor.fetchone()
                if row:
                    url_id = row[0]
                
                # Store url information into db
                cursor.execute("UPDATE URLs SET title = ?, description = ?, word_count = ?, crawled = 1 WHERE id = ?", (indexed_page["title"], indexed_page["description"], len(indexed_page["filtered_words"]), url_id))

                # Store word in inverted index
                for word in indexed_page["filtered_words"]:
                    cursor.execute("INSERT OR IGNORE INTO WORDS (word) VALUES (?)", (word, ))

                    cursor.execute("SELECT id FROM WORDS WHERE word = ?", (word, ))
                    word_id = cursor.fetchone()[0]
                    
                    cursor.execute("INSERT INTO INVERTED_INDEX (word_id, page_id, frequency) VALUES (?, ?, ?) ON CONFLICT DO UPDATE SET frequency = excluded.frequency", (word_id, url_id, indexed_page["filtered_words"][word]))

                connection.commit()
        except Exception as e:
            print(f"Error: {e}")
        finally:
            queue.task_done()

def spider_bot(connection, cursor):
    cursor.execute("SELECT url FROM URLs WHERE crawled = 0 LIMIT 3")
    starting_urls = [url[0] for url in cursor.fetchall()]
    if not starting_urls:
        starting_urls = [
            "https://en.wikipedia.org/wiki/Google",
            "https://www.bbc.com/news/world",
            "https://news.ycombinator.com/",
        ]
    for url in starting_urls:
        cursor.execute("INSERT OR IGNORE INTO URLs (url) VALUES (?)", (url, ))
        connection.commit()

    cursor.execute("SELECT url FROM URLs WHERE crawled = 0 LIMIT 3")
    urls_to_crawl = Queue()
    for url in cursor.fetchall():
        urls_to_crawl.put(url[0])
    crawl_count = [0]
    page_id = [0]
    errors = [0]
    word_index = {}
    page_index = {}

    # Number of URLs to crawl
    # Feel free to change for your preference
    # Change number of crawlers to help crawl faster
    MAX_URLS = 10000
    NUM_WORKERS = 20
    lock = threading.Lock()
    stop_crawl = threading.Event()

    args = {
        "count": crawl_count,
        "max_urls": MAX_URLS,
        "lock": lock,
        "queue": urls_to_crawl,
        "word_index": word_index,
        "page_index": page_index,
        "page_id": page_id,
        "stop_crawl": stop_crawl,
        "connection": connection,
        "cursor": cursor,
        "errors": errors
    }

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        for _ in range(NUM_WORKERS):
            executor.submit(crawl, args)

        print("All URLs have been crawled")

    # Rank pages and words
    ranking(connection, cursor)
    tf_idf(connection, cursor)
    combine_scores(connection, cursor)

    # Store urls that did not get crawled in the session into batch
    batch = []
    while not urls_to_crawl.empty():
        url = urls_to_crawl.get()
        batch.append((url, ))

    # Store urls that were not yet added to db into the db
    if batch:
        cursor.executemany("INSERT OR IGNORE INTO URLs (url) VALUES (?)", batch)
        connection.commit()

def main():
    connection, cursor = db_connect()
    spider_bot(connection, cursor)
    connection.close()

if __name__ == "__main__":
    main()