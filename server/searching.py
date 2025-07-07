from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
import math
import nltk
import sqlite3


ps = PorterStemmer()

def db_connect():
    connection = sqlite3.connect("search.db", check_same_thread=False)
    cursor = connection.cursor()

    return connection, cursor

def download_nltk():
    try:
        stopwords.words('english')
    except LookupError:
        nltk.download('stopwords')
    try:
        word_tokenize('test')
    except LookupError:
        nltk.download('punkt')

def search_api(user_search, page=1, items_per_page=20):
    connection, cursor = db_connect()
    offset = (page - 1) * items_per_page

    download_nltk()

    stop_words = set(stopwords.words('english'))
    tokens = word_tokenize(user_search.lower())
    valid_words = [ps.stem(word) for word in tokens if word.isalpha() and word not in stop_words]
    words = [word for word in valid_words]
    bindings = ", ".join(["?"] * len(words))

    cursor.execute(f'''SELECT COUNT(*) FROM URLs WHERE id IN (
                        SELECT page_id FROM INVERTED_INDEX WHERE word_id IN (
                            SELECT id FROM WORDS WHERE word IN ({bindings})
                        )          
                   )''', tuple(words))
    
    count = cursor.fetchone()[0]
    page_count = math.ceil(count / items_per_page)

    params = tuple(words) + (items_per_page, offset)

    cursor.execute(f'''SELECT title, url, description FROM URLs WHERE id IN (
                        SELECT page_id FROM INVERTED_INDEX WHERE word_id IN (
                            SELECT id FROM WORDS WHERE word IN ({bindings})
                        )          
                   ) ORDER BY final_rank DESC LIMIT ? OFFSET ?''', params)    

    search = cursor.fetchall()

    connection.close()
    
    return search, page_count

# Get a random url from lucky
def random_api():
    connection, cursor = db_connect()

    cursor.execute("SELECT url FROM URLs WHERE crawled = 1 ORDER BY RANDOM() LIMIT 1")
    random_url = cursor.fetchone()[0]
    connection.close()

    return random_url

def search_images(user_search, page=1, items_per_page=20):
    connection, cursor = db_connect()
    offset = (page - 1) * items_per_page

    cursor.execute(f"SELECT image, alt, source_url FROM IMAGES WHERE context LIKE ? LIMIT ? OFFSET ?", (f"%{user_search}%", items_per_page, offset, ))
    images = cursor.fetchall()
    connection.close()
    
    return images

def main():
    search_api()

if __name__ == "__main__":
    main()