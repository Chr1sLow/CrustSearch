from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
from nltk.tokenize import word_tokenize
from urllib.parse import urljoin
import nltk

ps = PorterStemmer()

def download_nltk():
    try:
        stopwords.words('english')
    except LookupError:
        nltk.download('stopwords')
    try:
        word_tokenize('test')
    except LookupError:
        nltk.download('punkt')

def index(args, page, url):
    lock = args["lock"]
    page_id = args["page_id"]
    word_index = args["word_index"]
    page_index = args["page_index"]
    
    # Get title of page
    if page.find("title"):
        title = page.find("title").get_text()
    else:
        title = url
    meta_description = page.find('meta', attrs={'name': 'description'})

    download_nltk()

    # Get description
    if meta_description and "content" in meta_description.attrs:
        description = meta_description["content"][:200]

        if len(description) > 200:
            description = description[:200] + "..."
    else:
        # If descrition doesn't exist, get the first 200 words
        text = page.get_text(separator=" ", strip=True)
        description = text[:200] + "..." if len(text) > 200 else text

    # Get all the words in the page
    text_content = page.get_text(separator=" ", strip=True)
    stop_words = set(stopwords.words('english'))
    tokens = word_tokenize(text_content.lower())
    valid_words = {}
    for word in tokens:
        if word.isalpha() and word not in stop_words:
            stem_word = ps.stem(word)
            # Count how many times that word appears in the page
            if stem_word in valid_words:
                valid_words[stem_word] += 1
            else:
                valid_words[stem_word] = 1

    with lock:
        for word in valid_words:
            if word not in word_index:
                word_index[word] = set()
            word_index[word].add(page_id[0])
        page_index[page_id[0]] = url
        page_id[0] += 1

    indexed_page = {
        "url": url,
        "title": title,
        "description": description,
        "filtered_words": valid_words
    }

    print(f"Url #{page_id[0]}: {url} \n Title: {title} \n Description: {description} \n Filtered Length: {len(valid_words)}")

    return indexed_page


def index_images(args, current_url, images):
    valid_images_data = []

    for img in images:
        src = img.get('src') or img.get('data-src') or img.get('data-lazy-src')
        if not src:
            continue
        if src.startswith("//"):
            new_src = "https:" + src

            img_url = urljoin(current_url, new_src)

            if img_url.endswith(".jpg") or img_url.endswith(".jpeg") or img_url.endswith(".png"):
                if img.find_previous('p'):
                    context = img.find_previous('p').text[:200]
                else:
                    context = None
                data = (
                    img_url,
                    img.get('title', ''),
                    img.get('alt', ''),
                    current_url,
                    context)
                
                valid_images_data.append(data)

    return valid_images_data