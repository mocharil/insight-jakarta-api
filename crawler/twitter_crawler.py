from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import time
import re
import pandas as pd
from elasticsearch import Elasticsearch, helpers
from dotenv import load_dotenv
import os
# Load environment variables
load_dotenv()

def setup_driver():
    """
    Set up and return a Selenium WebDriver instance.
    """
    service = ChromeService(executable_path=ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service)
    return driver

def login_to_twitter(driver, url="https://x.com/"):
    """
    Navigate to the Twitter login page and perform login.

    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.
        url (str): The URL of the Twitter login page.
    """
    driver.get(url)

    try:
        # Wait for the login button and click it
        login_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.LINK_TEXT, "Log in"))
        )
        login_button.click()

        # Wait for the username field and enter the username
        username_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, "text"))
        )
        username_field.send_keys(os.getenv("TWITTER_USERNAME"))

        # Click next or continue button
        next_button = driver.find_element(By.CSS_SELECTOR, "div[data-testid='LoginForm_Login_Button']")
        next_button.click()

        # Wait for the password field and enter the password
        password_field = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.NAME, os.getenv("TWITTER_PASSWORD"))))
        )
        password_field.send_keys("your_password")

        # Submit the login form
        login_button = driver.find_element(By.CSS_SELECTOR, "div[data-testid='LoginForm_Login_Button']")
        login_button.click()

        # Wait for the homepage to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='primaryColumn']"))
        )

        print("Login successful.")

    except Exception as e:
        print(f"Login failed: {e}")
        driver.quit()

def convert_formatted_number(formatted_number):
    """
    Convert formatted numbers like 1K, 1M to integers.

    Args:
        formatted_number (str): The formatted number string.

    Returns:
        int: The converted integer value.
    """
    if formatted_number.isdigit():
        return int(formatted_number)

    suffix_multipliers = {'K': 10**3, 'M': 10**6, 'B': 10**9}
    if formatted_number[-1] in suffix_multipliers:
        multiplier = suffix_multipliers[formatted_number[-1]]
        number_part = float(formatted_number[:-1])
        return int(number_part * multiplier)

    raise ValueError(f"Invalid formatted number: {formatted_number}")

def extract_tweet_data(tweet):
    """
    Extract data from a single tweet element.

    Args:
        tweet (BeautifulSoup): The tweet element parsed by BeautifulSoup.

    Returns:
        dict: A dictionary containing tweet data.
    """
    text_parts = []
    for element in tweet.find('div', {"data-testid": "tweetText"}).find_all(['span', 'img']):
        if element.name == 'span':
            text_parts.append(element.get_text(strip=True))
        elif element.name == 'img' and element.has_attr('alt'):
            text_parts.append(element['alt'])

    full_text = " ".join(text_parts)

    account = [i.text for i in tweet.find('div', {"data-testid": "User-Name"}).find_all(['span']) if i.text and i.text != '·']
    username = account[-1]
    name = account[0]

    waktu = tweet.find('time').get('datetime').split('T')
    tgl = waktu[0]
    jam = waktu[1].split('.')[0]

    try:
        link_image_url = re.findall(r'(https://pbs.twimg.com/profile_images/.*?)"', str(tweet))[0]
    except:
        link_image_url = 'https://abs.twimg.com/sticky/default_profile_images/default_profile_normal.png'

    link_post = 'https://x.com' + re.findall(r'href="(/[^\s]*?/status/.*?)"', str(tweet))[0]
    tweet_id = link_post.split('/')[-1]

    agg = [i.text if i.text else "0" for i in tweet.find_all('span', {'data-testid': 'app-text-transition-container'})]
    reply_count = convert_formatted_number(agg[0])
    retweet_count = convert_formatted_number(agg[1])
    like_count = convert_formatted_number(agg[2])
    views_count = convert_formatted_number(agg[3]) if len(agg) > 3 else 0

    mentions = re.findall(r'@\w+', full_text)
    hashtags = re.findall(r'#\w+', full_text)

    return {
        "id": tweet_id,
        "full_text": full_text,
        "link_post": link_post,
        "link_image_url": link_image_url,
        "username": username,
        "name": name,
        "date": tgl,
        "time": jam,
        "favorite_count": like_count,
        "retweet_count": retweet_count,
        "reply_count": reply_count,
        "views_count": views_count,
        "mentions": mentions,
        "hashtags": hashtags
    }

def scroll_and_collect_tweets(driver):
    """
    Scroll the page and collect tweets using BeautifulSoup.

    Args:
        driver (webdriver.Chrome): The Selenium WebDriver instance.

    Returns:
        list: A list of dictionaries containing tweet data.
    """
    results = []
    while True:
        soup = BeautifulSoup(driver.page_source, 'lxml')
        tweets = soup.find_all('article', {"data-testid": "tweet"})
        for tweet in tweets:
            results.append(extract_tweet_data(tweet))
        driver.execute_script("window.scrollBy(0, 1000);")
        time.sleep(2)
    return results

def ingest_to_elasticsearch(results, es_url="http://localhost:9200", index_name="twitter_jakarta"):
    """
    Ingest collected tweet data into Elasticsearch.

    Args:
        results (list): List of tweet data dictionaries.
        es_url (str): The Elasticsearch URL.
        index_name (str): The Elasticsearch index name.
    """
    es = Elasticsearch(es_url)

    if not es.indices.exists(index=index_name):
        es.indices.create(index=index_name)

    df = pd.DataFrame(results).drop_duplicates('id').to_dict(orient='records')
    helpers.bulk(es, [{"_index": index_name, "_source": record} for record in df])

    print(f"Successfully ingested {len(df)} tweets to Elasticsearch.")

def twitter_crawler():
    """
    Main function to crawl Twitter and ingest data into Elasticsearch.
    """
    driver = setup_driver()

    try:
        login_to_twitter(driver)
        results = scroll_and_collect_tweets(driver)
        ingest_to_elasticsearch(results)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        driver.quit()
