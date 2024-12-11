import os
import re
import json
import time
import uuid
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service as ChromeService
from elasticsearch import Elasticsearch, helpers
from newspaper import Article
from utils.gemini import GeminiConnector

# Initialize GeminiConnector
gemini_connector = GeminiConnector()

# Selenium WebDriver setup
chrome_service = ChromeService(ChromeDriverManager().install())
driver = webdriver.Chrome(service=chrome_service)

# Constants
KEYWORDS = [
    'buruh jakarta', 'dki jakarta', 'gubernur jakarta', 'infrastruktur jakarta',
    'jakarta', 'jakarta barat', 'jakarta bencana', 'jakarta ekonomi',
    'jakarta gubernur', 'jakarta kesehatan', 'jakarta masalah', 'jakarta pasar',
    'jakarta pilkada', 'jakarta pusat', 'jakarta selatan', 'jakarta terkini',
    'jakarta timur', 'jakarta utara', 'kriminal jakarta', 'lalu lintas jakarta',
    'pajak jakarta', 'pemerintah jakarta', 'pemukiman jakarta', 'penipuan jakarta',
    'pilkada jakarta', 'upah jakarta'
]

RSP = []  # List to store URLs

# Function to scrape URLs using Selenium
def scrape_urls():
    """
    Scrape news article URLs from DuckDuckGo based on defined keywords.
    """
    global RSP
    for keyword in KEYWORDS:
        print(f"Scraping for keyword: {keyword}")
        search_url = f"https://duckduckgo.com/?q={'+'.join(keyword.split())}&ia=news"
        driver.get(search_url)

        try:
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, 'a.result--more'))
            )
        except Exception as e:
            print(f"Error fetching results for {keyword}: {e}")

        links = set(re.findall(r'href=["\'](https?://[^"\']+)', driver.page_source))
        links = [link for link in links if 'duckduckgo' not in link]
        RSP.extend(links)

# Function to retrieve and parse an article
def get_article(url):
    """
    Retrieve and parse a web article.

    Args:
        url (str): The URL of the article.

    Returns:
        Article: Parsed article object, or None if fetching fails.
    """
    try:
        driver.get(url)
        time.sleep(3)
        article = Article(url)
        article.set_html(driver.page_source)
        article.parse()
        return article
    except Exception as e:
        print(f"Error fetching article: {e}")
        return None

# Function to generate bulk prompts for Gemini
def generate_bulk_prompt(news_list):
    prompt = "Given a list of twitter post, predict the following categories for each item: topic classification, urgency level, sentiment, target audience, affected region and Capture contextual or descriptive terms that support the main theme. Output should be in JSON format with each article's uuid included. \n\nGuidelines:\n\n"
    prompt += """
1. Topic Classification: Choose one of the following categories based on the main issue addressed:
   - Social and Economy
   - Infrastructure and Transportation
   - Public Health
   - Environment and Disaster
   - Safety and Crime
   - Government and Public Policy
   - Technology and Innovation
   - City Planning and Housing
   - Education and Culture
   - Tourism and Entertainment
   - Ecology and Green Spaces

2. Urgency Level: Provide a score from 0 to 100, where 100 indicates the highest urgency. This score represents how quickly the issue needs to be addressed to minimize its impact.

3. Sentiment: Classify sentiment as one of the following:
   - Positive
   - Neutral
   - Negative

4. Target Audience: Identify the primary groups affected by or interested in the news. Use the following categories:
   - Traditional Market Vendors
   - Business Owners
   - Local Government
   - General Public
   - Healthcare Workers
   - Environmental Agencies
   - Public Transport Users
   - Tourists
   - Students and Educators
   - Technology Enthusiasts
   - Safety and Security Agencies

5. Affected Region: Classify the region affected by the news as one of the following:
   - DKI Jakarta (for issues that generally affect all of Jakarta)
   - South Jakarta
   - North Jakarta
   - East Jakarta
   - West Jakarta
   - Central Jakarta
6. Contextual Keywords: Words and phrases that represent key themes, brands, products, individuals, locations, or technical specifications

Return the output in JSON format
Output:
[{
"id":<string>,
"topic_classification":<string>,
"urgency_level":<0-100>,
"sentiment":<string>,
"target_audience":<list of target>,
"affected_region":<string>,
"contextual_content": "This is a brief summary of the content related to the topic, capturing the main ideas and context. using indonesia language",
"contextual_keywords":<Top 5 list of contextual keyword or phrases in Indonesia Language>
}]

Process each news article separately using its uuid as an identifier.

News Articles:
"""
    prompt += f"""{news_list}"""
    return prompt


# Function to chunk a list
def chunk_list(data, chunk_size):
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

# Function to enrich news using Gemini
def enrich_news_with_gemini(news_chunks):
    enriched_data = []
    for chunk in tqdm(news_chunks, desc="Enriching news with Gemini"):
        prompt = generate_bulk_prompt(chunk)
        gemini_response = gemini_connector.generate_content(prompt)

        # Extract JSON from response
        extracted_json = re.findall(r'\[.*?\]', gemini_response, flags=re.I | re.S)
        if extracted_json:
            json_data = json.loads(extracted_json[0])

            # Clean contextual content
            for item in json_data:
                item["contextual_content"] = item["contextual_content"].replace('"', '')
            enriched_data.extend(json_data)
    return enriched_data

# Function to ingest data into Elasticsearch
def ingest_to_elasticsearch(data, index_name="news_jakarta"):
    es = Elasticsearch("http://localhost:9200")

    def generate_bulk_data(data):
        for record in data:
            yield {
                "_index": index_name,
                "_id": record["id"],
                "_source": record
            }

    try:
        helpers.bulk(es, generate_bulk_data(data))
        print("Data successfully ingested to Elasticsearch.")
    except Exception as e:
        print(f"Error ingesting data to Elasticsearch: {e}")

# Main execution flow
def main():
    # Step 1: Scrape URLs
    scrape_urls()

    # Step 2: Process articles into data
    news_data = []
    for url in tqdm(set(RSP), desc="Processing articles"):
        article = get_article(url)
        if article:
            article.nlp()
            news_data.append({
                'id': str(uuid.uuid5(uuid.NAMESPACE_DNS, url)),
                'title': article.title,
                'url': url,
                'description': article.summary,
                'content': article.text,
                'publish_at': str(article.publish_date or datetime.now()),
                'image_url': article.top_image
            })

    # Step 3: Enrich data with Gemini
    news_chunks = chunk_list(news_data, 20)
    enriched_news = enrich_news_with_gemini(news_chunks)

    # Step 4: Ingest data to Elasticsearch
    original_data = pd.DataFrame(news_data)
    enriched_df = pd.DataFrame(enriched_news)

    df_ingest = original_data.merge(enriched_df, on="id", how="inner").to_dict(orient="records")

    # Ensure "publish_at" is properly formatted
    for news in df_ingest:
        news["publish_at"] = datetime.fromisoformat(news["publish_at"])

    ingest_to_elasticsearch(df_ingest)

    print("Process completed.")

if __name__ == "__main__":
    main()
    driver.quit()
