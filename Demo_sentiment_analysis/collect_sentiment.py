import time
from transformers import pipeline, AutoModelForSequenceClassification, AutoTokenizer
from googlesearch import search
from bs4 import BeautifulSoup
import requests

# Load the financial sentiment analysis model and tokenizer
model_name = "yiyanghkust/finbert-tone"
model = AutoModelForSequenceClassification.from_pretrained(model_name)
tokenizer = AutoTokenizer.from_pretrained(model_name)
sentiment_analysis = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)


def get_sentiment_score(text):
    # Split the text into smaller segments
    text_segments = [text[i:i + 512] for i in range(0, len(text), 512)]

    sentiment_score_sum = 0.0
    for segment in text_segments:
        result = sentiment_analysis(segment)
        sentiment_score_sum += result[0]['score']

    # Calculate average sentiment score for the entire text
    average_sentiment_score = sentiment_score_sum / len(text_segments)

    return average_sentiment_score


def collect_sentiment_data(query, threshold=2):
    print("inside collect_sentiment_data")
    sentiment_data = []
    urls = search(query, num_results=5)  # Fetch the search results with googlesearch

    for url in urls:
        try:
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            text = soup.get_text()
            sentiment_score = get_sentiment_score(text)
            sentiment_data.append((url, sentiment_score))

        except Exception as e:
            print("Error occurred:", str(e))
            print("Skipping URL:", url)

    return sentiment_data


if __name__ == "__main__":
    query = "Is Teslas future bright?"
    sentiment_data = collect_sentiment_data(query, threshold=5)

    # Calculate the total sentiment mean from these five websites
    total_sentiment_mean = sum(score for _, score in sentiment_data) / len(sentiment_data)

    # Display collected sentiment data
    for url, score in sentiment_data:
        print("URL:", url)
        print("Sentiment Score:", score)
        print("-" * 50)

    print("Total Sentiment Mean from Five Websites:", total_sentiment_mean)
