from argparse import ArgumentParser
from posixpath import join as urljoin
from bs4 import BeautifulSoup
from posting import Posting
import configparser
import json
import os
import re


def parse_config(config_file="config/config.ini"):
    parser = ArgumentParser()
    parser.add_argument(
        "--config", type=str, choices=["ANALYST", "DEV"], default="ANALYST", help="Path to web data"
    )
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(config_file)
    default_config = config["DEFAULT"]
    data_config = config[args.config]

    return default_config, data_config


def generate_result_pages(disk_locs, static_dir, generated_result_dir, query=None):
    generated_pages = []
    result_dir = os.path.join(static_dir, generated_result_dir)
    os.makedirs(result_dir, exist_ok=True)

    for idx, disk_loc in enumerate(disk_locs):
        with open(disk_loc, encoding="utf8") as f:
            content = json.load(f)
        content = content["content"]
        # highlighting query if provided
        if query:
            content = highlight_html(content, query)

        file_name = f"result_{idx:02}.html"
        path = os.path.join(result_dir, file_name)
        with open(path, "w", encoding="utf8") as f:
            f.write(content)

        generated_pages.append(urljoin(generated_result_dir, file_name))

    return generated_pages


def highlight_html(html_str, query):
    soup = BeautifulSoup(html_str, "html.parser")
    query = query.split()
    query = list(set(query))

    # Pattern for complete match for word, not partial match
    patterns = [re.compile(f"(^|[^a-zA-Z\d]+|\s+)({word})($|[^a-zA-Z\d]+|\s+)", flags=re.IGNORECASE) for word in query]

    for pattern in patterns:
        for tag in soup.find_all(text=pattern):
            highlighted = re.sub(pattern, "\g<1><mark>\g<2></mark>\g<3>", tag)
            tag.replace_with(BeautifulSoup(highlighted, "html.parser"))

    return str(soup)


# For consistency between inverted index processing and query processing
def process_token(token: str, stemmer):
    if len(token) == 1 and not token.isalnum():  # Removing separator
        return None
    token = token.lower()
    if stemmer:
        token = stemmer.stem(token)
    # if token in stopwords: # uncomment to exclude stop words
    #     return None
    return token


# Parse position of posting
def parse_multiple_posting(posting_str: str):
    result = []
    posting_str = posting_str.splitlines()
    for i in range(0, len(posting_str), 2):
        posting = Posting()
        posting.parse_from_str(posting_str[i:i + 2])
        result.append(posting)
    return result