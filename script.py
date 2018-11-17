import json
import time

import pywren
import wikipedia

from db_utils import (initialize_db, add_node, get_links,
                      is_article_in_nodes_table, add_edge)


ARTICLES_FILE_PATH = 'articles.json'
ENCODING = 'UTF-8-sig'


def fetch_links(article_title: str):
    try:
        page = wikipedia.page(article_title)
        links = page.links
        return article_title, set(links)
    except Exception:
        pass


def create_nodes(article_title: str) -> None:
    article_title, links = fetch_links(article_title)
    add_node(article_title, links)


def create_edges(article_title: str) -> None:
    links = get_links(article_title)

    for link in links:
        if is_article_in_nodes_table(link):
            add_edge(article_title, link)


def main():
    initialize_db()
    wren_executor = pywren.default_executor()

    start_time = time.time()
    with open(ARTICLES_FILE_PATH, encoding=ENCODING) as articles_file:
        data = json.loads(articles_file.read())
        futures = wren_executor.map(create_nodes, data['titles'])
        pywren.wait(futures)
        futures = wren_executor.map(create_edges, data['titles'])
        pywren.wait(futures)
    end_time = time.time()

    print('Duration: {}'.format(end_time - start_time))


if __name__ == '__main__':
    main()
