#!/usr/local/bin/python3

import json
from os import environ
import os
from pathlib import Path
import shutil
import sys
from typing import Dict, List
from functools import cache
from urllib.parse import urlparse
import requests
import re


database_ids = environ['database_ids'].split(',')
integration_token = environ['integration_token']
cache_dir = Path(environ['alfred_workflow_cache'])
icons_dir = cache_dir.joinpath('icons')
alfred_json_cache_path = cache_dir.joinpath('cache.json')


def main():
    """
    Retrieve pages (title, url, icon) from Notion databases to display in Alfred.

    The Alfred workflow reads the cache file by itself, so this script only
    returns items that weren't already available in the cache file (and updates
    the cache file).
    """

    delete_icon_cache_if_necessary()
    make_sure_the_cache_directories_exist()
    alfred_items = []
    for database_id in database_ids:
        alfred_items.extend(
            talk_to_notion_api_and_create_alfred_items(database_id))
    write_alfred_items_to_stdout(difference_with_cache(alfred_items))
    update_cache_file(alfred_items)


def delete_icon_cache_if_necessary():
    if '--refresh-icons' in sys.argv[1:] and icons_dir.exists():
        shutil.rmtree(icons_dir)


def make_sure_the_cache_directories_exist():
    cache_dir.mkdir(exist_ok=True, parents=True)
    icons_dir.mkdir(exist_ok=True, parents=True)


def talk_to_notion_api_and_create_alfred_items(database_id: str) -> List[Dict]:
    alfred_items = []

    pagination_cursor = 0

    while True:
        database_title = retrieve_database_title(database_id)

        response = get_session().post(
            f'https://api.notion.com/v1/databases/{database_id}/query', data={'start_cursor': pagination_cursor})
        if response.status_code != 200:
            exit_with_error()

        response_json = response.json()
        notion_pages = response_json['results']

        if len(notion_pages) == 0:
            break

        title_property_name = find_title_property_name(
            notion_page_json=notion_pages[0])

        for page_dict in notion_pages:
            alfred_items.append(
                notion_page_to_alfred_item(
                    notion_page_dict=page_dict,
                    title_property_name=title_property_name,
                    database_title=database_title,
                )
            )

        if not response_json['has_more']:
            break
        pagination_cursor = response_json['next_cursor']

    return alfred_items


def retrieve_database_title(database_id):
    response = get_session().get(
        f'https://api.notion.com/v1/databases/{database_id}')
    if response.status_code != 200:
        exit_with_error()
    return to_plain_text(response.json()['title'])


def write_alfred_items_to_stdout(items: list):
    items_json = json.dumps({'items': items})
    sys.stdout.write(items_json)
    sys.stdout.flush()


def update_cache_file(alfred_items: list):
    updated_cache_json = json.dumps({'items': alfred_items})
    with open(alfred_json_cache_path, 'w') as output_file:
        output_file.write(updated_cache_json)


def exit_with_error(msg: str = 'error'):
    sys.exit(msg)


@cache
def get_session() -> requests.Session:
    session = requests.Session()
    session.headers['Authorization'] = 'Bearer ' + integration_token
    return session


def find_title_property_name(notion_page_json: dict):
    """ 
    example `result_json` format:
    ```
    'Subject': {
        'id': 'title',
        'title': [{}, ..],
        'type': 'title'
    }
    ```
    """
    for property_name, property_info in notion_page_json['properties'].items():
        if property_info['type'] == 'title':
            return property_name
    exit_with_error('expected title property from Notion API, none found')


def to_plain_text(rich_texts: List[dict]):
    """
    example `richt_texts` format:
    ```
    [{
        'annotations': {...},
        'href': None,
        'plain_text': 'Home appliances',
        'text': {'content': 'Home appliances',
                'link': None},
        'type': 'text'
    }, ...]
    ```
    """
    return ''.join([rich_text['plain_text'] for rich_text in rich_texts])


def download_icon_and_return_local_path(page_id: str, icon_json: dict) -> str:
    """
    example `icon_json` format:
    ```
    {
      'file': {
          'url': 'https://url.to/image.png',
          ...
      },
      'type': 'file'
    }
    ```
    """
    if icon_json is None:
        return None
    if 'file' not in icon_json:
        return None

    icon_url = icon_json['file']['url']

    file_extension = urlparse(icon_url).path.split('.')[-1]
    local_icon_path = icons_dir.joinpath(f'{page_id}.{file_extension}')
    if not local_icon_path.exists():
        # We don't want to download icons that are already cached.
        # And we do want to run the downloads in the background, 
        # so that you can back out of Alfred whenever you want.
        os.system(f'/usr/bin/curl -o "{local_icon_path}" "{icon_url}" &')
    return str(local_icon_path.absolute())


def notion_page_to_alfred_item(notion_page_dict, title_property_name, database_title) -> dict:
    page_id = notion_page_dict['id']
    page_title = to_plain_text(
        notion_page_dict['properties'][title_property_name]['title'])
    page_url = re.sub(r'^https', 'notion', notion_page_dict['url'])

    return {
        'uid': page_id,
        'title': page_title,
        'subtitle': database_title,
        'arg': page_url,
        'icon': {
            'path': download_icon_and_return_local_path(page_id=page_id, icon_json=notion_page_dict['icon'])
        },
        'autocomplete': page_title,
    }


def difference_with_cache(alfred_items: list):
    if not alfred_json_cache_path.exists():
        return alfred_items

    with open(alfred_json_cache_path, 'r') as cache_file:
        cached_results = json.load(cache_file)
    cached_page_titles = set([result['title']
                             for result in cached_results['items']])

    return [item for item in alfred_items if item['title'] not in cached_page_titles]


main()
