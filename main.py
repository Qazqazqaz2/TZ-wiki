import bz2
import xml.etree.ElementTree as ET
import html_to_json
from jsonformatter import JsonFormatter

import html
import json
import re

import threading
import asyncio

class ToJson:
    def __init__(self):
        self.bz2_dump_file = 'src/wiki11.bz2'
        self.articles = []
        asyncio.run(self.handler_dump())
        self.load_data()
        self.test_var = 0

    def clear(self, raw: str):
        raw = str(raw).replace(r"\n", ' ')
        if '_value' in raw:
            raw = json.loads(raw)
        return raw

    async def handler_dump(self):
        # Распаковка BZ2-файла
        with bz2.open(self.bz2_dump_file, 'rb') as f:
            loop = asyncio.get_event_loop()
            tasks = []
            for event, elem in ET.iterparse(f):
                if elem.tag.endswith("page"):
                    task = loop.run_in_executor(None, self.handler_pages, elem)
                    tasks.append(task)
                await asyncio.gather(*tasks)
                if len(tasks)>200:
                    break


    def handler_pages(self, elem):
        base_name = "{http://www.mediawiki.org/xml/export-0.10/}"

        title = elem.find(f'{base_name}title').text
        text = self.clear(html.unescape(elem.find(f'{base_name}revision/{base_name}text').text))
        if not ("_value" in str(text)):
            text = html_to_json.convert(text)

        data = {'title': title, "text": text}

        self.articles.append(data)

    def load_data(self):
        with open('src/output_json.json', 'w', encoding='utf-8') as json_file:
            articles = json.dumps(self.articles, indent=4)
            json_file.write(str(articles))

ToJson()