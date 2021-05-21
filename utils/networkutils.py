import os
import json
from json.decoder import JSONDecodeError
from time import time, sleep

import aiohttp
import asyncio
import requests as rq

from dotmap import DotMap
import backoff

import yaml as yml
from utils.helperutils import HelperUtils


class NetworkUtils:

    utils = HelperUtils()
    network_config = utils.get_network_config()

    def __init__(self):

        self.__responses = []

    @backoff.on_exception(backoff.expo, (rq.RequestException, json.decoder.JSONDecodeError), max_tries=network_config['max_tries'])
    async def __get_request(self, session, url):
        async with session.get(url) as res:
            print(f"GET {url}")
            raw_response = await res.read()
            response = DotMap(json.loads(raw_response))
            return response

    async def __get_requests_for_batch(self, batch):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for url in batch:
                tasks.append(asyncio.ensure_future(
                    self.__get_request(session, url)))
            original_response = await asyncio.gather(*tasks)
            self.__responses.extend(original_response)

    def get_response(self, urls, batch_size):
        self.__responses = []
        batches = self.utils.get_list_of_batches(urls, batch_size)
        count = 1
        for batch in batches:
            print(f"Get Batch => {count}")
            asyncio.run(self.__get_requests_for_batch(batch))
            sleep(1)
            count += 1
        return self.__responses
