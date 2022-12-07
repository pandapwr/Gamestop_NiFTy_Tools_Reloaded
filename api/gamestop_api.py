import aiohttp
import json
import logging
from datetime import datetime
import traceback

GS_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
}

class GamestopApi:
    def __init__(self):
        pass

    async def get_newest_collections(self, limit=48):
        """
        Gets the newest collections
        :param limit:
        :returns:
        """
        return await self.get_collections(limit=limit, offset=0, sort="created", sort_order="desc")

    async def get_collections(self, get_all=True, limit=500, offset=0, sort="created", sort_order="asc"):
        """
        Gets all collections
        :param bool get_all: Fetch all collections
        :param int limit: Number of collections to fetch
        :param int offset: Offset to start fetching from
        :param str sort: Sort by ``created``, ``updated``
        :param str sort_order: Sort order ``asc`` or ``desc``
        :returns: Dictionary of collections
        :rtype: dict
        """

        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            if get_all:
                api_url = "https://api.nft.gamestop.com/nft-svc-marketplace/getCollectionsPaginated?limit=0&sortBy=random"
                async with session.get(api_url) as response:
                    if response.status == 200:
                        response = await response.json()
                        total_num = response['totalNum']
                    else:
                        logging.error(f"Error getting total number of collections: {response.status} {response.reason}")
                        return None

                remaining = total_num
                collections_list = []
                while remaining > 0:
                    api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getCollectionsPaginated?limit=500&offset={offset}&sortBy={sort}&sortOrder={sort_order}"
                    async with session.get(api_url) as response:
                        if response.status == 200:
                            response = await response.json()
                            data = response.json()['data']
                            collections_list.extend(data)
                            offset += limit
                            remaining -= limit
                        else:
                            return None

            else:
                api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getCollectionsPaginated?"
                           f"limit={limit}&offset={offset}&sortBy={sort}&sortOrder={sort_order}")
                async with session.get(api_url) as response:
                    if response.status == 200:
                        collections_list = await response.json()
                    else:
                        return None

            for collection in collections_list:
                collection['createdAt'] = datetime.strptime(collection['createdAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
                collection['updatedAt'] = datetime.strptime(collection['updatedAt'], "%Y-%m-%dT%H:%M:%S.%fZ")

            return collections_list

