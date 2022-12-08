import aiohttp
import json
import asyncio
import logging
from datetime import datetime
from nifty_database import NiftyDB
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
                            data = response['data']
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

    async def save_collections(self):
        """
        Saves all collections to the database
        :returns:
        """
        nf = NiftyDB()
        collections = await self.get_collections()

        if collections is not None:
            # Query DB for existing collections
            collectionId_str = ', '.join(['\'%s\'' % collection['collectionId'] for collection in collections])
            with nf.db.connect() as conn:
                existing_collections = conn.execute(f"SELECT collectionid FROM collections WHERE collectionid IN ({collectionId_str})").fetchall()
                existing_collections_list = [collection['collectionid'] for collection in existing_collections]

            # Check which collections are new
            new_collections = []
            for collection in collections:
                if collection['collectionId'] not in existing_collections_list:
                    new_collections.append(collection)

            # Insert new collections
            for collection in new_collections:
                async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
                    api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getCollectionStats?collectionId={collection['collectionId']}"
                    async with session.get(api_url) as response:
                        if response.status == 200:
                            response = await response.json()
                            numNfts = response['itemCount']

                    print(f"Adding {collection['name']} ({collection['collectionId']}) to database")
                    if collection['bannerUri'] is None:
                        bannerUri = ""
                    else:
                        bannerUri = f"https://static.gstop-content.com/{collection['bannerUri'][7:]}"

                    if collection['avatarUri'] is None:
                        avatarUri = ""
                    else:
                        avatarUri = f"https://static.gstop-content.com/{collection['avatarUri'][7:]}"

                    if collection['tileUri'] is None:
                        tileUri = ""
                    else:
                        tileUri = f"https://static.gstop-content.com/{collection['tileUri'][7:]}"

                    nf.insert_collection(collection['collectionId'],
                                         collection['name'],
                                         collection['slug'],
                                         collection['creator']['displayName'],
                                         collection['description'],
                                         bannerUri,
                                         avatarUri,
                                         tileUri,
                                         int(collection['createdAt'].timestamp()),
                                         numNfts,
                                         collection['layer'])



if __name__ == "__main__":
    api = GamestopApi()
    asyncio.run(api.save_collections())