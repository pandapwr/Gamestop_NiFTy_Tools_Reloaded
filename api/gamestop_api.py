import aiohttp
import requests
import json
import asyncio
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from config import *
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

class NftCollection:
    def __init__(self, collectionId):
        self.collectionId = collectionId
        self.stats = None
        self.metadata = None
        self.collection_nfts = None

    @classmethod
    async def load(cls, collectionId, get_collection_metadata=True, get_collection_nfts=False):
        self = cls(collectionId)
        self.stats = await self.get_collection_stats()
        if get_collection_metadata:
            self.metadata = await self.get_collection_metadata()
        if get_collection_nfts:
            self.collection_nfts = await self.get_collection_nfts()
        return self

    def _add_datetime(self, data):

        for nft in data:
            try:
                nft['firstMintedAt'] = datetime.strptime(nft['firstMintedAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
            except:
                pass

            try:
                nft['createdAt'] = datetime.strptime(nft['createdAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
            except:
                pass

            try:
                nft['updatedAt'] = datetime.strptime(nft['updatedAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
            except:
                pass

        return data

    async def get_collection_stats(self):
        """
        Gets collection stats
        :return: Collection stats
        :rtype: dict
        """
        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getCollectionStats?collectionId={self.collectionId}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response
                else:
                    logging.error(f"Error getting collection stats for {self.collectionId}: {response.status}")
                    return None

    async def get_collection_metadata(self):
        """
        Gets collection metadata
        :return: Collection metadata
        :rtype: dict
        """
        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getCollections?collectionId={self.collectionId}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response[0]
                else:
                    logging.error(f"Error getting collection metadata for {self.collectionId}: {response.status}")
                    return None

    async def get_collection_nfts(self, get_all=True, limit=500, offset=0, sort="created", sort_order="desc"):
        """
        Gets collection NFTs
        :param bool get_all: Retrieve all NFTs in collection (default: True)
        :param int limit: Limit of NFTs to retrieve (default: 500)
        :param int offset: Offset of NFTs to retrieve (default: 0)
        :param str sort: Sort by (default: created)
        :param str sort_order: Sort order (default: desc)
        :return: Dictionary of NFTs in collection
        :rtype: dict
        """
        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            # Get the total number of items in the collection
            api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNftsPaginated?"
                       f"nativeLayer=Loopring&limit=0&collectionId={self.collectionId}")
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    num_items = response['totalNum']
                    print(f"{self.get_name()} has {num_items} items, grabbing NFTs now...")
                else:
                    logging.error(f"Error getting collection NFTs for {self.collectionId}: {response.status}")
                    return None

            # Get the NFTs
            if get_all:
                offsets = [i for i in range(0, num_items, 500)]
                nfts = []

                def wrapper(coro):
                    return asyncio.run(coro)

                async def get_nft_batch(offset):
                    async with aiohttp.ClientSession(headers=GS_HEADERS) as req:
                        api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNftsPaginated?"
                                   f"nativeLayer=Loopring&limit=500&offset={offset}&collectionId={self.collectionId}"
                                   f"&sortBy={sort}&sortOrder={sort_order}")
                        async with req.get(api_url) as data:
                            if data.status == 200:
                                data = await data.json()
                                return data['data']

                with ThreadPoolExecutor(max_workers=16) as executor:
                    batches = [get_nft_batch(offset) for offset in offsets]
                    for r in executor.map(wrapper, batches):
                        nfts.extend(r)
            else:
                api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNftsPaginated?"
                           f"nativeLayer=Loopring&limit={limit}&offset={offset}&collectionId={self.collectionID}"
                           f"&sortBy={sort}&sortOrder={sort_order}")
                async with session.get(api_url) as response:
                    if response.status == 200:
                        response = await response.json()
                        nfts = response['data']
                    else:
                        logging.error(f"Error getting collection NFTs for {self.collectionId}: {response.status}")
                        return None

            if len(nfts) == 0:
                return None

            return self._add_datetime(nfts)


    '''
        Functions for returning collection information
    '''

    def get_name(self):
        """
        Gets collection name
        :return: Collection name
        :rtype: str
        """
        return self.metadata['name']

    def get_contract_address(self):
        """
        Gets collection contract address
        :return: Collection contract address
        :rtype: str
        """
        return self.metadata['contract']

    def get_description(self):
        """
        Gets collection description
        :return: Collection description
        :rtype: str
        """
        return self.metadata['description']

    def get_thumbnail_uri(self):
        """
        Gets collection thumbnail URI
        :return: Collection thumbnail URI
        :rtype: str
        """
        return self.metadata['thumbnail_uri']

    def get_banner_uri(self):
        """
        Gets collection banner URI
        :return: Collection banner URI
        :rtype: str
        """
        return self.metadata['banner_uri']

    def get_avatar_uri(self):
        """
        Gets collection avatar URI
        :return: Collection avatar URI
        :rtype: str
        """
        return self.metadata['avatar_uri']

    def get_tile_uri(self):
        """
        Gets collection tile URI
        :return: Collection tile URI
        :rtype: str
        """
        return self.metadata['tile_uri']

    def get_item_count(self):
        """
        Gets collection item count
        :return: Collection item count
        :rtype: int
        """
        return self.stats['itemCount']

    def get_floor_price(self):
        """
        Gets collection floor price
        :return: Collection floor price
        :rtype: float
        """
        return self.stats['floorPrice']

    def get_total_volume(self):
        """
        Gets collection total volume
        :return: Collection total volume
        :rtype: float
        """
        return self.stats['totalVolume']

    def get_for_sale(self):
        """
        Gets collection for sale count
        :return: Collection for sale count
        :rtype: int
        """
        return self.stats['forSale']

    async def get_nftId_list(self):
        """
        Gets list of NFT IDs in collection
        :return: List of NFT IDs in collection
        :rtype: list
        """
        if self.collection_nfts is None:
            self.collection_nfts = await self.get_collection_nfts()
        if self.collection_nfts is None:
            return None
        else:
            return [nft['nftId'] for nft in self.collection_nfts]


class Nft:
    def __init__(self, nft_id):
        self.nft_id = nft_id
        self.lowest_price = 0
        self.on_gs_nft = False
        self.from_db = False
        self.data = None
        self.get_all_data = False
        self.orders = []
        self.history = []
        self.sellers = []

    @classmethod
    async def load(cls, nft_id, get_orders=False, get_history=False, get_sellers=False, get_all_data=False):
        self = cls(nft_id)
        self.data = await self.get_nft_info()
        self.get_all_data = get_all_data
        if get_orders:
            self.data['orders'] = await self.get_orders()
        if get_history:
            self.data['history'] = await self.get_history()
        if get_sellers:
            self.data['sellers'] = await self.get_sellers()

        return self

    def _add_datetime(self, data, from_timestamp=False):
        if from_timestamp:
            data['createdAt'] = datetime.fromtimestamp(data['createdAt'])
            data['updatedAt'] = datetime.fromtimestamp(data['updatedAt'])
            data['firstMintedAt'] = datetime.fromtimestamp(data['firstMintedAt'])
        else:
            data['createdAt'] = datetime.strptime(data['createdAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
            data['updatedAt'] = datetime.strptime(data['updatedAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
            data['firstMintedAt'] = datetime.strptime(data['firstMintedAt'], "%Y-%m-%dT%H:%M:%S.%fZ")
        return data

    async def get_nft_info(self):
        """
        Gets NFT information from Database or API
        :return: NFT data
        :rtype: dict
        """
        # Query database to see if NFT is already cached
        db = NiftyDB()
        db_data = await db.get_nft_data(self.nft_id)

        if db_data is not None and self.get_all_data is False:
            data = dict()
            data['nftId'] = db_data['nftId']
            data['name'] = db_data['name']
            data['nftData'] = db_data['nftData']
            data['tokenId'] = db_data['tokenId']
            data['contractAddress'] = db_data['contractAddress']
            data['creatorEthAddress'] = db_data['creatorEthAddress']
            data['attributes'] = json.loads(db_data['attributes'])
            data['amount'] = db_data['amount']
            data['collectionId'] = db_data['collectionId']
            data['createdAt'] = db_data['createdAt']
            data['updatedAt'] = db_data['updatedAt']
            data['firstMintedAt'] = db_data['firstMintedAt']
            data['thumbnailUrl'] = db_data['thumbnailUrl']
            data['mintPrice'] = db_data['mintPrice']
            self.on_gs_nft = True
            self.from_db = True

            return self._add_datetime(data, from_timestamp=True)

        else:
            # Get NFT data from API
            async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
                if len(self.nft_id) > 100:
                    api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNft?"
                               f"tokenIdAndContractAddress={self.nft_id}")
                else:
                    api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNft?"
                               f"nftId={self.nft_id}")
                for i in range(GS_RETRY_ATTEMPTS):
                    async with session.get(api_url) as response:
                        if response.status == 200:
                            response = await response.json()
                            break
                        else:
                            await asyncio.sleep(GS_RETRY_DELAY)

                if "nftId" in response:
                    self.on_gs_nft = True
                    response = self._add_datetime(response)
                    response['createdAt'] = time.mktime(response['createdAt'].timetuple())
                    response['updatedAt'] = time.mktime(response['updatedAt'].timetuple())
                    response['firstMintedAt'] = time.mktime(response['firstMintedAt'].timetuple())
                    thumbnailUrl = f"https://www.gstop-content.com/ipfs/{response['mediaThumbnailUri'][7:]}"
                    if 'metadataJson' in response and 'attributes' in response['metadataJson']:
                        attributes = json.dumps(response['metadataJson']['properties'])
                    else:
                        attributes = json.dumps({})
                    db.insert_nft(response['nftId'], response['loopringNftInfo']['nftData'][0], response['tokenId'],
                                  response['contractAddress'], response['creatorEthAddress'],
                                  response['metadataJson']['name'],
                                  response['amount'], attributes, response['collectionId'],
                                  response['createdAt'], response['firstMintedAt'], response['updatedAt'], thumbnailUrl,
                                  0)
                    return response
                else:
                    return None

    async def get_orders(self):
        """
        Gets NFT orders from Database or API
        :return: NFT orders
        :rtype: list
        """
        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getNftOrders?"
                       f"nftId={self.nft_id}")
            async with session.get(api_url) as response:
                for i in range(GS_RETRY_ATTEMPTS):
                    if response.status != 200:
                        await asyncio.sleep(GS_RETRY_DELAY)
                        continue

                    response = await response.json()
                    lowest_price = 10000000
                    for order in response:
                        order['pricePerNft'] = float(order['pricePerNft']) / 10 ** 18
                        if order['pricePerNft'] < lowest_price:
                            lowest_price = order['pricePerNft']
                        order['createdAt'] = datetime.strptime(order['createdAt'], "%Y-%m-%dT%H:%M:%S.%f%z")
                        order['updatedAt'] = datetime.strptime(order['updatedAt'], "%Y-%m-%dT%H:%M:%S.%f%z")
                        order['validUntil'] = datetime.fromtimestamp(order['validUntil'])
                    self.orders = response
                    if lowest_price == 100000000:
                        self.lowest_price = 0
                    else:
                        self.lowest_price = lowest_price
                    break

                return response





async def main():
    time_start = time.time()
    collection = await NftCollection.load("36fab6f7-1e51-49d9-a0be-39343abafd0f")
    nfts = await collection.get_nftId_list()
    print(f"Retrieved {len(nfts)} NFTs from {collection.get_name()}")
    print(f"Time elapsed: {time.time() - time_start}")

if __name__ == "__main__":
    asyncio.run(main())



