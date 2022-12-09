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
from loopring_api import LoopringAPI
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
                existing_collections = conn.execute(
                    f"SELECT collectionid FROM collections WHERE collectionid IN ({collectionId_str})").fetchall()
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
    async def load(cls, nft_id, get_orders=False, get_sellers=False, get_all_data=False):
        self = cls(nft_id)
        self.data = await self.get_nft_info()
        self.get_all_data = get_all_data
        if get_orders:
            self.data['orders'] = await self.get_orders()
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

    async def get_detailed_orders(self, limit=None):
        """
        Gets detailed order information (number owned and total for sale for a given user)
        :param float limit: Price limit of orders to retrieve, given as multiple of current floor price
        :return:
        """
        # Get orders, sort by price, and make a copy of the complete orderbook
        orders = await self.get_orders()
        self.orders = orders.copy()
        orders.sort(key=lambda x: x['pricePerNft'])
        orders_complete = orders.copy()

        # If limit is specified, remove orders above the limit
        if limit is not None:
            min_price = orders[0]['pricePerNft']
            max_price = round(min_price * limit, 4)
            print(f"Limiting results to orders with a max price of {max_price} ETH")
            orders = [order for order in orders if order['pricePerNft'] <= max_price]

        orderbook = dict()

        def wrapper(coro):
            return asyncio.run(coro)

        async def fetch_owned_nfts(idx, order):
            print(f"Fetching owned NFTs for order {idx + 1} of {len(orders)}")
            user = await User.load(address=order['ownerAddress'])
            order['sellerName'] = user.username

            if user.username not in orderbook:
                num_owned = user.get_nft_number_owned(self.get_nft_data(), use_lr=True)
                if num_owned is None:
                    num_owned = 0
            else:
                num_owned = orderbook[user.username][0]['numOwned']

            order['numOwned'] = int(num_owned)

            order['amount'] = int(order['amount']) - int(order['fulfilledAmount'])
            if order['sellerName'] not in orderbook:
                orderbook[order['sellerName']] = [order]
            else:
                orderbook[order['sellerName']].append(order)

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [fetch_owned_nfts(idx, order) for idx, order in enumerate(orders)]
            executor.map(wrapper, futures)

        # Count the number of total sales for each seller in the complete orderbook
        seller_totals = dict()
        for order in orders_complete:
            user = await User.load(address=order['ownerAddress'])
            username = user.get_username()
            if username not in seller_totals:
                seller_totals[username] = int(order['amount'])
            else:
                seller_totals[username] += int(order['amount'])

        # Remove any order where the seller no longer owns the NFT, and append total for sale for each seller
        orderbook_purged = []

        for order in orders:
            if order['numOwned'] == 0:
                continue
            order['totalForSale'] = seller_totals[order['sellerName']]
            orderbook_purged.append(order)

        return orderbook_purged

    async def get_sellers(self):
        """
        Gets all sellers for the NFT
        :return: Dictionary of sellers and their orders
        :rtype: dict
        """
        if len(self.orders) == 0:
            self.orders = await self.get_orders()
        sellers = dict()

        for order in self.orders:
            if order['ownerAddress'] not in sellers:
                sellers.update({order['ownerAddress']: {
                    'amount_for_sale': int(order['amount']),
                    'orders':
                        [{'orderId': order['orderId'],
                          'amount': order['amount'],
                          'price': order['pricePerNft']}]
                }})
            else:
                sellers[order['ownerAddress']]['amount_for_sale'] += int(order['amount'])
                sellers[order['ownerAddress']]['orders'].append(
                    {'orderId': order['orderId'],
                     'amount': order['amount'],
                     'price': order['pricePerNft']})

        def get_username(address):
            user = User(address=address)
            return [address, user.username, 2]

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(get_username, address) for address in sellers.keys()]
            for future in futures:
                sellers[future.result()[0]]['username'] = future.result()[1]
                sellers[future.result()[0]]['number_owned'] = future.result()[2]

        self.sellers = sellers
        return sellers

    '''
    Functions for returning NFT information
    '''

    def get_name(self):
        if self.get_all_data is False and self.from_db is True:
            return self.data['name']
        else:
            return self.data['metadataJson']['name']

    def get_total_number(self):
        return int(self.data['amount'])

    def get_traits(self):
        if self.from_db:
            return self.data['attributes']
        else:
            return self.data['metadataJson']['properties']

    def get_nft_data(self):
        if self.from_db:
            return self.data['nftData']
        else:
            return self.data['loopringNftInfo']['nftData'][0]

    def get_thumbnail(self):
        if self.from_db:
            return self.data['thumbnailUrl']
        else:
            return f"https://www.gstop-content.com/ipfs/{self.data['mediaThumbnailUri'][7:]}"

    def get_minted_datetime(self):
        return self.data['firstMintedAt']

    def get_created_datetime(self):
        return self.data['createdAt']

    def get_updated_datetime(self):
        return self.data['updatedAt']

    def get_state(self):
        if self.get_all_data is False:
            self.get_all_data = True
            self.data = self.get_nft_info()
        return self.data['state']

    def get_url(self):
        return f"https://nft.gamestop.com/token/{self.data['contractAddress']}/{self.data['tokenId']}"

    def get_nftId(self):
        return self.data['nftId']

    def get_collection(self):
        if self.data is None:
            return None

        collection = self.data.get('collectionId')
        if collection is None:
            return None
        else:
            return collection

    def get_lowest_price(self):
        if len(self.orders) == 0:
            self.get_orders()
        return float(self.lowest_price)

    def get_mint_price(self):
        if self.data['mintPrice'] == 0:
            db = NiftyDB()
            mint_price = float(db.get_mint_price(self.data['nftId']))
            if mint_price > 0:
                db.update_mint_price(self.data['nftId'], mint_price)
        else:
            mint_price = self.data['mintPrice']
        return float(mint_price)


class User:
    def __init__(self):
        self.username = None
        self.address = None
        self.accountId = None
        self.created_collections = []
        self.owned_nfts = []
        self.number_of_nfts = 0

    @classmethod
    async def load(cls, username=None, address=None, accountId=None, get_nfts=False, get_collections=False, check_new_name=False):
        self = cls()

        if username is None and address is None and accountId is None:
            return None

        db = NiftyDB()
        lr = LoopringAPI()
        if not check_new_name:
            if address is not None:
                self.accountId, self.address, self.username = db.get_user_info(address=address)
            elif accountId is not None:
                self.accountId, self.address, self.username = db.get_user_info(accountId=accountId)
            else:
                self.accountId, self.address, self.username = db.get_user_info(username=username)

        if self.accountId is None:
            if username is not None:
                await self.get_user_profile(username=username, updateDb=True, check_new_name=check_new_name)
            elif address is not None:
                await self.get_user_profile(address=address, updateDb=True, check_new_name=check_new_name)
            elif accountId is not None:
                address = await lr.get_user_address(accountId)
                await self.get_user_profile(address=address, updateDb=True, check_new_name=check_new_name)

        if get_collections:
            self.number_of_collections = self.get_created_collections()
        if get_nfts:
            self.owned_nfts = self.get_owned_nfts()

        return self

    async def get_user_profile(self, username=None, address=None, updateDb=False, check_new_name=False):
        """
        Gets user profile information from Gamestop API
        :param str username: GamestopNFT Username
        :param str address: Wallet address
        :param bool updateDb: Update database with latest username
        :param bool check_new_name: Update
        :return:
        """
        lr = LoopringAPI()
        db = NiftyDB()

        if username is not None:
            api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getPublicProfile?displayName={username}"
        elif address is not None:
            api_url = f"https://api.nft.gamestop.com/nft-svc-marketplace/getPublicProfile?address={address}"
        else:
            raise Exception("No username or address provided")

        async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
            for i in range(GS_RETRY_ATTEMPTS):
                async with session.get(api_url) as response:
                    if response.status != 200:
                        await asyncio.sleep(GS_RETRY_DELAY)
                        continue
                    else:
                        response = await response.json()
                        break

        if 'userName' in response and response['userName'] is not None:
            self.username = response['userName']
        else:
            self.username = response['l1Address']
        self.address = response['l1Address']

        self.accountId = await lr.get_accountId_from_address(self.address)

        if check_new_name:
            if len(self.username) != 42:
                print(f"Found username for {self.address}: {self.username}, updating database")
                await db.update_username(accountId=self.accountId, username=self.username)
        else:
            if updateDb:
                await db.insert_user_info(accountId=self.accountId, address=self.address, username=self.username,
                                         discord_username=None)

        return

    async def get_owned_nfts_lr(self):
        """
        Gets owned NFTs from Loopring API
        :return: List of NFTs owned by user
        :rtype: list
        """
        lr = LoopringAPI()
        nfts = await lr.get_user_nft_balance(self.accountId)
        return nfts

    async def get_owned_nfts(self, use_lr=True):
        """
        Gets user's owned NFTs
        :param bool use_lr: Use Loopring API to get NFTs
        :return:
        """
        timer_start = time.time()

        # Retrieve all owned NFTs
        if not use_lr:
            cursor = 0
            nft_list = []
            async with aiohttp.ClientSession(headers=GS_HEADERS) as session:
                while True:
                    api_url = ("https://api.nft.gamestop.com/nft-svc-marketplace/getLoopringNftBalances?"
                               f"address={self.address}")
                    if cursor > 0:
                        api_url += f"&cursor={cursor}"

                    async with session.get(api_url) as response:
                        if response.status != 200:
                            await asyncio.sleep(GS_RETRY_DELAY)
                            continue
                        else:
                            response = await response.json()
                    nft_list.extend(response['entries'])
                    if 'nextCursor' not in response.keys():
                        break
                    cursor = int(response['nextCursor'])

        else:
            nft_list = await self.get_owned_nfts_lr()

        done_timer = time.time()
        print(f"Retrieved {len(nft_list)} NFTs for {self.get_username()} in {done_timer - timer_start} seconds")
        self.number_of_nfts = len(nft_list)

        # Load owned NFTs into dictionary
        owned_nfts = {}
        for nft in nft_list:
            if not use_lr:
                owned_nfts[nft['loopringNftData']] = nft['amount']
            else:
                owned_nfts[nft['nftData']] = int(nft['total'])

        # Query DB for Gamestop NFTs
        if not use_lr:
            nftData_list = ', '.join(['\'%s\'' % w['loopringNftData'] for w in nft_list])
        else:
            nftData_list = ', '.join(['\'%s\'' % w['nftData'] for w in nft_list])
        query = f"SELECT nftid, nftdata, name, thumbnailurl FROM nfts WHERE nftdata IN ({nftData_list})"
        nf = NiftyDB()
        with nf.db.connect() as conn:
            result = conn.execute(query).fetchall()

        # Create list of owned Gamestop NFTs with amount owned
        owned_gs_nfts = []
        for nft in result:
            owned_gs_nfts.append({'nftId': nft['nftid'],
                                  'nftData': nft['nftdata'],
                                  'name': nft['name'],
                                  'thumbnailUrl': nft['thumbnailurl'],
                                  'amount': owned_nfts[nft[1]]})

        return owned_gs_nfts

    async def get_nft_number_owned(self, nft_id, use_lr=True):
        """
        Gets qty of a NFT owned by user
        :param nft_id: nftData or nftId
        :param use_lr: Use Loopring API
        :return:
        """
        if len(nft_id) == 36:
            db = NiftyDB()
            nft_info = db.get_nft_data(nft_id)
            if len(nft_info) == 0:
                return 0
            else:
                nft_id = nft_info['nftdata']
        if len(self.owned_nfts) == 0:
            if use_lr:
                self.owned_nfts = await self.get_owned_nfts_lr()
            else:
                self.owned_nfts = await self.get_owned_nfts()
        for nft in self.owned_nfts:
            if use_lr:
                if nft['nftData'] == nft_id:
                    return nft['total']
            else:
                if nft['nftId'] == nft_id:
                    return nft['number_owned']
        return None

    def get_username(self):
        if len(self.username) > 30:
            return f"{self.username[2:6]}...{self.username[-4:]}"
        else:
            return self.username


if __name__ == "__main__":
    pass
