import aiohttp
import json
import logging
from ratelimit import limits, sleep_and_retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import traceback
import asyncio
import time
from nifty_database import NiftyDB
#from gamestop_api import User
from random import choice
from config import *


class LoopringAPI:
    def __init__(self):
        self.headers = {
            'Accept': 'application/json',
            'X-API-KEY': choice(LR_API_KEYS)
        }
        self.lr = None

    def __exit__(self, type, value, traceback):
        self.lr.close()

    def close(self):
        self.lr.close()

    async def get_lrc_price(self):
        """
        Get the current LRC price in USD from Loopring Exchange
        :return:
        """
        api_url = "https://api3.loopring.io/api/v3/ticker?market=LRC-USDT"
        async with self.lr.get(api_url) as response:
            if response.status == 200:
                response = await response.json()
                return float(response['tickers'][0][7])
            else:
                logging.error(f"Error getting LRC price: {response.status}")
                return None

    @sleep_and_retry
    @limits(calls=5, period=1)
    async def get_num_nft_holders(self, nftData):
        """
        Get the number of NFT holders for a given nftData. Automatically retries if the API returns an error.
        :param str nftData:
        :return: Number of holders
        :rtype: int
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = f"https://api3.loopring.io/api/v3/nft/info/nftHolders?nftData={nftData}&limit=500"
            for i in range(LR_RETRY_ATTEMPTS):
                async with session.get(api_url) as response:
                    if response.status == 200:
                        response = await response.json()
                        return response['totalNum']
                await asyncio.sleep(LR_RETRY_DELAY)
            logging.error(f"Error getting number of NFT holders: {response.status}")
            return None

    async def get_user_address(self, accountId):
        """
        Get the user's address from their accountId
        :param str accountId: User's accountId
        :return: User's address
        :rtype: str
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = f"https://api3.loopring.io/api/v3/account?accountId={accountId}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response['address']
                else:
                    logging.error(f"Error getting user address: {response.status}")
                    return None

    async def get_accountId_from_address(self, address):
        """
        Get the user's accountId from their address
        :param str address: User's address
        :return: User's accountId
        :rtype: str
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = f"https://api3.loopring.io/api/v3/account?address={address}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response['accountId']
                else:
                    logging.error(f"Error getting user accountId: {response.status}")
                    return None

    @sleep_and_retry
    @limits(calls=5, period=1)
    async def get_nft_holders(self, nftData, verbose=True):
        """
        Get the list of NFT holders for a given nftData. Automatically retries if the API returns an error.
        :param str nftData: nftData to get holders for
        :param bool verbose: Verbose output
        :return: Total number of holders, List of NFT holders
        :rtype: int, list
        """
        index = 0
        results_limit = 500
        total_holders = await self.get_num_nft_holders(nftData)
        if total_holders == 0:
            return 0, []

        holders_list = []
        async with aiohttp.ClientSession(headers=self.headers) as session:
            while index < total_holders:
                api_url = (f"https://api3.loopring.io/api/v3/nft/info/nftHolders?nftData={nftData}"
                           f"&offset={index}&limit={results_limit}")
                for i in range(LR_RETRY_ATTEMPTS):
                    async with session.get(api_url) as response:
                        if response.status == 200:
                            response = await response.json()
                            holders_list.extend(response['nftHolders'])
                            index += results_limit
                            break
                    await asyncio.sleep(LR_RETRY_DELAY)
                if verbose:
                    logging.info(f"Retrieved {index} of {total_holders} NFT holders.")

            accountId_list = ', '.join(['\'%s\'' % w['accountId'] for w in holders_list])

            query = f"SELECT * FROM users WHERE accountId IN ({accountId_list})"
            nf = NiftyDB()
            with nf.db.connect() as conn:
                account_list = conn.execute(query.format(accountId_list=accountId_list)).fetchall()

            account_dict = {}
            for account in account_list:
                account_dict[account['accountid']] = {'address': account['address'], 'username': account['username']}

            holders = []
            for holder in holders_list:
                if holder['accountId'] in account_dict:
                    holders.append({'address': account_dict[holder['accountId']]['address'],
                                    'username': account_dict[holder['accountId']]['username'],
                                    'amount': holder['amount']})

            return total_holders, holders

    @sleep_and_retry
    @limits(calls=5, period=1)
    async def get_user_nft_balance(self, accountId):
        """
        Get the user's NFT balance.
        :param str accountId: User's accountId
        :return: List of NFTs owned by user
        :rtype: List
        """

        offset = 0
        data = []
        retrieved_all = False

        async with aiohttp.ClientSession(headers=self.headers) as session:
            while not retrieved_all:
                api_url = f"https://api3.loopring.io/api/v3/user/nft/balances?accountId={accountId}&offset={offset}&limit=50"
                for i in range(LR_RETRY_ATTEMPTS):
                    async with session.get(api_url) as response:
                        if response.status == 200:
                            response = await response.json()
                            if response['totalNum'] == 0:
                                retrieved_all = True
                            else:
                                data.extend(response['data'])
                            offset += 50
                            break
                    await asyncio.sleep(LR_RETRY_DELAY)

        return data

    @sleep_and_retry
    @limits(calls=5, period=1)
    async def get_block(self, blockId):
        """
        Get the block details for a given blockId
        :param str blockId: Block ID to get details for
        :return: Block details
        :rtype: dict
        """
        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = f"https://api3.loopring.io/api/v3/block/getBlock?blockId={blockId}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response
                else:
                    logging.error(f"Error getting block details: {response.status}")
                    return None

    async def get_pending(self, spot_trades=True, transfers=True, mints=True):
        """
        Get pending transactions
        :param bool spot_trades: Get pending spot trades
        :param bool transfers: Get pending transfers
        :param bool mints: Get pending mints
        :return: List of pending transactions
        :rtype: list
        """

        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = f"https://api3.loopring.io/api/v3/block/getPendingRequests"
            nft_txs = dict()
            nft_txs['transactions'] = []
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    if spot_trades:
                        spot_trades = [tx for tx in response if tx['txType'] == 'SpotTrade']
                        spot_trades_nft = [tx for tx in spot_trades if tx['orderA']['nftData'] != '']
                        nft_txs['transactions'].extend(spot_trades_nft)

                    if transfers:
                        transfers = [tx for tx in response if tx['txType'] == 'Transfer']
                        transfers_nft = [tx for tx in transfers if tx['token']['nftData'] != '']
                        nft_txs['transactions'].extend(transfers_nft)

                    if mints:
                        mints = [tx for tx in response if tx['txType'] == 'NftMint']
                        mints_nft = [tx for tx in mints if tx['nftToken']['nftData'] != '']
                        nft_txs['transactions'].extend(mints_nft)
                    return nft_txs
                else:
                    logging.error(f"Error getting pending transactions: {response.status}")
                    return None

    async def filter_nft_txs(self, blockId):
        """
        Filter NFT transactions from a block
        :param int blockId: Block ID to filter
        :return: Block dict with block details and NFT transactions
        :rtype: dict
        """
        print(f"Processing block {blockId}")
        block_txs = await self.get_block(blockId)
        nft_txs = dict()
        nft_txs['blockId'] = blockId
        nft_txs['createdAt'] = block_txs['createdAt']
        nft_txs['transactions'] = []

        spot_trades = [tx for tx in block_txs['transactions'] if tx['txType'] == 'SpotTrade']
        spot_trades_nft = [tx for tx in spot_trades if tx['orderA']['nftData'] != '']
        nft_txs['transactions'].extend(spot_trades_nft)

        transfers = [tx for tx in block_txs['transactions'] if tx['txType'] == 'Transfer']
        transfers_nft = [tx for tx in transfers if tx['token']['nftData'] != '']
        nft_txs['transactions'].extend(transfers_nft)

        mints = [tx for tx in block_txs['transactions'] if tx['txType'] == 'NftMint']
        mints_nft = [tx for tx in mints if tx['nftToken']['nftData'] != '']
        nft_txs['transactions'].extend(mints_nft)

        return nft_txs

    async def save_nft_tx(self, blockData):
        """
        Save NFT transactions to database
        :param dict blockData: Block data
        :return: None
        """

        nf = NiftyDB()

        block_price_eth = await nf.get_historical_price('ETH', int(blockData['createdAt'] / 1000))
        block_price_lrc = await nf.get_historical_price('LRC', int(blockData['createdAt'] / 1000))
        if block_price_eth is None or block_price_lrc is None:
            logging.error(f"Error getting historical price for block {blockData['blockId']}")
            return None

        if nf.check_if_block_exists(blockData['blockId']):
            logging.info(f"Block {blockData['blockId']} already exists in database")
            return None

        created = int(blockData['createdAt'] / 1000)
        for tx in blockData['transactions']:
            if tx['txType'] == 'SpotTrade':
                # Check to see if transaction was done using LRC
                if tx['orderA']['tokenS'] == 1:
                    price_lrc = float(tx['orderA']['amountS']) / 10 ** 18 / float(tx['orderA']['amountB'])
                    price_usd = round(price_lrc * block_price_lrc, 2)
                    price_eth = round(price_usd / block_price_eth, 4)
                else:
                    price_eth = float(tx['orderA']['amountS']) / 10 ** 18 / float(tx['orderA']['amountB'])
                    price_usd = round(price_eth * block_price_eth, 2)
                nf.insert_transaction(blockData['blockId'], created, tx['txType'],
                                      tx['orderA']['nftData'], tx['orderB']['accountID'], tx['orderA']['accountID'],
                                      tx['orderB']['fillS'], price_eth, price_usd)
            elif tx['txType'] == 'Transfer':
                nf.insert_transaction(blockData['blockId'], created, tx['txType'],
                                      tx['token']['nftData'], tx['accountId'], tx['toAccountId'],
                                      tx['token']['amount'], 0, 0)
            elif tx['txType'] == 'NftMint':
                nf.insert_transaction(blockData['blockId'], created, tx['txType'],
                                      tx['nftToken']['nftData'], tx['minterAccountId'], tx['toAccountId'],
                                      tx['nftToken']['amount'], 0, 0)

        print(f"Saved block {blockData['blockId']} to database")

    async def get_nft_info(self, nftData):
        """
        Get NFT info from Loopring API
        :param str nftData: NFT data
        :return: NFT info
        :rtype: dict
        """

        async with aiohttp.ClientSession(headers=self.headers) as session:
            api_url = "https://api3.loopring.io/api/v3/nft/info/nfts?nftDatas={nftData}"
            async with session.get(api_url) as response:
                if response.status == 200:
                    response = await response.json()
                    return response.json()[0]
                else:
                    logging.error(f"Error fetching NFT Info for {nftData}: {response.status}")
                    return None


if __name__ == "__main__":
    lrc = LoopringAPI()
    time_start = time.time()
    asyncio.run(lrc.get_nft_holders("0x27665297fab3c72a472f81e6a734ffe81c8c1940a82164aca76476ca2b506724"))
    #print(asyncio.run(lrc.get_user_nft_balance(92477)))
    #print(asyncio.run(lrc.get_block(28000)))
    #print(asyncio.run(lrc.filter_nft_txs(28000)))
    print(f"{time.time()- time_start} seconds")
