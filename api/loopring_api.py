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

def async_run(func):
    return asyncio.run(func)

class LoopringAPI(object):
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

            nf = NiftyDB()
            with nf.db.connect() as conn:

                with ThreadPoolExecutor(max_workers=30) as executor:
                    async def check_db_for_user_info(conn, accountId, amount):
                        nf = NiftyDB()
                        _, address, username = nf.get_user_info(conn, accountId=accountId)
                        print(f"Checking DB for {accountId} - {address} - {username}")
                        if username is not None:
                            return {'address': address, 'user': username, 'accountId': accountId, 'amount': int(amount)}
                        else:
                            address = self.get_user_address(accountId)
                            username = "Null"
                            return {'address': address, 'user': username, 'accountId': accountId, 'amount': int(amount)}

                    #tasks = [executor.submit(async_run(check_db_for_user_info(holder['accountId'], holder['amount']))) for holder in holders_list]
                    tasks = [check_db_for_user_info(conn, holder['accountId'], holder['amount']) for holder in holders_list]
                    holders_list = []
                    for result in executor.map(async_run, tasks):
                        holders_list.append({'user': result['user'], 'address': result['address'],
                                             'accountId': result['accountId'], 'amount': result['amount']})

                return total_holders, holders_list


if __name__ == "__main__":
    lrc = LoopringAPI()
    time_start = time.time()
    total_holders, holders_list = asyncio.run(lrc.get_nft_holders("0x27665297fab3c72a472f81e6a734ffe81c8c1940a82164aca76476ca2b506724"))

    print(holders_list)
    print(f"{time.time()- time_start} seconds")
