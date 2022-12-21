import csv
from datetime import datetime, timedelta
import os
import asyncio
import time
import pandas as pd
from nft_ids import *
import plotly.graph_objects as go
import numpy as np
from plotly.subplots import make_subplots
import networkx as nx
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from xlsxwriter import Workbook
import traceback

import api.config
from api.gamestop_api import GamestopApi, NftCollection, Nft, User
from api.loopring_api import LoopringAPI
from api.nifty_database import NiftyDB
from api.coinbase_api import CoinbaseAPI


GS_HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
}

def get_historical_crypto_data(currency, start_date):
    print(f"Getting historical data for {currency} starting at {start_date}")

    return CoinbaseAPI(f'{currency}-USD', start_date).retrieve_data()

def update_historical_crypto_data(currency):
    nf = NiftyDB()
    last_price_timestamp = nf.get_last_historical_price_data(currency)
    if last_price_timestamp is None:
        last_price_timestamp = 1640995200
    last_price_timestamp = datetime.utcfromtimestamp(last_price_timestamp).strftime('%Y-%m-%d-%H-%M')
    data = get_historical_crypto_data(currency, last_price_timestamp)
    nf.insert_historical_price_data(currency, data[1:])

async def grab_new_blocks(find_missing=False, find_new_users=True):
    update_historical_crypto_data('ETH')
    update_historical_crypto_data('LRC')
    lr = LoopringAPI()
    nf = NiftyDB()
    last_block = nf.get_latest_saved_block()
    if last_block is None:
        last_block = 24340
    if find_missing:
        last_block = 24340
    i = 1

    # Grab the latest block, and generate batches of block IDs to retrieve in parallel
    latest_block = await lr.get_latest_block()
    if latest_block == last_block:
        print("No new blocks to retrieve")
        return

    batches = []
    while last_block < latest_block:
        batches.append([last_block + i for i in range(5)])
        last_block += 5
    batches[-1] = list(range(batches[-2][4]+1, latest_block+1))

    for batch in batches:
        results = await asyncio.gather(*[lr.filter_nft_txs(blockId) for blockId in batch])
        for i in results:
            await lr.save_nft_tx(i)

    if find_new_users:
        print("Looking for new users...")
        await pull_usernames_from_transactions(blockId=last_block)


async def dump_detailed_orderbook_and_holders(output_name, nftId_list=None, collectionId_list=None, limit=None):
    """
    Dumps detailed orderbook and holders of any NFT to a XLSX file
    :param str output_name: Name of output file
    :param list nftId_list: List of nftIds to include
    :param list collectionId_list: List of collectionIds to include
    :param int limit: Limit price of orders to include, given as a multiple of the floor price
    :return:
    """
    lr = LoopringAPI()
    gs = await GamestopApi.load()
    date = datetime.now().strftime('%Y-%m-%d')
    date_and_time = datetime.now().strftime('%Y-%m-%d %H-%M-%S')
    folder = f"Reports\\Detailed_Orderbooks\\{date}"
    filename = f"{date_and_time}_{output_name}.xlsx"
    if not os.path.exists(folder):
        os.makedirs(folder)

    # Check that NFTs and/or Collections are provided
    if nftId_list is None and collectionId_list is None:
        print("No nftId or collectionId list provided")
        return

    # Retrieve nftId's from nftId_list and collectionId_list
    nftIds = []
    if nftId_list is not None:
        nftIds.extend(nftId_list)
    if collectionId_list is not None:
        for collection in collectionId_list:
            coll = await NftCollection.load(collection, get_collection_nfts=True)
            nftIds.extend([nft['nftId'] for nft in coll.collection_nfts])

    with pd.ExcelWriter(f"{folder}\\{filename}") as writer:
        for idx, nftId in enumerate(nftId_list):
            # Retrieve the orderbook and load into a dataframe
            nft = await Nft.load(nftId)
            orderbook = await nft.get_detailed_orders(limit=limit)
            df = pd.DataFrame(orderbook, columns=['pricePerNft', 'amount', 'sellerName', 'ownerAddress', 'totalForSale',
                                                  'numOwned'])
            df.columns = ['Price', 'Amount', 'Seller', 'Address', 'Total # For Sale', 'Owned']
            df['Price USD'] = round(df['Price'] * gs.eth_usd, 2)
            df = df[['Price', 'Price USD', 'Amount', 'Seller', 'Address', 'Total # For Sale', 'Owned']]
            sheet_name = str(idx + 1) + " " + ''.join(x for x in nft.get_name() if (x.isalnum() or x in "._- "))[:27]

            df.to_excel(writer, startrow=6, startcol=6, freeze_panes=(7, 0), index=False, sheet_name=sheet_name)
            worksheet = writer.sheets[sheet_name]
            worksheet.write(0, 0, 'NFT Name')
            worksheet.write(0, 1, nft.get_name())
            worksheet.write(1, 0, 'NFT ID')
            worksheet.write(1, 1, nftId)
            worksheet.write(2, 0, 'Data Retrieved')
            worksheet.write(2, 1, date_and_time)

            num_holders, nft_holders = await lr.get_nft_holders(nft.get_nft_data())
            holders_sorted = sorted(nft_holders, key=lambda d: int(d['amount']), reverse=True)
            holders_df = pd.DataFrame(holders_sorted, columns=['user', 'amount', 'address', 'accountId'])
            holders_df.columns = ['User', 'Amount', 'Address', 'Account ID']
            holders_df.to_excel(writer, startrow=6, freeze_panes=(7, 0), index=False, sheet_name=sheet_name)
            worksheet.write(3, 0, '# Holders')
            worksheet.write(3, 1, num_holders)
            worksheet.write(5, 2, 'Wallets Holding')
            worksheet.write(5, 10, 'Wallets Selling')

            writer.sheets[sheet_name].set_column(0, 0, 15)
            writer.sheets[sheet_name].set_column(1, 1, 8)
            writer.sheets[sheet_name].set_column(2, 2, 45)
            writer.sheets[sheet_name].set_column(3, 3, 10)
            writer.sheets[sheet_name].set_column(9, 9, 15)
            writer.sheets[sheet_name].set_column(10, 10, 45)
            writer.sheets[sheet_name].set_column(11, 11, 12)



async def pull_usernames_from_transactions(blockId):
    """
    Pulls usernames of all users with transactions since blockId and adds new users to the database
    :param int blockId: Starting blockId
    :return:
    """
    nf = NiftyDB()
    lr = LoopringAPI()

    txs = nf.get_transactions_since_block(blockId)
    accountId_list = [user['accountid'] for user in nf.get_all_gamestop_nft_users()]
    print(f"Found {len(accountId_list)} users in DB")
    for tx in txs:
        if tx['selleraccount'] not in accountId_list:
            user = User(accountId=tx['selleraccount'])
            user = await user.load(accountId=tx['selleraccount'])
            accountId_list.append(tx['selleraccount'])
            print(f"Retrieved username for {tx['selleraccount']}: {user.username}")
        if tx['buyeraccount'] not in accountId_list:
            user = User(accountId=tx['buyeraccount'])
            user = await user.load(accountId=tx['buyeraccount'])
            accountId_list.append(tx['buyeraccount'])
            print(f"Retrieved username for {tx['buyeraccount']}: {user.username}")


async def main():
    time_start = time.time()
    nf = NiftyDB()
    lr = LoopringAPI()

    #await grab_new_blocks()
    await pull_usernames_from_transactions(31000)
    print("Time elapsed: " + str(time.time() - time_start))

if __name__ == '__main__':
    asyncio.run(main())