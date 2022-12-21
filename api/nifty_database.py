from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from .config import *
import logging
import traceback
import pandas as pd


class NiftyDB:
    def __init__(self):
        self.db = create_engine(f"{DB_TYPE}+{DB_ENGINE}://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
                                poolclass=NullPool)


    #############################
    # GETS
    #############################

    def get_user_info(self, accountId=None, address=None, username=None):
        with self.db.connect() as conn:
            if address is not None:
                result = conn.execute(text("SELECT * FROM users WHERE address=:address"), address=address)
            elif accountId is not None:
                result = conn.execute(text("SELECT * FROM users WHERE accountId=:accountId"), accountId=accountId)
            elif username is not None:
                result = conn.execute(text("SELECT * FROM users WHERE username=:username"), username=username)

            result = result.fetchone()

            if result is None:
                return None, None, None
            else:
                return result['accountid'], result['address'], result['username']

    def get_discord_server_stats(self, serverId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM discord_stats WHERE serverId=:serverId"), serverId=serverId)
            result = result.fetchall()

            if result is None:
                return None
            else:
                return result

    def get_last_hold_time_entry(self, nftId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM nft_stats WHERE nftId=:nftId ORDER BY timestamp DESC LIMIT 1"),
                                  nftId=nftId)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['timestamp']

    def get_first_sale(self, nftData):
        with self.db.connect() as conn:
            result = conn.execute(text(
                "SELECT * FROM transactions WHERE nftData=:nftData AND txType='SpotTrade' ORDER BY createdAt LIMIT 1"),
                                  nftData=nftData)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['createdat']

    def get_latest_saved_block(self):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM transactions ORDER BY blockId DESC LIMIT 1"))
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['blockid']

    def get_last_historical_price_data(self, currency):
        with self.db.connect() as conn:
            result = conn.execute(
                text("SELECT * FROM historical_crypto_prices WHERE currency=:currency ORDER BY timestamp DESC LIMIT 1"),
                currency=currency)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['timestamp']

    def get_historical_price(self, currency, timestamp):
        with self.db.connect() as conn:
            start = timestamp - 1000
            end = timestamp + 500
            print(f"Retrieving price for {currency} at {timestamp}")
            result = conn.execute(text("SELECT * FROM historical_crypto_prices WHERE currency=:currency AND timestamp "
                                       "BETWEEN :start AND :end ORDER BY timestamp DESC"),
                                  currency=currency, start=start, end=end)
            result = result.fetchone()
            if result is None:
                print(f"No historical price data found for {currency} at {timestamp}")
                return None
            else:
                return result['price']

    def get_nft_transactions(self, nftData):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM transactions WHERE nftData=:nftData ORDER BY blockId"),
                                  nftData=nftData)
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_nft_data(self, nft_id):
        with self.db.connect() as conn:
            # nftData has length of 66, tokenId+contractAddress has length of 109, nftId has length, 36
            if len(nft_id) == 66:
                result = conn.execute(text("SELECT * FROM nfts WHERE nftData=:nftData"), nftData=nft_id)
            elif len(nft_id) == 109:
                result = conn.execute(text("SELECT * FROM nfts WHERE tokenId=:tokenId"), tokenId=nft_id[:66])
            elif len(nft_id) == 36:
                result = conn.execute(text("SELECT * FROM nfts WHERE nftId=:nftId"), nftId=nft_id)
            result = result.fetchone()
            if result is None:
                return None
            elif len(result) == 0:
                return None
            else:
                return result

    def get_user_trade_history(self, accountId, nftData_List=None):
        with self.db.connect() as conn:
            query = (
                "SELECT transactions.*, nfts.nftData, nfts.name, buyer.username as buyer, seller.username as seller "
                "FROM transactions "
                "LEFT JOIN nfts on transactions.nftData = nfts.nftData "
                "LEFT JOIN users as buyer on transactions.buyerAccount = buyer.accountId "
                "LEFT JOIN users as seller on transactions.sellerAccount = seller.accountId "
                "WHERE (buyerAccount=:buyerAccount OR sellerAccount=:sellerAccount)")

            if nftData_List is not None:
                formatted_nftData_List = ', '.join(['\'%s\'' % w for w in nftData_List])
                query += f" AND (transactions.nftdata IN ({formatted_nftData_List})) "

            query += f"ORDER BY transactions.blockid"
            query = text(query)

            print(query)

            result = conn.execute(query, buyerAccount=accountId, sellerAccount=accountId)

            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_nft_trade_history(self, nft_id):
        nftData = self.get_nft_data(nft_id)['nftdata']

        with self.db.connect() as conn:
            result = conn.execute(text(
                "SELECT transactions.*, seller.username as seller, buyer.username as buyer FROM transactions "
                "INNER JOIN users as seller ON transactions.sellerAccount = seller.accountId "
                "INNER JOIN users AS buyer ON transactions.buyerAccount = buyer.accountId "
                "WHERE nftdata=:nftData"), nftData=nftData)

            result = result.fetchall()
            if result is None:
                print(f"No transactions found for {nft_id}")
                return None
            else:
                return result

    def get_nft_collection_tx(self, collectionId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT tx.*, nfts.nftId, nfts.name FROM transactions AS tx "
                                       "INNER JOIN nfts ON nfts.nftData = tx.nftData "
                                       "WHERE nfts.collectionId=:collectionId "
                                       "ORDER BY tx.blockId"), collectionId=collectionId)

            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_collection_info(self, collectionId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM collections WHERE collectionId=:collectionId"), collectionId=collectionId)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result

    def get_all_gamestop_nft_users(self):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM users ORDER BY accountId"))
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_transactions_since_block(self, blockId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM transactions WHERE blockId > :blockId ORDER BY blockId"), blockId=blockId)
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_users_without_usernames(self):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT *, LENGTH(username) AS user_length FROM users WHERE LENGTH(username)=42"))
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_last_collection_stats_timestamp(self, collectionId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT MAX(timestamp) AS timestamp FROM collection_stats WHERE collectionId=:collectionId"), collectionId=collectionId)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['timestamp']

    def get_collection(self, collectionId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM collections WHERE collectionId=:collectionId"), collectionId=collectionId)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result

    def get_collection_ids(self):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT collectionId FROM collections ORDER BY createdAt DESC"))
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_nfts_in_collection(self, collectionId):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM nfts WHERE collectionId=:collectionId"), collectionId=collectionId)
            result = result.fetchall()
            if result is None:
                return None
            else:
                return result

    def get_loopingu_rarity(self, number):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM loopingu_rarity WHERE number=:number"), number=number)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result

    def get_nft_by_name(self, name):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM nfts WHERE name=:name"), name=name)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result

    def get_nft_owner(self, nftData):
        with self.db.connect() as conn:
            result = conn.execute(text("SELECT * FROM transactions WHERE nftData=:nftData ORDER BY createdAt DESC LIMIT 1"), nftData=nftData)
            result = result.fetchone()
            if result is None:
                return None
            else:
                return result['buyeraccount']

    def get_mint_price(self, nft_id):
        with self.db.connect() as conn:
            if len(nft_id) == 36:
                nft = conn.execute(text("SELECT nftData FROM nfts WHERE nftId=:nft_id"), nft_id=nft_id).fetchone()
                nftData = nft['nftdata']
            else:
                nftData = nft_id
            first_sale = conn.execute(text("SELECT price FROM transactions WHERE nftData=:nftData AND txType IN ('SpotTrade', 'Transfer') ORDER BY blockId ASC LIMIT 1"), nftData=nftData).fetchone()
            if first_sale is None:
                return None
            else:
                return first_sale['price']



    #############################
    # UPDATES
    #############################

    def update_username(self, accountId, username):
        with self.db.connect() as conn:
            try:
                conn.execute(text("UPDATE users SET username=:username WHERE accountId=:accountId"),
                             username=username, accountId=accountId)
                return True
            except Exception as e:
                logging.error(f"Error updating username for {accountId}: {e} {traceback.format_exc()}")
                return False

    def update_num_nfts_in_collection(self, collectionId, numNfts):
        with self.db.connect() as conn:
            try:
                conn.execute(text("UPDATE collections SET numNfts=:numNfts WHERE collectionId=:collectionId"),
                             numNfts=numNfts, collectionId=collectionId)
                return True
            except Exception as e:
                logging.error(f"Error updating numNfts for {collectionId}: {e} {traceback.format_exc()}")
                return False

    def update_mint_price(self, nftId, price):
        with self.db.connect() as conn:
            try:
                conn.execute(text("UPDATE nfts SET mintPrice=:price WHERE nftId=:nftId"),
                             price=price, nftId=nftId)
                return True
            except Exception as e:
                logging.error(f"Error updating mint price for {nftId}: {e} {traceback.format_exc()}")
                return False

    #############################
    # INSERTS
    #############################

    def insert_user_info(self, accountId, address, username, discord_username):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO users (accountId, address, username, discord_username) VALUES (:accountId, :address, :username, :discord_username)"),
                             accountId=accountId, address=address, username=username, discord_username=discord_username)
                return True
            except Exception as e:
                logging.error(f"Error inserting user info for {accountId}: {e} {traceback.format_exc()}")
                return False

    def insert_nft(self, nftId, nftData, tokenId, contractAddress, creatorEthAddress, name, amount, attributes,
                   collectionId, createdAt, firstMintedAt, updatedAt, thumbnailUrl, mintPrice):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO nfts (nftId, nftData, tokenId, contractAddress, creatorEthAddress, name, amount, attributes, collectionId, createdAt, firstMintedAt, updatedAt, thumbnailUrl, mintPrice) "
                                  "VALUES (:nftId, :nftData, :tokenId, :contractAddress, :creatorEthAddress, :name, :amount, :attributes, :collectionId, :createdAt, :firstMintedAt, :updatedAt, :thumbnailUrl, :mintPrice)"),
                             nftId=nftId, nftData=nftData, tokenId=tokenId, contractAddress=contractAddress, creatorEthAddress=creatorEthAddress, name=name, amount=amount, attributes=attributes,
                             collectionId=collectionId, createdAt=createdAt, firstMintedAt=firstMintedAt, updatedAt=updatedAt, thumbnailUrl=thumbnailUrl, mintPrice=mintPrice)
                return True
            except Exception as e:
                logging.error(f"Error inserting nft info for {nftId}: {e} {traceback.format_exc()}")
                return False

    def insert_nft_stats(self, nftId, timestamp, hold_time, num_holders, whale_amount, top3, top5, avg_amount,
                         median_amount):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO nft_stats (nftId, timestamp, hold_time, num_holders, whale_amount, top3, top5, avg_amount, median_amount) "
                                  "VALUES (:nftId, :timestamp, :hold_time, :num_holders, :whale_amount, :top3, :top5, :avg_amount, :median_amount)"),
                             nftId=nftId, timestamp=timestamp, hold_time=hold_time, num_holders=num_holders, whale_amount=whale_amount, top3=top3, top5=top5, avg_amount=avg_amount,
                             median_amount=median_amount)
                return True
            except Exception as e:
                logging.error(f"Error inserting nft stats for {nftId}: {e} {traceback.format_exc()}")
                return False

    def insert_collection_stats(self, collectionId, timestamp, volume, volume_usd, unique_holders):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO collection_stats (collectionId, timestamp, total_volume, total_volume_usd, unique_holders) "
                                  "VALUES (:collectionId, :timestamp, :volume, :volume_usd, :unique_holders)"),
                             collectionId=collectionId, timestamp=timestamp, volume=volume, volume_usd=volume_usd, unique_holders=unique_holders)
                return True
            except Exception as e:
                logging.error(f"Error inserting collection stats for {collectionId}: {e} {traceback.format_exc()}")
                return False

    def insert_transaction(self, blockId, createdAt, txType, nftData, sellerAccount, buyerAccount, amount, price, priceUsd):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO transactions (blockId, createdAt, txType, nftData, sellerAccount, buyerAccount, amount, price, priceUsd) "
                                  "VALUES (:blockId, :createdAt, :txType, :nftData, :sellerAccount, :buyerAccount, :amount, :price, :priceUsd)"),
                             blockId=blockId, createdAt=createdAt, txType=txType, nftData=nftData, sellerAccount=sellerAccount, buyerAccount=buyerAccount, amount=amount, price=price, priceUsd=priceUsd)
                return True
            except Exception as e:
                logging.error(f"Error inserting transaction for {nftData}: {e} {traceback.format_exc()}")
                return False

    def insert_transactions(self, transactions):
        with self.db.connect() as conn:
            try:
                query = "INSERT INTO transactions (blockId, createdAt, txType, nftData, sellerAccount, buyerAccount, amount, price, priceUsd) VALUES "
                for transaction in transactions:
                    query += f"({transaction[0]}, {transaction[1]}, '{transaction[2]}', '{transaction[3]}', {transaction[4]}, {transaction[5]}, {transaction[6]}, {transaction[7]}, {transaction[8]}),"
                query = query[:-1]

                conn.execute(text(query))
                return True
            except Exception as e:
                logging.error(f"Error inserting transactions: {e} {traceback.format_exc()}")
                return False

    def insert_discord_server_stats(self, serverId, serverName, timestamp, num_members, num_online):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO discord_stats (serverId, serverName, timestamp, num_members, num_online) "
                                  "VALUES (:serverId, :serverName, :timestamp, :num_members, :num_online)"),
                             serverId=serverId, serverName=serverName, timestamp=timestamp, num_members=num_members, num_online=num_online)
                return True
            except Exception as e:
                logging.error(f"Error inserting discord server stats for {serverId}: {e} {traceback.format_exc()}")
                return False

    def insert_collection(self, collectionId, name, slug, creator, description, bannerUri, avatarUri, tileUri, createdAt, numNfts, layer):
        with self.db.connect() as conn:
            try:
                conn.execute(text("INSERT INTO collections (collectionId, name, slug, creator, description, bannerUri, avatarUri, tileUri, createdAt, numNfts, layer) "
                                  "VALUES (:collectionId, :name, :slug, :creator, :description, :bannerUri, :avatarUri, :tileUri, :createdAt, :numNfts, :layer)"),
                             collectionId=collectionId, name=name, slug=slug, creator=creator, description=description, bannerUri=bannerUri, avatarUri=avatarUri, tileUri=tileUri,
                             createdAt=createdAt, numNfts=numNfts, layer=layer)
                return True
            except Exception as e:
                logging.error(f"Error inserting collection for {collectionId}: {e} {traceback.format_exc()}")
                return False

    def insert_historical_price_data(self, currency, dataFrame):
        with self.db.connect() as conn:
            for index in dataFrame.index:
                timestamp = (dataFrame['time'][index] - pd.Timestamp("1970-01-01")) // pd.Timedelta('1s')
                datetime = dataFrame['time'][index].strftime('%Y-%m-%d %H:%M:%S')
                price = dataFrame['open'][index]
                conn.execute(text("INSERT INTO historical_crypto_prices VALUES (:timestamp, :datetime, :currency, :price)"), timestamp=timestamp, datetime=datetime, currency=currency, price=price)
                print(f"Inserted {timestamp} {datetime} | {currency}-USD: ${price}")


    #############################
    # CHECKS
    #############################

    def check_if_block_exists(self, blockId):
        with self.db.connect() as conn:
            try:
                result = conn.execute(text("SELECT blockId FROM transactions WHERE blockId=:blockId"), blockId=blockId).fetchone()
                return result is not None
            except Exception as e:
                logging.error(f"Error checking if block exists for {blockId}: {e} {traceback.format_exc()}")
                return False

if __name__ == "__main__":
    db = NiftyDB()
    print(db.get_last_collection_stats_timestamp('f6ff0ed8-277a-4039-9c53-18d66b4c2dac'))
