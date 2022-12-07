

def pull_usernames_from_transactions(blockId):
    """
    Pulls usernames of all users with transactions since blockId and adds new users to the database
    :param int blockId: Starting blockId
    :return:
    """
    nf = nifty.NiftyDB()
    lr = loopring.LoopringAPI()

    txs = nf.get_transactions_since_block(blockId)
    accountId_list = (user['accountid'] for user in nf.get_all_gamestop_nft_users())
    for tx in txs:
        if tx['selleraccount'] not in accountId_list:
            user = User(accountId=tx['selleraccount'])
            print(f"Retrieved username for {tx['selleraccount']}: {user.username}")
        if tx['buyeraccount'] not in accountId_list:
            user = User(accountId=tx['buyerAccount'])
            print(f"Retrieved username for {tx['buyeraccount']}: {user.username}")