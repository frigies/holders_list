from entities import AddressBalance, Blockchain, Token, ExcludedAddress
from entities import address_balance
from format_currency import to_satoshi
import pandas as pd
from pprint import pprint
import requests
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

import time


def create_db_connection():
    db_name = os.getenv("FRIGIES_HOLDERS_DB_NAME_SQLITE")
    connection_url = f"sqlite:///{db_name}"
    engine = create_engine(connection_url)
    return engine


def get_excluded_addresses(engine, blockchain):
    query = f"SELECT value FROM excluded_address WHERE blockchain_id={blockchain.blockchain_id}"
    data = pd.read_sql_query(query, engine)
    excluded_addresses = []
    for _, row in data.iterrows():
        address = row["value"]
        if blockchain.blockchain_id == 56:
            address = address.lower()
        excluded_addresses.append(address)
    return excluded_addresses


def get_holders_addresses(token_address, blockchain):
    if blockchain.short_name_lower == "bsc":
        params = {
            "format": "JSON",
            "key": os.getenv("COVALENT_API_KEY")
        }
        chain_id = blockchain.blockchain_id
        url = f"https://api.covalenthq.com/v1/{chain_id}/tokens/{token_address}/token_holders/"

        holders_items = []

        while True:
            response = requests.get(url, params=params)
            data = response.json()
            data = data.get("data")
            items = data.get("items")
            holders_items += items

            pagination = data.get("pagination")
            has_more = pagination.get("has_more")
            if not has_more:
                break
            page_number = pagination.get("page_number")
            params["page-number"] = page_number + 1
    else:
        url0 = f"https://nodes.wavesnodes.com/blocks/height"
        response0 = requests.get(url0)
        data0 = response0.json()
        height = data0.get("height")
        height -= 1

        params = {}

        url1 = f"https://nodes.wavesnodes.com/assets/{token_address}/distribution/{height}/limit/1000"
        holders_items = {}

        while True:
            response1 = requests.get(url1, params=params)
            data1 = response1.json()
            items = data1.get("items")

            holders_items.update(items)
            has_more = data1.get("hasNext")
            if not has_more:
                break
            page_number = data1.get("lastItem")
            params["after"] = page_number
    return holders_items


def is_address(address, blockchain):
    url = f"https://deep-index.moralis.io/api/v2/erc20/metadata"
    headers = {
        'accept': 'application/json',
        'X-API-Key': os.getenv("MORALIS_API_KEY")
    }
    blockchain_name = blockchain.short_name
    params = {
        "chain": blockchain_name,
        "addresses": address
    }

    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    data = data[0]
    name = data.get("name")
    symbols = data.get("symbols")
    decimals = data.get("decimals")
    indicator0 = not any((name, symbols, decimals))

    if not indicator0:
        return False

    url = "https://api.bscscan.com/api"
    params = {
        "module": "proxy",
        "action": "eth_getCode",
        "address": address,
        "tag": "latest",
        "apikey": os.getenv("BSCSCAN_API_KEY")
    }
    response = requests.get(url, params=params)
    # pprint(response)
    data = response.json()
    # pprint(data)
    result = data.get("result")
    try:
        int(result, 0)
    except ValueError:
        indicator1 = True
    else:
        indicator1 = False
    return indicator1


def get_adresses_and_balances(engine, frigies):
    list_of_address_balances = []
    min_holder_balance = to_satoshi(frigies.min_holder_balance)
    excluded_addresses = get_excluded_addresses(engine, frigies.blockchain)

    if frigies.blockchain.short_name_lower == "bsc":
        holders = get_holders_addresses(frigies.address, frigies.blockchain)

        for holder in holders:
            address = holder.get("address")
            if address in excluded_addresses:
                continue

            balance = int(holder.get('balance'))
            decimals = int(holder.get("contract_decimals"))
            if balance < min_holder_balance:
                continue

            if not is_address(address, frigies.blockchain):
                continue

            address_balance = AddressBalance(address, balance, frigies.blockchain.blockchain_id, decimals)
            list_of_address_balances.append(address_balance)
            time.sleep(1)
    else:
        holders = get_holders_addresses(frigies.address, frigies.blockchain)
        for address, balance in holders.items():
            if address in excluded_addresses:
                continue
            if balance < min_holder_balance:
                continue
            address_balance = AddressBalance(address, balance, frigies.blockchain.blockchain_id)
            list_of_address_balances.append(address_balance)

    return list_of_address_balances


def get_holders(engine, frigies):
    holders = get_adresses_and_balances(engine, frigies)
    return holders


def update_excluded_addresses(addresses):
    excluded_addresses = tuple(map(
        lambda address: ExcludedAddress(address.get("value"), address.get("blockchain")),
        addresses))
    engine = create_db_connection()
    with Session(engine) as session:
        for excluded_address in excluded_addresses:
            try:
                session.add(excluded_address)
                session.commit()
            except IntegrityError:
                session.rollback()
            else:
                print(excluded_address)


def update_holders_list():
    print("Обновление списка холдеров")
    engine = create_db_connection()
    blockchain_waves = Blockchain(1, "Waves", "WAVES")
    blockchain_bsc = Blockchain(56, "BNB Smart Chain", "BSC")

    frigies_on_waves = Token("Frigies", "FRG", "B3mFpuCTpShBkSNiKaVbKeipktYWufEMAEGvBAdNP6tu", blockchain_waves)
    frigies_on_bsc = Token("Frigies", "FRG", "0x1680D783cc8f7A02cA792F534F9D62cB337C20aC", blockchain_bsc, 500)
    holders = get_holders(engine, frigies_on_waves) + get_holders(engine, frigies_on_bsc)
    with Session(engine) as session:
        delete_statement = address_balance.delete()
        engine.execute(delete_statement)
        session.add_all(holders)
        session.commit()
    print("Обновление списка холдеров завершено")
    return "OK"


def get_holders_list(format_):
    engine = create_db_connection()
    query = "SELECT * FROM holder"
    data = pd.read_sql_query(query, engine)
    if format_ == "json":
        content = data.to_json(orient="records")
    else:
        data = data.drop(["satoshi_balance", "blockchain_ticker"], axis=1)
        data.rename(columns={
            "address": "Адрес кошелька",
            "balance": "Баланс",
            "blockchain_name": "Блокчейн"
        }, inplace=True)
        data.index += 1
        content = data.to_html(justify="center")
    return content