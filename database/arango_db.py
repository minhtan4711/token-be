from collections import OrderedDict
from arango import ArangoClient

client = ArangoClient()

db = client.db("token_transfers_bsc", username='root', password='471100')
tokens_collection = db.collection('tokens')
dapps_collection = db.collection('dapps')
token_wallets_collection = db.collection('token_wallets')
transfers_collection = db.collection('transfers')


def get_transfers_by_group(token_address, address_list, start_timestamp, end_timestamp):
    formatted_address_list = [f"wallets/{address}" for address in address_list]
    query = f"""
            FOR t IN transfers
                FILTER (t.contract_address == '{token_address}'
                        AND (t._from IN {formatted_address_list} OR t._to IN {formatted_address_list})
                        AND TO_NUMBER(t.transact_at) >= {start_timestamp}
                        AND TO_NUMBER(t.transact_at) <= {end_timestamp})
                SORT t.value DESC
                LIMIT 5
                RETURN {{
                    "from": t._from, "to": t._to, "value": t.value
                }}
        """
    transfers_cursor = db.aql.execute(query)

    return list(transfers_cursor)


def get_top_5_wallet(token_address, limit=5, offset=0):
    query = f"""
    FOR transfer IN transfers
        FILTER transfer.contract_address == "{token_address}"
        COLLECT address = transfer._from INTO group
        LET numTransfers = LENGTH(group)
        LET totalValue = SUM(group[*].transfer.value)
        SORT numTransfers DESC
        LIMIT {offset}, {limit}
        RETURN {{ "wallet": address, "number_of_transfers": numTransfers, "amount": totalValue }}
    """

    cursor = db.aql.execute(query)
    result = list(cursor)

    for wallet in result:
        wallet["wallet"] = wallet["wallet"].split('/')[1]

    return result


def get_top_5_transfers(token_address, limit=5, offset=0):
    query = f"""
    FOR transfer IN transfers
    FILTER transfer.contract_address == '{token_address}'
    SORT transfer.value DESC
    LIMIT {offset}, {limit}
    RETURN {{ 'transaction_hash': transfer.transaction_hash, 'value': transfer.value }}
    """
    cursor = db.aql.execute(query)
    return [item for item in cursor]


def get_dapps_by_token(token_address):
    query = f"""
    FOR dapp IN dapps
    LET token_address_in_key = SPLIT(dapp._key, '_')[0]
    FILTER token_address_in_key == '{token_address}'
    RETURN {{
        'id': dapp.idCMC,
        'name': dapp.name,
        'address': dapp.address,
        'image': dapp.image,
    }}
    """
    cursor = db.aql.execute(query)
    dapps = [doc for doc in cursor]

    return dapps


def get_token_info(token_address):
    result = tokens_collection.get(token_address)
    token_info = {
        'contract_address': result.get('contract_address'),
        'decimals': result.get('decimals'),
        'name': result.get('name'),
        'id': result.get('idCGK'),
        'symbol': result.get('symbol'),
        'logo': result.get('logo'),

    }

    return token_info


def get_token_transfers_by_timestamp(token_address, start_timestamp, end_timestamp):
    result = tokens_collection.get(token_address)
    maps = [
        'numberOfTransferChangeLogs',
        'tradingVolumeChanges',
        'numberOfAddressChangeLogs',
        'numberOfDappChangeLogs',
        'numberOfHolderChangeLogs',
        'numberOfWhaleWalletChangeLogs',
        'averageNumberOfTransactionPerDay'
    ]

    timestamps = set()
    for map_name in maps:
        timestamps.update(result.get(map_name, {}).keys())

    data_by_timestamp = {}

    for timestamp in timestamps:
        if start_timestamp <= int(timestamp) <= end_timestamp:
            data_by_timestamp[timestamp] = {}
            for map_name in maps:
                map_data = result.get(map_name, {})
                data_by_timestamp[timestamp][map_name] = map_data.get(
                    timestamp, 0)

    return OrderedDict(sorted(data_by_timestamp.items(), key=lambda t: t[0]))


def get_address_balance_by_timestamp(token_address, wallet_address, start_timestamp, end_timestamp):
    wallet_info = token_wallets_collection.get(
        f"{token_address}_{wallet_address}")
    wallet_balance_by_timestamp = wallet_info.get('balanceChangeLogs', {})
    filtered_balance_logs = {
        k: v for k,
        v in wallet_balance_by_timestamp.items()
        if start_timestamp <= int(k) <= end_timestamp
    }

    balance = 0
    is_whale = False
    for log in filtered_balance_logs.values():
        balance += log['balance']
        is_whale = is_whale or log['isWhale']

    return {
        'balance': balance,
        'isWhale': is_whale
    }


def get_dapp_info(token_address, dapp_id):
    key = f"{token_address}_{dapp_id}"

    document = dapps_collection.get(key)

    # Check if the document exists
    if document is None:
        return None

    dapp_info = {
        'idCMC': document.get('idCMC'),
        'address': document.get('address'),
        'name': document.get('name'),
        'image': document.get('image'),
    }

    return dapp_info


def is_dapp_address(token_address, dapp_id, wallet_address):
    key = f"{token_address}_{dapp_id}"
    document = dapps_collection.get(key)
    dapp_addresses = document.get('address', [])

    # Check if the wallet_address is in the dapp_addresses list
    return wallet_address in dapp_addresses


def is_whale_address(token_address, wallet_address, start_timestamp, end_timestamp):
    address_info = get_address_balance_by_timestamp(
        token_address, wallet_address, start_timestamp, end_timestamp)

    return address_info['isWhale']


def get_graph_data_by_timestamp(token_address, start_timestamp, end_timestamp, dapp_id=None):
    token_info = tokens_collection.get(token_address)
    clusters_by_timestamp = token_info.get(
        'walletClusterByNumberOfTransfer', {})

    force_graph_data = []

    for timestamp, clusters in clusters_by_timestamp.items():
        if start_timestamp <= int(timestamp) <= end_timestamp:
            timestamp_data = {'timestamp': timestamp, 'nodes': [], 'links': []}
            link_data_dict = {}
            address_to_cluster = {}
            cluster_balances = {}

            dapp_address_count = {}
            dapp_addresses = []
            whale_address_count = {}

            for cluster_name, cluster_data in clusters.items():
                addresses = cluster_data.get("addresses", [])
                normal_addresses_in_cluster = []

                for address in addresses:
                    if dapp_id is not None and is_dapp_address(token_address, dapp_id, address):
                        dapp_addresses.append(address)
                    elif is_whale_address(token_address, address, start_timestamp, end_timestamp):
                        whale_address_count[address] = whale_address_count.get(
                            address, 0) + 1
                        timestamp_data['nodes'].append({
                            'id': f"{address}",
                            'group': "WHALE",
                            'balance': abs(get_address_balance_by_timestamp(token_address, address, start_timestamp,
                                                                            end_timestamp)['balance']),
                            'numberOfAddress': whale_address_count[address],
                            'addresses': [address]
                        })
                    else:
                        normal_addresses_in_cluster.append(address)
                        address_to_cluster[address] = cluster_name

                cluster_balances[cluster_name] = sum(
                    get_address_balance_by_timestamp(
                        token_address, address, start_timestamp, end_timestamp)['balance']
                    for address in normal_addresses_in_cluster
                )

                timestamp_data['nodes'].append({
                    'id': cluster_name,
                    'group': cluster_name,
                    'balance': abs(cluster_balances[cluster_name]),
                    'numberOfAddress': len(normal_addresses_in_cluster),
                    'addresses': normal_addresses_in_cluster
                })

            if dapp_addresses:
                dapp_info = get_dapp_info(token_address, dapp_id)
                image_url = dapp_info.get(
                    'image', 'default_image_url') if dapp_info else 'default_image_url'
                timestamp_data['nodes'].append({
                    'id': "DAPP",
                    'group': "DAPP",
                    'balance': abs(sum(get_address_balance_by_timestamp(token_address, address, start_timestamp,
                                                                        end_timestamp)['balance'] for address in
                                       dapp_addresses)),
                    'numberOfAddress': len(dapp_addresses),
                    'image': image_url,
                    'addresses': dapp_addresses
                })

            for cluster_name, cluster_data in clusters.items():
                addresses = cluster_data.get("addresses", [])

                for address in addresses:
                    formatted_address = "wallets/" + address

                    query = f"""
                        FOR t IN transfers
                            FILTER (t._from == '{formatted_address}' OR t._to == '{formatted_address}')
                            AND
                            TO_NUMBER(t.transact_at) >= {start_timestamp}
                            AND
                            TO_NUMBER(t.transact_at) <= {end_timestamp}
                            RETURN t
                    """
                    transfers_cursor = db.aql.execute(query)

                    transfers = list(transfers_cursor)

                    for transfer in transfers:
                        source_address = transfer['_from'].replace(
                            'wallets/', '')
                        target_address = transfer['_to'].replace(
                            'wallets/', '')

                        if source_address in dapp_addresses:
                            source_cluster = "DAPP"
                        elif source_address in address_to_cluster:
                            source_cluster = address_to_cluster[source_address]
                        else:  # Must be a whale address
                            source_cluster = source_address

                        if target_address in dapp_addresses:
                            target_cluster = "DAPP"
                        elif target_address in address_to_cluster:
                            target_cluster = address_to_cluster[target_address]
                        else:  # Must be a whale address
                            target_cluster = target_address

                        value = transfer['value']

                        link_key = f"{source_cluster}_{target_cluster}"

                        if link_key in link_data_dict:
                            link_data_dict[link_key]['totalValue'] += value
                            link_data_dict[link_key]['numberOfTransfer'] += 1
                        else:
                            link_data_dict[link_key] = {
                                'source': source_cluster,
                                'target': target_cluster,
                                'totalValue': abs(value),
                                'numberOfTransfer': 1
                            }

            timestamp_data['links'] = list(link_data_dict.values())
            force_graph_data.append(timestamp_data)

    return force_graph_data
