from sanic import Sanic
from sanic.response import json
from database.arango_db import get_token_info as get_token_info_from_db
from database.arango_db import get_token_transfers_by_timestamp as get_token_transfers_by_timestamp_from_db
from database.arango_db import get_graph_data_by_timestamp as get_graph_data_by_timestamp_from_db
from database.arango_db import get_address_balance_by_timestamp as get_address_balance_by_timestamp_from_db
from database.arango_db import get_dapp_info as get_dapp_info_from_db
from database.arango_db import get_dapps_by_token as get_dapps_by_token_from_db
from database.arango_db import get_top_5_transfers as get_top_5_transfers_from_db
from database.arango_db import get_top_5_wallet
from database.arango_db import get_transfers_by_group as get_transfers_by_group_from_db
from database.arango_db import get_dapp_at_timestamp as get_dapp_at_timestamp_from_db

app = Sanic(__name__)
app.config.CORS_ORIGINS = "*"
app.config.CORS_ALLOW_HEADERS = "*"


@app.route("/dapp-at-timestamp/<token_address>")
async def get_dapp_at_timestamp(request, token_address):
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    result = get_dapp_at_timestamp_from_db(
        token_address=token_address,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    return json(result)


@app.route("/group-transfer/<token_address>")
async def get_transfers_by_group(request, token_address):
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    address_list_str = request.args.get('address_list')
    address_list = address_list_str.split(',')
    result = get_transfers_by_group_from_db(
        token_address=token_address,
        address_list=address_list,
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp
    )
    return json(result)


@app.route("/top-wallets/<token_address>")
async def get_top_wallet(request, token_address):
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    result = get_top_5_wallet(token_address=token_address, limit=limit, offset=offset)
    return json(result)


@app.route("/top-transfers/<token_address>")
async def get_top_transfer_by_token(request, token_address):
    limit = request.args.get('limit')
    offset = request.args.get('offset')
    result = get_top_5_transfers_from_db(token_address=token_address, limit=limit, offset=offset)
    return json(result)


@app.route("/dapps/<token_address>")
async def get_dapps_by_token(request, token_address):
    result = get_dapps_by_token_from_db(token_address=token_address)
    return json(result)


@app.route("/dapp-info/<token_address>")
async def get_dapp_info(request, token_address):
    dapp_id = request.args.get('dapp_id')
    result = get_dapp_info_from_db(token_address, dapp_id)
    return json(result)


@app.route("/token-wallet/<token_address>")
async def get_wallet_balance(request, token_address):
    wallet_address = request.args.get('wallet_address')
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    result = get_address_balance_by_timestamp_from_db(
        token_address, wallet_address, start_timestamp, end_timestamp)
    return json(result)


@app.route("/token-info/<token_address>")
async def get_token_info(request, token_address):
    token_info = get_token_info_from_db(token_address=token_address)
    return json(token_info)


@app.route("/token-transfer/<token_address>")
async def get_token_transfer(request, token_address):
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')

    if not start_timestamp or not end_timestamp:
        return json({"error": "Both start_timestamp and end_timestamp are required."}, status=400)

    token_transfer = get_token_transfers_by_timestamp_from_db(
        token_address=token_address,
        start_timestamp=int(start_timestamp),
        end_timestamp=int(end_timestamp)
    )
    return json(token_transfer)


@app.route("/graph-data/<token_address>")
async def get_graph_data(request, token_address):
    start_timestamp = request.args.get('start_timestamp')
    end_timestamp = request.args.get('end_timestamp')
    dapp_id_list_str = request.args.get('dapp_id')
    dapp_list = dapp_id_list_str.split(',')

    if not start_timestamp or not end_timestamp:
        return json({"error": "Both start_timestamp and end_timestamp are required to retrieve graph data."}, status=400)

    graph_data = get_graph_data_by_timestamp_from_db(
        token_address=token_address,
        start_timestamp=int(start_timestamp),
        end_timestamp=int(end_timestamp),
        dapp_ids=dapp_list
    )
    return json(graph_data)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
