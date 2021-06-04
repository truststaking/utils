import json
from datetime import datetime
import time

import requests
from boto3.dynamodb.conditions import Key

from erdpy.accounts import Address
from erdpy.proxy import ElrondProxy
from erdpy.contracts import SmartContract
import statistics
import boto3

from utils import getEpoch

mainnet_proxy = ElrondProxy('https://gateway.elrond.com')
session = boto3.Session(profile_name='default')
dynamodb = session.resource('dynamodb', region_name='eu-west-1')
avg_apy = dynamodb.Table('avg_apy')

url = 'http://api.elrond.tax/rewardsHistory'
datas = {}


def calculate_avg_apy(table, agency, start_epoch=250):
    global datas
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    contract = SmartContract(bech32_address)

    reply = contract.query(mainnet_proxy, 'getContractConfig', [])
    owner_address = Address(json.loads(reply[0].to_json())['hex']).bech32()
    params = {'address': owner_address}
    resp = requests.get(url, params)
    data = resp.json()
    datas[bech32_address] = []
    try:
        current_epoch = getEpoch(int(datetime.utcnow().timestamp()))
        for epoch in range(start_epoch, current_epoch + 1):
            if bech32_address not in data['rewards_per_epoch']:
                datas[bech32_address].append(float(0))
            else:
                n = next((i for i, epochs in enumerate(data['rewards_per_epoch'][bech32_address])
                          if 'APRDelegator' in epochs and epoch == epochs['epoch']), None)
                if n:
                    datas[bech32_address].append(
                        float(data['rewards_per_epoch'][bech32_address][n]['APRDelegator']))
                else:
                    datas[bech32_address].append(float(0))

            avg_at_epoch = statistics.mean(datas[bech32_address])
            item = {'provider': bech32_address, 'epoch': epoch, 'avg_apy': '{:.4f}'.format(avg_at_epoch)}
            table.put_item(Item=item)
            print(item)

    except Exception as e:
        print('error for provider:', bech32_address, owner_address)
        print(e)


def update_avg_apy(table, agency):
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    kce = Key('provider').eq(bech32_address) & Key('epoch').gte(250)
    reply = table.query(KeyConditionExpression=kce, ScanIndexForward=False, Limit=1)
    if not reply['Items']:
        calculate_avg_apy(table, agency)
    else:
        calculate_avg_apy(table, agency, reply['Items']['epoch'])


def update_avg_apy_all_agencies(table):
    agencies = SmartContract('erd1qqqqqqqqqqqqqqqpqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqylllslmq6y6').query(mainnet_proxy,
                                                                                                     'getAllContractAddresses',
                                                                                                     [])
    for agency in agencies:
        update_avg_apy(table, agency)


if __name__ == '__main__':
    while True:
        update_avg_apy_all_agencies(avg_apy)
        t = datetime.today()
        future = datetime(t.year, t.month, t.day + 1, 15, 31)
        time.sleep((future - t).total_seconds())
