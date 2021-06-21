import json
import sys
from datetime import datetime
import time

import requests
from boto3.dynamodb.conditions import Key
import json
from erdpy.accounts import Address
from erdpy.proxy import ElrondProxy
from erdpy.contracts import SmartContract
import statistics
import boto3

from utils import getEpoch, convert_number

mainnet_proxy = ElrondProxy('https://gateway.elrond.com')
session = boto3.Session(profile_name='default')
dynamodb = session.resource('dynamodb', region_name='eu-west-1')
avg_apy = dynamodb.Table('avg_apy')

url = 'http://api.elrond.tax/'
datas = {}


def calculate_avg_apy(table, agency, start_epoch=250):
    global datas
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    contract = SmartContract(bech32_address)
    reply = contract.query(mainnet_proxy, 'getContractConfig', [])
    owner_address = Address(json.loads(reply[0].to_json())['hex']).bech32()
    max_cap = convert_number(json.loads(reply[2].to_json())['number']) if reply[2] != '' else 'no_cap'
    has_deleg_cap = bytes.fromhex(json.loads(reply[5].to_json())['hex']).decode('utf-8')
    check_cap_redeleg = bytes.fromhex(json.loads(reply[7].to_json())['hex']).decode('utf-8')
    params = {'address': owner_address}
    resp = requests.get(url + 'rewardsHistory', params)
    try:
        data = resp.json()
    except Exception as e:
        print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
        print(e, file=sys.stderr)
        return

    if not bech32_address in datas:
        datas[bech32_address] = []
    print('provider: ', bech32_address, " owner: ", owner_address)
    print('\t', end='')
    if 'error' in data:
        print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
        print(data['error'], file=sys.stderr)
        return
    if 'rewards_per_epoch' not in data:
        print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
        print('No rewards_per_epoch', file=sys.stderr)
        return
    current_epoch = getEpoch(int(datetime.utcnow().timestamp()))
    for epoch in range(start_epoch, current_epoch + 1):
        if bech32_address not in data['rewards_per_epoch']:
            print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
            print('Owner has no staked amount to his own pool', file=sys.stderr)
            break
        else:
            n = next((i for i, epochs in enumerate(data['rewards_per_epoch'][bech32_address])
                      if 'APRDelegator' in epochs and epoch == epochs['epoch']), None)
            if n:
                datas[bech32_address].append(
                    float(data['rewards_per_epoch'][bech32_address][n]['APRDelegator']))
            else:
                if not datas[bech32_address]:
                    continue
                datas[bech32_address].append(float(0))

        avg_at_epoch = statistics.mean(datas[bech32_address])
        item = {
            'provider': bech32_address,
            'owner': owner_address,
            'epoch': epoch,
            'avg_apy': '{:.4f}'.format(avg_at_epoch),
            'max_cap': str(max_cap),
            'has_deleg_cap': has_deleg_cap,
            'check_cap_redeleg': check_cap_redeleg
        }
        table.put_item(Item=item)
        print('(', item['epoch'], item['avg_apy'], datas[bech32_address][-1], end='),\n\t')
    print()

def update_avg_apy(table, agency):
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    kce = Key('provider').eq(bech32_address) & Key('epoch').gte(250)
    reply = table.query(KeyConditionExpression=kce, ScanIndexForward=False, Limit=1)
    if not reply['Items']:
        calculate_avg_apy(table, agency)
    else:
        calculate_avg_apy(table, agency, int(reply['Items'][0]['epoch']))


def update_avg_apy_all_agencies(table):
    agencies = SmartContract('erd1qqqqqqqqqqqqqqqpqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqylllslmq6y6').query(mainnet_proxy,
                                                                                                     'getAllContractAddresses',
                                                                                                     [])
    for agency in agencies:
        update_avg_apy(table, agency)


if __name__ == '__main__':
    while True:
        with open('datas.json', 'r') as fp:
            datas = json.load(fp)
        update_avg_apy_all_agencies(avg_apy)
        t = datetime.today()
        future = datetime(t.year, t.month, t.day + 1, 14, 35)
        with open('datas.json', 'w') as fp:
            json.dump(datas, fp)
        total_sec = (future - t).total_seconds()
        print(t.timestamp(), " -> seconds to sleep: ", total_sec)
        time.sleep(total_sec)
