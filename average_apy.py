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


def calculate_avg_apy(table, agency, daily_apys=None, start_epoch=250):
    if daily_apys is None:
        daily_apys = []
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    contract = SmartContract(bech32_address)
    reply = contract.query(mainnet_proxy, 'getContractConfig', [])
    owner_address = Address(json.loads(reply[0].to_json())['hex']).bech32()

    params = {'address': owner_address}
    resp = requests.get(url + 'rewardsHistory', params)
    try:
        data = resp.json()
    except Exception as e:
        print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
        print(e, file=sys.stderr)
        return

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
    print("current_epoch: ", current_epoch)
    for epoch in range(start_epoch, current_epoch + 1):
        if bech32_address not in data['rewards_per_epoch']:
            print('provider: ', bech32_address, " owner: ", owner_address, file=sys.stderr)
            print('Owner has no staked amount to his own pool', file=sys.stderr)
            break
        else:
            n = next((i for i, epochs in enumerate(data['rewards_per_epoch'][bech32_address])
                      if 'APRDelegator' in epochs and epoch == epochs['epoch']), None)
            daily_apy = 0
            if n is not None:
                daily_apy = float(data['rewards_per_epoch'][bech32_address][n]['APRDelegator'])
            else:
                if not daily_apys:
                    continue
            daily_apys.append(daily_apy)

        avg_at_epoch = statistics.mean(daily_apys)
        item = {
            'provider': bech32_address,
            'owner': owner_address,
            'epoch': epoch,
            'avg_apy': '{:.4f}'.format(avg_at_epoch),
            'daily_apy': '{:.4f}'.format(daily_apy)
        }
        table.put_item(Item=item)

def update_avg_apy(table, agency):
    hex_address = json.loads(agency.to_json())['hex']
    bech32_address = Address(hex_address).bech32()
    kce = Key('provider').eq(bech32_address) & Key('epoch').gte(250)
    reply = table.query(KeyConditionExpression=kce, ScanIndexForward=False)
    if not reply['Items']:
        return calculate_avg_apy(table, agency)
    else:
        daily_apys = [float(item['daily_apy']) for item in reply['Items']]
        return calculate_avg_apy(table, agency, daily_apys, int(reply['Items'][0]['epoch']))

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
        future = datetime(t.year, t.month, t.day + 1, 15, 35)
        total_sec = (future - t).total_seconds()
        print(t.timestamp(), " -> seconds to sleep: ", total_sec)
        time.sleep(total_sec)
