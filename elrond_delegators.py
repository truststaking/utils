from datetime import datetime
import json
import time

import numpy as np
import requests
from erdpy.accounts import Address

from agency_info import agencies
from utils import dynamodb, Phase3, getEpoch

url = 'http://api.elrond.tax/allTransactions'

agencies_distribution = dynamodb.Table('agencies_distribution')


def get_network_delegators(table=None, after_epoch=None):
    delegators = []
    for agency in agencies:
        hex_address = json.loads(agency.to_json())['hex']
        contract = Address(hex_address).bech32()
        print(contract)
        params = {'address': contract}
        resp = requests.get(url, params)
        data = resp.json()
        datas = {}
        withdrawers = 0
        total_withdrawn = np.float128(0.0)
        all_time_delegators = 0
        all_time_unb = 0
        all_time_w = 0
        try:
            for epoch in data:
                for d in data[epoch]:
                    if 'data' not in d:
                        continue
                    command = d['data'].split('@')[0]
                    if command == 'delegate':
                        if d['sender'] not in datas:
                            datas[d['sender']] = np.float128(d['value'])
                            all_time_delegators += 1
                        else:
                            datas[d['sender']] += np.float128(d['value'])
                    elif command == 'reDelegateRewards':
                        if d['sender'] not in datas:
                            print("X->", d['sender'])
                            continue
                        if 'data' not in d['scResults'][0]:
                            datas[d['sender']] += np.float128(d['scResults'][0]['value'])
                        else:
                            datas[d['sender']] += np.float128(d['scResults'][1]['value'])

                    elif command == 'unDelegate':
                        if d['sender'] not in datas:
                            print("X->", d['sender'])
                            continue

                        datas[d['sender']] -= np.float128(d['value'])
                        total_withdrawn += np.float128(d['value'])
                        if datas[d['sender']] < 10 ** 18:
                            del datas[d['sender']]
                            all_time_unb += 1
                        withdrawers += 1
                        # else:
                        #     windrawers.append(d['sender'])
                    elif command == 'withdraw':
                        all_time_w += 1
                        withdrawers -= 1
                    else:
                        # do nothing
                        continue
                if table is not None \
                    and after_epoch is not None \
                    and int(epoch) >= after_epoch:
                    distribution = {
                        'provider': contract,
                        'epoch': int(epoch),
                        'no_withdrawers': withdrawers,
                        'all_time_delegators': all_time_delegators,
                        'all_time_withdrawers': all_time_w,
                        'under2': 0,
                        'under5': 0,
                        'under10': 0,
                        'under25': 0,
                        'under50': 0,
                        'under100': 0,
                        'under200': 0,
                        'under300': 0,
                        'under500': 0,
                        'under1000': 0,
                        'under1500': 0,
                        'under3000': 0,
                        'more': 0
                    }
                    for d in datas:
                        v = datas[d] / 10 ** 18
                        if v < 2:
                            distribution['under2'] += 1
                        elif v < 5:
                            distribution['under5'] += 1
                        elif v < 10:
                            distribution['under10'] += 1
                        elif v < 25:
                            distribution['under25'] += 1
                        elif v < 50:
                            distribution['under50'] += 1
                        elif v < 100:
                            distribution['under100'] += 1
                        elif v < 200:
                            distribution['under200'] += 1
                        elif v < 300:
                            distribution['under300'] += 1
                        elif v < 500:
                            distribution['under500'] += 1
                        elif v < 1000:
                            distribution['under1000'] += 1
                        elif v < 1500:
                            distribution['under1500'] += 1
                        elif v < 3000:
                            distribution['under3000'] += 1
                        else:
                            distribution['more'] += 1
                    table.put_item(Item=distribution)
                    print(distribution)
            delegators.extend(datas.keys())
        except Exception as e:
            print("error for:", agency, e)
    return delegators


if __name__ == '__main__':
#    get_network_delegators(agencies_distribution, 250)
    while True:
        t = datetime.today()
        next_epoch = getEpoch(t.timestamp())
        print("next epoch: ", next_epoch)
        get_network_delegators(agencies_distribution, next_epoch)
        future = datetime(t.year, t.month, t.day + 1, 14, 35)
        t_sleep = (future - t).total_seconds()
        print("sleep for: ", t_sleep, " seconds")
        time.sleep(t_sleep)
