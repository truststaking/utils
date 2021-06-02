import datetime
import json

import numpy as np
import pandas as pd
import requests
from erdpy.accounts import Address, _decode_bech32
from erdpy.contracts import SmartContract

import agency_info
from agency_info import agencies

url = 'http://api.elrond.tax/allTransactions'


def get_network_delegators():
    delegators = []
    for agency in agencies:
        hex_address = json.loads(agency.to_json())['hex']
        contract = Address(hex_address).bech32()
        print(contract)
        # address = 'erd1qqqqqqqqqqqqqqqpqqqqqqqqqqqqqqqqqqqqqqqqqqqqqzhllllsp9wvyl' #'erd1f3r5fz2mpq4nfsxwsf65wvxjradfq8sdqt4m32x86pjv9sjrwk5s8evers'
        params = {'address': contract}
        resp = requests.get(url, params)
        data = resp.json()
        datas = {}
        # windrawers = []
        withdrawers = 0
        total_withdrawn = np.float128(0.0)
        all_time_delegators = 0
        all_time_unb = 0
        all_time_w = 0
        try:
            for d in data['transactions']:
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
            s = np.float128(0.0)
            print(all_time_delegators, all_time_unb, all_time_w)
            print('last timestamp:', datetime.datetime.utcfromtimestamp(data['transactions'][-1]['timestamp']))
            name, identity = agency_info.Agency(contract=SmartContract(contract)).get_agency_name()
            if identity == '':
                identity = contract
            if name == '':
                name = contract
            # with open("wallets_" + name, "w") as fp:
            delegators.extend(datas.keys())
        except:
            print("error for:", agency)
    return delegators
