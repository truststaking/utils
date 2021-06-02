# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import json
import numpy as np
import requests
from erdpy.contracts import SmartContract
from erdpy.proxy import ElrondProxy
from erdpy.accounts import Address
from elrond_delegators import get_network_delegators

url = 'http://api.elrond.tax/rewardsHistory'
mainnet_proxy = ElrondProxy('https://gateway.elrond.com')

def query(provider, function, args=None):
    if args is None:
        args = []
    contract = SmartContract(provider)
    return contract.query(mainnet_proxy, function, args)


def convert_number(number, decimals=2):
    return number // 10 ** (18 - decimals) / 10 ** decimals


def get_value(obj):
    if obj == [] or obj[0] == "":
        return 0
    return json.loads(obj[0].to_json())['number']


def get_total_rewards(provider, address):
    contract = SmartContract(provider)
    addr = f"0x{Address(address).hex()}"
    totalRewards = convert_number(
        get_value(contract.query(mainnet_proxy, 'getTotalCumulatedRewardsForUser', [addr])), 18)

    return totalRewards

err = float(0)
no_errors = 0
tests = 0
def test_rewards(address):

    global err
    global no_errors
    global tests
    try:
        params = {'address': address}
        resp = requests.get(url, params)
        data = resp.json()
        agencies = data['total_per_provider']
        for provider in agencies:
            totalRewards = get_total_rewards(provider, address)
            diff = abs(totalRewards - agencies[provider])
            tests += 1
            if abs(totalRewards - agencies[provider]) > 0.000001:
                with open('errors.txt', 'a') as errors:
                    print(address, file=errors)
                    print('\t', provider, file=errors)
                    print('\t\t', totalRewards, agencies[provider], file=errors)
                    err = (err + diff) / 2
                    no_errors += 1
                    print("current avr err:", err, file=errors)
                    print('errors: ', str(no_errors) + '/' + str(tests), file=errors)
            print('errors: ', str(no_errors) + '/' + str(tests))

    except Exception as e:
        print("error", e)

def save_delegators():
    delegators = get_network_delegators()
    np.savetxt("delegators.txt", np.asarray(delegators, dtype=str), delimiter='\n', fmt='%s')

def main():
    delegators = np.loadtxt('delegators.txt', dtype=str)
    for address in delegators:
        test_rewards(address)

if __name__ == '__main__':
    # save_delegators()
    main()