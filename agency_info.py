import json
import threading
import time
from datetime import datetime, timedelta
from threading import Thread
from uuid import uuid4

import requests
from erdpy.contracts import SmartContract
from erdpy.proxy import ElrondProxy
from erdpy.accounts import Address

mainnet_proxy = ElrondProxy('https://gateway.elrond.com')
TrustStaking_contract = SmartContract('erd1qqqqqqqqqqqqqqqpqqqqqqqqqqqqqqqqqqqqqqqqqqqqqzhllllsp9wvyl')

def get_value(obj):
    if obj == [] or obj[0] == "":
        return 0
    return json.loads(obj[0].to_json())['number']

class Agency:
    def __init__(self, proxy=mainnet_proxy, contract=TrustStaking_contract, extra_info=False):
        self.proxy = proxy
        self.contract = contract

    def query(self, function, args=None):
        if args is None:
            args = []
        return get_value(self.contract.query(self.proxy, function, args))

    def get_address_info(self, address):
        print("get_address_info called")
        addr = f"0x{Address(address).hex()}"
        claimable = get_value(self.contract.query(self.proxy, 'getClaimableRewards', [addr]))
        totalRewards = get_value(self.contract.query(self.proxy, 'getTotalCumulatedRewardsForUser', [addr]))
        active = get_value(self.contract.query(self.proxy, 'getUserActiveStake', [addr]))
        undelegated_list = self.contract.query(self.proxy, 'getUserUnDelegatedList', [addr])

        return active #, claimable, totalRewards
    def get_agency_name(self):
        metaData = self.contract.query(self.proxy, 'getMetaData', [])
        if metaData == []:
            return "", ""
        name_in_hex = json.loads(metaData[0].to_json())['hex']
        name = bytes.fromhex(name_in_hex).decode('utf-8')
        if 'Pro Crypto' in name:
            name = 'ProCrypto 🌍 Distributed Staking'
        website_in_hex = json.loads(metaData[1].to_json())['hex']
        website = bytes.fromhex(website_in_hex).decode('utf-8')
        identity_in_hex = json.loads(metaData[2].to_json())['hex']
        identity = bytes.fromhex(identity_in_hex).decode('utf-8')
        return name, identity

TS = Agency()
agencies = SmartContract('erd1qqqqqqqqqqqqqqqpqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqylllslmq6y6').query(mainnet_proxy,
                                                                                              'getAllContractAddresses',
                                                                                              [])