genesis = {
    'timestamp': 1596112200,
    'epoch': 0,
}

binance_listing = 1599102000

def getEpoch(timestamp):
    diff = timestamp - genesis['timestamp']
    return genesis['epoch'] + diff // (60 * 60 * 24)