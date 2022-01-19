from threading import Lock
from requests import Session
import json
import logging
import os
import sys

currency = os.environ.get('CURRENCY', 'USD')
cmc_key = os.environ.get('COINMARKETCAP_API_KEY')

lock = Lock()

# logging setup
log = logging.getLogger('cmcFeed')
log.setLevel(logging.INFO)
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)

class Cmc_API():
  def __init__(self):
    self.url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    self.headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': cmc_key}
    self.parameters = {'start': '1', 'limit': '5000', 'convert': currency}

  def tickers(self):
    log.info('Query coinmarketcap api...')
    session = Session()
    session.headers.update(self.headers)
    response = session.get(self.url, params=self.parameters)
    data = json.loads(response.text)
    if 'data' not in data:
      log.error('Error in CMC API call.  Likely a cmc key issue.')
      log.info(data)
    return data

  def collect(self):
    with lock:
      log.info('Query coinmarketcap api...')
      # query the api
      response = self.tickers()
      if 'data' not in response:
        log.error('Error in CMC API call.  Likely a cmc key issue.')
      else:                 
        ''' throw away, but writing out to a file for review. '''
        f = open("cmcApiCapture.txt", "w")
        for value in response['data']:
            f.write('row!' + str(value) + '\n')
        f.close()
        return response

