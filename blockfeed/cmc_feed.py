from cmc_api import Cmc_API
import cmc_postgress_load

''' pull data from coinMarketCap(cmc) '''
cmc_api = Cmc_API()
cmc_response = cmc_api.collect()
''' push cmc data to postgres '''
cmc_postgress_load.connect()
cmc_postgress_load.insert_cmc_feed(cmc_response)
