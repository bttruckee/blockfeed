"""Now store the coinmarketcap data to postgres."""
import os
import psycopg2 as pg2
import psycopg2.extras as pg2ext
from datetime import date
import logging

batch_limit = 1000

# Pstgres setup - pull in .pgpass info
db_file = open("{0}/../.pgpass".format(os.path.dirname(os.path.abspath(__file__))),'r')
db_raw = db_file.readline().strip().split(':')
db_params = {
    'database': db_raw[2],
    'user': db_raw[3],
    'password': db_raw[4],
    'host': db_raw[0],
    'port': db_raw[1]
}
db_file.close()

conn = None

log = logging.getLogger('cmcFeed')

def connect():
    """Connect to the database."""
    log.info('Connect...')
    global conn
    if conn is not None:
        return conn
    else:
        conn = pg2.connect(**db_params)
        log.info('Postgres connect success.')
        return conn

def get_cursor():
    return connect().cursor(cursor_factory = pg2ext.RealDictCursor)

def cursor():
    return connect().cursor()

def insert_cmc_feed(response):
    log.info('Start loading cmc_feed into Postgres.')
    cursor = get_cursor()
    date_created = date.today()
    for row in response['data']:
        data = str(row)

        # breaking out separate for now as still want to create appropriate tables to organize.
        id = row['id']
        name = row['name']
        symbol = row['symbol']
        slug = row['slug']
        num_market_pairs = row['num_market_pairs']
        date_added = row['date_added']
        # making -1 rather than none for now.
        max_supply = row['max_supply']
        if max_supply == 'NONE':
            max_supply = -1
        total_supply = row['total_supply']
        last_updated = row['last_updated']

        # quote/pricing
        quote = row['quote']
        currency = quote['USD']
        price = currency['price']
        volume_24h = currency['volume_24h']
        volume_change_24h = currency['volume_change_24h']
        percent_change_1h = currency['percent_change_1h']
        percent_change_24h = currency['percent_change_24h']
        percent_change_7d = currency['percent_change_7d']
        percent_change_30d = currency['percent_change_30d']                
        percent_change_60d = currency['percent_change_60d']
        percent_change_90d = currency['percent_change_90d']
        market_cap = currency['market_cap']
        market_cap_dominance = currency['market_cap_dominance']
        fully_diluted_market_cap = currency['fully_diluted_market_cap']
        last_updated = currency['last_updated']
        
        data_string = (id, name, symbol, slug, num_market_pairs, date_added,
            max_supply,  total_supply, last_updated, price,
            volume_24h, volume_change_24h, percent_change_1h, 
            percent_change_24h, percent_change_7d, percent_change_30d,
            percent_change_60d, percent_change_90d, market_cap,
            market_cap_dominance, fully_diluted_market_cap, date_created)
 
        insert_query = ('insert into public."cmcFeed" (id, name, symbol, slug, num_market_pairs, date_added, ' + 
            'max_supply,  total_supply, last_updated, price, ' +
            'volume_24h, volume_change_24h, percent_change_1h, ' +
            'percent_change_24h, percent_change_7d, percent_change_30d, ' +
            'percent_change_60d, percent_change_90d, market_cap,' +
            'market_cap_dominance, fully_diluted_market_cap, date_created) ' +
            'values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)')
        cursor.execute(insert_query, data_string)

         # tags here.
         # for now, keying off coin symbol.  only add if it does not exist.

         # first check if we already have the symbol/tag entry.
        select_tag_match = 'select symbol, tag from public."cmcTag" where symbol = \'' + symbol +"\'"
        cursor.execute(select_tag_match)
        tag_db_records = cursor.fetchall()
        num_records = len(tag_db_records)

        #if not records then write the new symbol/tag combo
        if num_records == 0:
            tags = row['tags']
            for tag in tags:
                data_string = (symbol, tag, date_created)

                insert_query = ('insert into public."cmcTag" (symbol, tag, date_created) ' +
                    'values (%s,%s,%s)')
                cursor.execute(insert_query, data_string)
       
        cursor.execute("""COMMIT""")


    print("Complete")

