#!./venv/bin/python
import requests
import sqlite3
import argparse
import sys

cr_conf = __import__('cr-conf')
cf = cr_conf.conf


def setupdb(db, raiders=True, gear=True):
    cur = db.cursor()
    if raiders:
        cur.execute('''CREATE TABLE IF NOT EXISTS raiders(
            id INTEGER PRIMARY KEY,
            nft_token VARCHAR(255),
            name VARCHAR(255),
            image TEXT,
            race VARCHAR(255),
            generation INTEGER,
            birthday INTEGER,
            experience INTEGER,
            level INTEGER,
            strength INTEGER,
            intelligence INTEGER,
            agility INTEGER,
            wisdom INTEGER,
            charm INTEGER,
            luck INTEGER)''')
    if gear:
        cur.execute('''CREATE TABLE IF NOT EXISTS gear(
            name VARCHAR(255),
            equipped INTEGER,
            slot VARCHAR(255),
            strength INTEGER,
            intelligence INTEGER,
            agility INTEGER,
            wisdom INTEGER,
            charm INTEGER,
            luck INTEGER,
            owner_id INTEGER,
            source VARCHAR(255),
            FOREIGN KEY(owner_id) REFERENCES raiders(id))''')
    db.commit()


def get_owned_raider_nfts():
    print('querying polygon owned NFTs via alchemy')
    r = requests.get('%s/%s/getNFTs/?owner=%s&contractAddresses[]=%s' % (
        cf.alchemy_api_url, cf.alchemy_api_key, cf.nft_owner, cf.nft_contract))
    data = r.json()
    print('found %d raider NFTs' % (data['totalCount'],))
    for nft in data['ownedNfts']:
        if nft['contract']['address'] == cf.nft_contract:
            yield nft['id']['tokenId']


def lookup_nft_raider_id(tokenid):
    r = requests.get('%s/%s/getNFTMetadata/?contractAddress=%s&tokenId=%s' % (
        cf.alchemy_api_url, cf.alchemy_api_key, cf.nft_contract, tokenid))
    data = r.json()
    # XXX fetch data['tokenUri']['gateway'] if metadata missing
    return data['metadata']['id']


def get_questing_raider_ids():
    contract = cf.get_eth_contract('questing-raiders')
    print('querying questing raiders via alchemy eth_call')
    owner = cf.get_polygon_web3().toChecksumAddress(cf.nft_owner)
    return contract.functions.getOwnedRaiders(owner).call()


def import_all_raiders(db):
    def rlist(r):
        return ' '.join(sorted(map(str, r)))
    raiders = set(lookup_nft_raider_id(i) for i in get_owned_raider_nfts())
    print('  found %d owned raiders: %s' % (len(raiders), rlist(raiders)))
    questy = get_questing_raider_ids()
    print('  found %d questing raiders: %s' % (len(questy), rlist(questy)))
    raiders.update(questy)

    cur = db.cursor()
    cur.execute('SELECT id FROM raiders')
    skipped = set(i[0] for i in cur.fetchall()) - raiders
    if len(skipped):
        print('Warning: skipping %d raiders: %s' % (
            len(skipped), rlist(skipped)))

    cur.execute('BEGIN TRANSACTION')
    ids = tuple(raiders)
    for first in range(0, len(ids), 100):
        import_raiders(cur, ids[first:first+100])
    print('imported or updated %d raiders via CR API' % (len(ids),))
    db.commit()


def import_one_raider(db, rid):
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    import_raiders(cur, (rid,))
    print('imported or updated raider %d via CR API' % (rid,))
    db.commit()


def import_raiders(cur, ids):
    r = requests.get('https://api.cryptoraiders.xyz/raiders/',
                     params={'ids[]': ids})
    for data in r.json():
        params = {i['trait_type']: i['value']
                  for i in data['attributes'] if 'value' in i}
        params['id'] = data['id']
        params['image'] = data['image']
        params['name'] = data['name'].split('] ', 1)[1]
        cur.execute('''INSERT OR REPLACE INTO raiders (
            id, name, image,
            race, generation, birthday, experience, level,
            strength, intelligence, agility, wisdom, charm, luck) VALUES (
            :id, :name, :image,
            :Race, :Generation, :Birthday, :Experience, :Level,
            :Strength, :Intelligence, :Agility, :Wisdom, :Charm, :Luck)''',
                    params)


def import_raider_gear(db, rid=None):
    source = 'crguru'
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    print('querying guru database')
    crg_domain = 'europe-west3-cryptoraiders-guru.cloudfunctions.net'
    crg_url = 'https://%s/getRawDatas' % (crg_domain,)
    hdr = {'authority': crg_domain,
           'origin': 'https://www.cryptoraiders.guru',
           'referer': 'https://www.cryptoraiders.guru/',
           'sec-fetch-site': 'cross-site',
           'sec-fetch-mode': 'cors',
           'sec-fetch-dest': 'empty'}
    r = requests.post(crg_url, headers=hdr,
                      json={'data': {'id': cf.nft_owner}})
    data = r.json()
    stats = ('strength', 'intelligence', 'agility', 'wisdom', 'charm', 'luck')
    for raider in data['data']['data']['raiders']:
        if rid is not None and rid != raider['tokenId']:
            continue
        print('  found %d items for %d - [%d] %s from %s' % (
            len(raider['inventory']), raider['tokenId'], raider['level'],
            raider['name'], raider['updatedAt']))
        cur.execute('DELETE FROM gear WHERE owner_id = :tokenId', raider)
        for inv in raider['inventory']:
            params = {'name': inv['item']['name'],
                      'slot': inv['item']['slot'],
                      'equipped': bool(inv.get('equipped')),
                      'owner_id': raider['tokenId'],
                      'source': source}
            params.update({s: 0 for s in stats})
            if inv['item'].get('stats'):
                params.update(inv['item']['stats'])
            cur.execute('''INSERT INTO gear (
                name, equipped, slot, owner_id, source,
                strength, intelligence, agility, wisdom, charm, luck) VALUES (
                :name, :equipped, :slot, :owner_id, :source,
                :strength, :intelligence, :agility, :wisdom, :charm, :luck)''',
                        params)
    db.commit()


def findraider(db, ident):
    try:
        return int(ident)
    except ValueError:
        cur = db.cursor()
        cur.execute('SELECT id FROM raiders WHERE lower(name) = ?',
                    (str(ident).lower(),))
        row = cur.fetchall()
        if len(row) > 0:
            return row[0][0]


def import_or_update(db, raider=None, gear=False):
    if raider is None:
        import_all_raiders(db)
    else:
        import_one_raider(db, raider)
    if gear:
        import_raider_gear(db, raider)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='raider',
                        help='Update only a single raider')
    parser.add_argument('-g', dest='gear', default=False, action='store_true',
                        help='import gear from CR guru')
    parser.add_argument
    args = parser.parse_args()

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)

    cf.makedirs()
    db = sqlite3.connect(cf.db_path)
    setupdb(db)

    raider = None
    if args.raider is not None:
        raider = findraider(db, args.raider)
        if raider is None:
            print('No raider named "%s" found' % (args.raider,))
            parser.print_usage()
            sys.exit(1)

    import_or_update(db, raider=raider, gear=args.gear)


if __name__ == '__main__':
    main()
