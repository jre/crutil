#!./venv/bin/python
import requests
import sqlite3
import argparse
import sys

cr_conf = __import__('cr-conf')
cf = cr_conf.CRConf()


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


def get_raider_nfts():
    print('querying polygon NFTs')
    r = requests.get('%s/%s/getNFTs/?owner=%s&contractAddresses[]=%s' % (
        cf.alchemy_api_url, cf.alchemy_api_key, cf.nft_owner, cf.nft_contract))
    data = r.json()
    print('found %d NFTs' % (data['totalCount'],))
    for nft in data['ownedNfts']:
        if nft['contract']['address'] == cf.nft_contract:
            yield nft['id']['tokenId']


def import_all_raiders(db):
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    cur.execute('DELETE FROM raiders')
    for token in get_raider_nfts():
        import_raider_meta(cur, token)
    db.commit()


def import_one_raider(db, rid):
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    cur.execute('SELECT nft_token FROM raiders WHERE id = ?', (rid,))
    token_id = cur.fetchone()[0]
    cur.execute('DELETE FROM raiders WHERE id = ?', (rid,))
    import_raider_meta(cur, token_id)
    db.commit()


def import_raider_meta(cur, tokenid):
    print('querying polygon NFT %s' % (tokenid,))
    r = requests.get('%s/%s/getNFTMetadata/?contractAddress=%s&tokenId=%s' % (
        cf.alchemy_api_url, cf.alchemy_api_key, cf.nft_contract, tokenid))
    data = r.json()
    # XXX fetch data['tokenUri']['gateway'] if metadata missing
    print('  found raider %(id)d %(name)s' % data['metadata'])
    params = {i['trait_type']: i['value']
              for i in data['metadata']['attributes'] if 'value' in i}
    params['id'] = data['metadata']['id']
    params['nft'] = tokenid
    params['image'] = data['metadata']['image']
    params['name'] = data['metadata']['name'].split('] ', 1)[1]
    cur.execute('''INSERT INTO raiders (
        id, nft_token, name, image,
        race, generation, birthday, experience, level,
        strength, intelligence, agility, wisdom, charm, luck) VALUES (
        :id, :nft, :name, :image,
        :Race, :Generation, :Birthday, :Experience, :Level,
        :Strength, :Intelligence, :Agility, :Wisdom, :Charm, :Luck)''', params)


def import_raider_gear(db):
    source = 'crguru'
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    cur.execute('DELETE FROM gear WHERE source = ?', (source,))
    print('querying cryptoraiders guru database')
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
    print('last updated at %s' % (data['data']['updated_at'],))
    stats = ('strength', 'intelligence', 'agility', 'wisdom', 'charm', 'luck')
    for raider in data['data']['data']['raiders']:
        print('  found gear for %(tokenId)d - [%(level)d] %(name)s' % raider)
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
        :strength, :intelligence, :agility, :wisdom, :charm, :luck)''', params)
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
    if args.raider is None:
        import_all_raiders(db)
    else:
        raider = findraider(db, args.raider)
        if raider is None:
            print('No raider named "%s" found' % (args.raider,))
            parser.print_usage()
            sys.exit(1)
        import_one_raider(db, raider)
    if args.gear:
        import_raider_gear(db)


if __name__ == '__main__':
    main()
