#!./venv/bin/python
import requests
import sqlite3
import argparse
import sys
import time
import datetime

cr_conf = __import__('cr-conf')
cf = cr_conf.conf


def setupdb(db):
    cur = db.cursor()

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

    cur.execute('''CREATE TABLE IF NOT EXISTS raids(
        raider INTEGER PRIMARY KEY,
        remaining INTEGER,
        last_raid INTEGER,
        last_endless INTEGER,
        FOREIGN KEY(raider) REFERENCES raiders(id))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS recruiting(
        raider INTEGER PRIMARY KEY,
        next INTEGER,
        cost INTEGER,
        FOREIGN KEY(raider) REFERENCES raiders(id))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS quests(
        raider INTEGER PRIMARY KEY,
        status INTEGER,
        contract VARCHAR(255),
        started_on INTEGER,
        return_divisor INTEGER,
        returns_on INTEGER,
        FOREIGN KEY(raider) REFERENCES raiders(id))''')

    db.commit()


def get_owned_raider_nfts():
    all_nfts = []
    for owner in cf.nft_owners():
        print('querying alchemy for NFTs owned by %s' % (owner,))
        r = requests.get('%s/%s/getNFTs/?owner=%s&contractAddresses[]=%s' % (
            cf.alchemy_api_url, cf.alchemy_api_key, owner, cf.nft_contract))
        data = r.json()
        found = tuple(n['id']['tokenId']
                      for n in data['ownedNfts']
                      if n['contract']['address'] == cf.nft_contract)
        print('  found %d raider NFTs: %s' % (
            data['totalCount'],
            ' '.join(str(int(i.lower().lstrip('0x').lstrip('0'), 16))
                     for i in found)))
        all_nfts.extend(found)
    return all_nfts


def lookup_nft_raider_id(tokenid):
    r = requests.get('%s/%s/getNFTMetadata/?contractAddress=%s&tokenId=%s' % (
        cf.alchemy_api_url, cf.alchemy_api_key, cf.nft_contract, tokenid))
    data = r.json()
    # XXX fetch data['tokenUri']['gateway'] if metadata missing
    return data['metadata']['id']


def get_questing_raider_ids():
    contract = cf.get_eth_contract('questing-raiders')
    ids = []
    for o in cf.nft_owners():
        print('querying questing raiders via alchemy eth_call for %s' % (o,))
        owner = cf.get_polygon_web3().toChecksumAddress(o)
        new_ids = contract.functions.getOwnedRaiders(owner).call()
        print('  found %d questing raiders: %s' % (
            len(new_ids), ' '.join(map(str, new_ids))))
        ids.extend(new_ids)
    return ids


def import_all_raiders(db):
    def rlist(r):
        return ' '.join(sorted(map(str, r)))
    raiders = set(lookup_nft_raider_id(i) for i in get_owned_raider_nfts())
    questy = get_questing_raider_ids()
    raiders.update(questy)

    cur = db.cursor()
    cur.execute('SELECT id FROM raiders')
    skipped = set(i[0] for i in cur.fetchall()) - raiders
    if len(skipped):
        # XXX can the owner method show if we have sold the raider
        # or maybe raiderInfo.raidersOwnedBy
        print('Warning: skipping %d unknown raiders: %s' % (
            len(skipped), rlist(skipped)))

    cur.execute('BEGIN TRANSACTION')
    ids = tuple(sorted(raiders))
    for first in range(0, len(ids), 100):
        import_raiders(cur, ids[first:first+100])
    print('imported or updated %d raiders via CR API' % (len(ids),))
    db.commit()
    return ids


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


def iso_datetime_to_secs(isotime):
    dt = datetime.datetime.fromisoformat(isotime.rstrip('Z'))
    return int(dt.timestamp())


def import_raider_gear(db, rid=None):
    source = 'crguru'
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')

    # this ultimately comes from https://play.cryptoraiders.xyz/api/raiders
    # but that endpoint requires a cookie
    crg_domain = 'europe-west3-cryptoraiders-guru.cloudfunctions.net'
    crg_url = 'https://%s/getRawDatas' % (crg_domain,)
    hdr = {'authority': crg_domain,
           'origin': 'https://www.cryptoraiders.guru',
           'referer': 'https://www.cryptoraiders.guru/',
           'sec-fetch-site': 'cross-site',
           'sec-fetch-mode': 'cors',
           'sec-fetch-dest': 'empty'}
    stats = ('strength', 'intelligence', 'agility', 'wisdom', 'charm', 'luck')

    for owner in cf.nft_owners():
        print('querying guru database for %s' % (owner,))
        r = requests.post(crg_url, headers=hdr,
                          json={'data': {'id': owner}})
        if not r.ok:
            print('  failed %d %s' % (r.status_code, r.reason))
            continue
        data = r.json()

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
                    strength, intelligence, agility, wisdom, charm, luck)
                    VALUES (:name, :equipped, :slot, :owner_id, :source,
                    :strength, :intelligence, :agility, :wisdom, :charm, :luck
                    )''', params)

            params = {
                'raider': raider['tokenId'],
                'remaining': raider['raidsRemaining'],
                'last_raid': iso_datetime_to_secs(raider['lastRaided']),
                'last_endless': iso_datetime_to_secs(raider['lastEndless']),
            }
            cur.execute('''INSERT OR REPLACE INTO raids (
                raider, remaining, last_raid, last_endless) VALUES (
                :raider, :remaining, :last_raid, :last_endless)''', params)
    print('finished updating from guru')
    db.commit()


def import_raider_recruitment(db, idlist):
    cur = db.cursor()
    recruiting = cf.get_eth_contract('recruiting').functions
    print('querying recruitment contracts for %s raiders' % (len(idlist),))
    for rid in idlist:
        cost = recruiting.getRaiderRecruitCost(rid).call()
        utcnow = int(time.time())
        # XXX does this return a negative number when a recruit is available?
        delta = recruiting.nextRecruitTime(rid).call()
        cur.execute('''INSERT OR REPLACE INTO recruiting (
            raider, next, cost) VALUES (?, ?, ?)''',
                    (rid, utcnow + delta, cost))
    db.commit()


def import_raider_quests(db, idlist):
    def sql_insert(p):
        cur.execute(
            'INSERT OR REPLACE INTO quests (%s) VALUES (%s)' % (
                ', '.join(sorted(p.keys())), ', '.join('?' * len(p))),
            tuple(p[i] for i in sorted(p.keys())))

    cur = db.cursor()
    print('querying questing contracts for %s raiders' % (len(idlist),))
    questing = cf.get_eth_contract('questing-raiders').functions

    for rid in sorted(idlist):
        params = {'raider': rid}
        if not questing.onQuest(rid).call():
            params['status'] = 0
            sql_insert(params)
            continue
        params['contract'] = questing.raiderQuest(rid).call()

        try:
            myquest = cf.get_eth_contract(address=params['contract']).functions
        except ValueError:
            print('unknown quest contract for raider %d: %s' % (
                rid, params['contract']))
            continue

        params['status'] = myquest.raiderStatus(rid).call()
        returning = cf.quest_returning[params['status']]
        if returning is None:
            sql_insert(params)
            continue
        if returning:
            params['returns_on'] = myquest.timeHome(rid).call()
        else:
            params['started_on'] = myquest.questStartedTime(rid).call()
            params['return_divisor'] = myquest.returnHomeTimeDivisor().call()
        sql_insert(params)
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


def import_or_update(db, raider=None, gear=True, timing=True):
    if raider is None:
        ids = import_all_raiders(db)
    else:
        ids = (raider,)
        import_one_raider(db, raider)
    if gear:
        import_raider_gear(db, raider)
    if timing:
        import_raider_recruitment(db, ids)
        import_raider_quests(db, ids)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='raider',
                        help='Update only a single raider')
    parser.add_argument('-G', dest='gear', default=True, action='store_false',
                        help='Skip import from CR guru')
    parser.add_argument('-T', dest='times', default=True, action='store_false',
                        help='Skip retrieving timing information')
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

    import_or_update(db, raider=raider, gear=args.gear, timing=args.times)


if __name__ == '__main__':
    main()
