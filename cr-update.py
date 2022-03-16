#!./venv/bin/python
import requests
import sqlite3
import argparse
import sys
import time
import datetime
import json

cr_conf = __import__('cr-conf')
cf = cr_conf.conf


def noop(*a, **kw):
    pass


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

    cur.execute('''CREATE TABLE IF NOT EXISTS gear_localid(
        local_id INTEGER PRIMARY KEY,
        equipped INTEGER,
        slot VARCHAR(255),
        raider_id INTEGER,
        source VARCHAR(255),
        dedup_id INTEGER,
        FOREIGN KEY(raider_id) REFERENCES raiders(id),
        FOREIGN KEY(dedup_id) REFERENCES gear_uniq(dedup_id))''')

    cur.execute('''CREATE TABLE IF NOT EXISTS gear_uniq(
        dedup_id INTEGER PRIMARY KEY,
        key VARCHAR(255) UNIQUE,
        name VARCHAR(255),
        strength INTEGER,
        intelligence INTEGER,
        agility INTEGER,
        wisdom INTEGER,
        charm INTEGER,
        luck INTEGER)''')

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


def ensure_dedup_gear(cur, name_stats):
    assert len(name_stats) == 7
    key = json.dumps(name_stats)
    cur.execute('SELECT dedup_id FROM gear_uniq WHERE key = ?', (key,))
    rows = cur.fetchall()
    if len(rows):
        return rows[0][0]

    cur.execute('''INSERT INTO gear_uniq (key, name,
        strength, intelligence, agility, wisdom, charm, luck) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?)''', (key,) + tuple(name_stats))
    return cur.lastrowid


def raider_has_gear(cur, raider_id, dedup_id):
    cur.execute('''SELECT local_id FROM gear_localid
        WHERE dedup_id = ? AND raider_id = ?''', (dedup_id, raider_id))
    rows = cur.fetchall()
    if len(rows):
        return rows[0][0]


def migrate_old_gear(db, periodic=noop):
    cur = db.cursor()
    periodic(message='migrating gear to deduplicated tables')
    cur.execute('''SELECT equipped, slot, owner_id, source,
        name, strength, intelligence, agility, wisdom, charm, luck
        FROM gear''')
    rows = cur.fetchall()

    count = 0
    for row in rows:
        periodic()
        equip, slot, raider_id, source = row[:4]
        name_stats = row[4:]
        dedup_id = ensure_dedup_gear(cur, name_stats)
        if raider_has_gear(cur, raider_id, dedup_id):
            continue
        cur.execute('''INSERT INTO gear_localid (equipped, slot, raider_id,
            source, dedup_id) VALUES (?, ?, ?, ?, ?)''', (
                equip, slot, raider_id, source, dedup_id))
        count += 1
    periodic(message='migrated %d gear items' % (count,))
    db.commit()


def get_owned_raider_nfts(periodic=noop):
    all_nfts = []
    for owner in cf.nft_owners():
        periodic(message='querying raider NFTs for %s' % (owner,))
        r = requests.get('%s/%s/getNFTs/?owner=%s&contractAddresses[]=%s' % (
            cf.alchemy_api_url, cf.alchemy_api_key, owner, cf.nft_contract))
        data = r.json()
        found = tuple(n['id']['tokenId']
                      for n in data['ownedNfts']
                      if n['contract']['address'] == cf.nft_contract)
        periodic(message='found %d raider NFTs: %s' % (
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


def get_questing_raider_ids(periodic=noop):
    periodic(message='fetching contract ABI')
    contract = cf.get_eth_contract('questing-raiders')
    ids = []
    for o in cf.nft_owners():
        periodic(message='querying questing raiders for %s' % (o,))
        owner = cf.get_polygon_web3().toChecksumAddress(o)
        new_ids = contract.functions.getOwnedRaiders(owner).call()
        periodic(message='found %d questing raiders: %s' % (
            len(new_ids), ' '.join(map(str, new_ids))))
        ids.extend(new_ids)
    return ids


def import_all_raiders(db, periodic=noop):
    def rlist(r):
        return ' '.join(sorted(map(str, r)))
    periodic('Counting raiders', 'counting owned raider NFTs on chain')
    raiders = set(lookup_nft_raider_id(i)
                  for i in get_owned_raider_nfts(periodic=periodic))
    periodic(message='counting questing raider NFTs on chain')
    questy = get_questing_raider_ids()
    raiders.update(questy)
    periodic(message='found %d raiders total' % len(raiders))

    cur = db.cursor()
    cur.execute('SELECT id FROM raiders')
    skipped = set(i[0] for i in cur.fetchall()) - raiders
    if len(skipped):
        # XXX can the owner method show if we have sold the raider
        # or maybe raiderInfo.raidersOwnedBy
        print('Warning: skipping %d unknown raiders: %s' % (
            len(skipped), rlist(skipped)))

    periodic()

    cur.execute('BEGIN TRANSACTION')
    ids = tuple(sorted(raiders))
    import_raiders(cur, ids, periodic=periodic)
    db.commit()
    periodic()
    return ids


def import_one_raider(db, rid, periodic=noop):
    periodic()
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    import_raiders(cur, (rid,), periodic=periodic)
    db.commit()
    periodic()


def import_raiders(cur, all_ids, periodic=noop):
    periodic('Importing raider data from CR API')

    for first in range(0, len(all_ids), 100):
        chunk_ids = all_ids[first:first+100]
        periodic(message='fetching %d raiders' % (len(chunk_ids),))
        r = requests.get('https://api.cryptoraiders.xyz/raiders/',
                         params={'ids[]': chunk_ids})
        periodic()
        data_rows = r.json()

        for data in data_rows:
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
            periodic()
        periodic(message='imported %d/%d' % (first + len(data_rows),
                                             len(all_ids)))


def iso_datetime_to_secs(isotime):
    dt = datetime.datetime.fromisoformat(isotime.rstrip('Z'))
    return int(dt.timestamp())


def import_raider_gear(db, rid=None, periodic=noop):
    periodic('Importing guru data', message='querying API')
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
    found = {}

    for owner in cf.nft_owners():
        periodic(message='querying guru database for %s' % (owner,))
        r = requests.post(crg_url, headers=hdr,
                          json={'data': {'id': owner}})
        if not r.ok:
            # XXX how to signal failure here and keep going
            print('  failed %d %s' % (r.status_code, r.reason))
            continue
        data = r.json()
        periodic(message='found data for %d raiders for %s' % (
            len(data['data']['data']['raiders']), owner))

        for raider in data['data']['data']['raiders']:
            raider_id = raider['tokenId']
            if rid is not None and rid != raider_id:
                continue
            found.setdefault(raider_id, 0)
            periodic(message='found %d items for %d - [%d] %s from %s' % (
                len(raider['inventory']), raider_id, raider['level'],
                raider['name'], raider['updatedAt']))

            periodic()
            for inv in raider['inventory']:
                name_stats = [inv['item']['name']]
                name_stats.extend((inv['item'].get('stats') or {}).get(i, 0)
                                  for i in stats)
                dedup_id = ensure_dedup_gear(cur, name_stats)
                if raider_has_gear(cur, raider_id, dedup_id):
                    periodic()
                    continue
                params = {'dedup_id': dedup_id,
                          'slot': inv['item']['slot'],
                          'equipped': bool(inv.get('equipped')),
                          'raider_id': raider_id,
                          'source': source}
                cur.execute('''INSERT INTO gear_localid (
                    dedup_id, equipped, slot, raider_id, source) VALUES (
                    :dedup_id, :equipped, :slot, :raider_id, :source)''',
                            params)
                periodic()
                found[raider_id] += 1

            params = {
                'raider': raider_id,
                'remaining': raider['raidsRemaining'],
                'last_raid': iso_datetime_to_secs(raider['lastRaided']),
                'last_endless': iso_datetime_to_secs(raider['lastEndless']),
            }
            cur.execute('''INSERT OR REPLACE INTO raids (
                raider, remaining, last_raid, last_endless) VALUES (
                :raider, :remaining, :last_raid, :last_endless)''', params)
    periodic()
    db.commit()
    periodic(message='Found %d new gear items for %d raiders' % (
        sum(found.values()), len(found)))


def import_raider_recruitment(db, idlist, periodic=noop):
    periodic('Importing recruitment data from chain',
             message='fetching contract ABI')
    cur = db.cursor()
    recruiting = cf.get_eth_contract('recruiting').functions
    for idx, rid in enumerate(sorted(idlist)):
        periodic(message='%d/%d - raider %d' % (idx, len(idlist), rid))
        cost = recruiting.getRaiderRecruitCost(rid).call()
        periodic()
        utcnow = int(time.time())
        # XXX does this return a negative number when a recruit is available?
        delta = recruiting.nextRecruitTime(rid).call()
        periodic()
        cur.execute('''INSERT OR REPLACE INTO recruiting (
            raider, next, cost) VALUES (?, ?, ?)''',
                    (rid, utcnow + delta, cost))
    periodic(message='%d/%d - done' % (len(idlist), len(idlist)))
    db.commit()
    periodic()


def import_raider_quests(db, idlist, periodic=noop):
    def sql_insert(p):
        cur.execute(
            'INSERT OR REPLACE INTO quests (%s) VALUES (%s)' % (
                ', '.join(sorted(p.keys())), ', '.join('?' * len(p))),
            tuple(p[i] for i in sorted(p.keys())))
        periodic()

    cur = db.cursor()
    periodic('Importing quest data from chain',
             message='fetching contract ABI')
    questing = cf.get_eth_contract('questing-raiders').functions

    for idx, rid in enumerate(sorted(idlist)):
        periodic(message='%d/%d - raider %d' % (idx, len(idlist), rid))
        params = {'raider': rid}
        if not questing.onQuest(rid).call():
            periodic()
            params['status'] = 0
            sql_insert(params)
            continue
        periodic()
        params['contract'] = questing.raiderQuest(rid).call()
        periodic()

        try:
            myquest = cf.get_eth_contract(address=params['contract']).functions
            periodic()
        except ValueError:
            print('unknown quest contract for raider %d: %s' % (
                rid, params['contract']))
            continue

        params['status'] = myquest.raiderStatus(rid).call()
        periodic()
        returning = cf.quest_returning[params['status']]
        if returning is None:
            sql_insert(params)
            continue
        if returning:
            params['returns_on'] = myquest.timeHome(rid).call()
            periodic()
        else:
            params['started_on'] = myquest.questStartedTime(rid).call()
            periodic()
            params['return_divisor'] = myquest.returnHomeTimeDivisor().call()
            periodic()
        sql_insert(params)
    periodic(message='%d/%d - done' % (len(idlist), len(idlist)))
    db.commit()
    periodic()


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


def import_or_update(db, raider=None, gear=True, timing=True,
                     periodic=noop):
    if raider is None:
        periodic('Updating all raiders')
        ids = import_all_raiders(db, periodic=periodic)
    else:
        periodic('Updating raider %d' % (raider,))
        ids = (raider,)
        import_one_raider(db, raider, periodic=periodic)
    if gear:
        import_raider_gear(db, raider, periodic=periodic)
        migrate_old_gear(db, periodic=periodic)
    if timing:
        import_raider_recruitment(db, ids, periodic=periodic)
        import_raider_quests(db, ids, periodic=periodic)


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

    last_section = ['']

    def showstatus(section=None, message=None):
        if section:
            last_section[0] = section
        if message:
            print(' %s: %s' % (last_section[0], message))
        elif section:
            print(' %s' % (section,))

    import_or_update(db, raider=raider, gear=args.gear, timing=args.times,
                     periodic=showstatus)


if __name__ == '__main__':
    main()
