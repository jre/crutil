#!./venv/bin/python
import argparse
import contextlib
import datetime
import json
import mmh3
import os
import requests
import sqlite3
import struct
import subprocess
import sys
import tempfile
import threading

cr_conf = __import__('cr-conf')
cf = cr_conf.conf
cr_report = __import__('cr-report')

schema_version = 2


class DBVersionError(Exception):
    def __init__(self, version):
        self.version = version
        super().__init__('expected DB schema version %d but found %d' % (
            schema_version, version))


def noop(*a, **kw):
    pass


_last_section = ['']


def periodic_print(section=None, message=None):
    if section:
        _last_section[0] = section
    if message:
        print(' %s: %s' % (_last_section[0], message))
    elif section:
        print(' %s' % (section,))


def setupdb(db):
    cur = db.cursor()

    cur.execute('''CREATE TABLE IF NOT EXISTS meta(
        name VARCHAR(255) PRIMARY KEY,
        value INTEGER)''')
    cur.execute('INSERT OR IGNORE INTO meta (name, value) VALUES (?, ?)',
                ('schema-version', schema_version))

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
        local_id INTEGER PRIMARY KEY,
        hash INTEGER NOT NULL,
        raider_id INTEGER NOT NULL,
        name VARCHAR(255),
        equipped INTEGER,
        slot VARCHAR(255),
        strength INTEGER,
        intelligence INTEGER,
        agility INTEGER,
        wisdom INTEGER,
        charm INTEGER,
        luck INTEGER,
        FOREIGN KEY(raider_id) REFERENCES raiders(id))''')
    cur.execute('CREATE INDEX IF NOT EXISTS gear__hash on gear(hash)')
    cur.execute('CREATE INDEX IF NOT EXISTS gear__raider on gear(raider_id)')

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
        reward_time INTEGER,
        FOREIGN KEY(raider) REFERENCES raiders(id))''')

    db.commit()


def schema_upgrade_v1(db):
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    cur.execute('''CREATE TABLE meta(
        name VARCHAR(255) PRIMARY KEY,
        value INTEGER)''')
    cur.execute('''INSERT INTO meta (name, value)
        VALUES (?, ?), (?, ?), (?, ?), (?, ?)''',
                ('schema-version', 2, 'snapshot-started', 0,
                 'snapshot-updated', 0, 'snapshot-finished', 0))
    db.commit()


def schema_upgrade_v2(db):
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    cur.execute('DROP TABLE IF EXISTS gear')
    cur.execute('''CREATE TABLE gear(
        local_id INTEGER PRIMARY KEY,
        hash INTEGER NOT NULL,
        raider_id INTEGER NOT NULL,
        name VARCHAR(255),
        equipped INTEGER,
        slot VARCHAR(255),
        strength INTEGER,
        intelligence INTEGER,
        agility INTEGER,
        wisdom INTEGER,
        charm INTEGER,
        luck INTEGER,
        FOREIGN KEY(raider_id) REFERENCES raiders(id))''')
    cur.execute('CREATE INDEX gear__hash on gear(hash)')
    cur.execute('CREATE INDEX gear__raider on gear(raider_id)')
    cur.execute('''SELECT l.local_id, l.raider_id, l.equipped, l.slot, u.name,
        u.strength, u.intelligence, u.agility, u.wisdom, u.charm, u.luck
        FROM gear_localid l, gear_uniq u WHERE l.dedup_id = u.dedup_id''')
    for row in list(cur.fetchall()):
        hash = hash_gear_uniq(*row[-7:])
        cur.execute('''INSERT INTO gear (local_id, raider_id, equipped, slot,
            name, strength, intelligence, agility, wisdom, charm, luck, hash)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', row + (hash,))
    cur.execute('DROP TABLE gear_localid')
    cur.execute('DROP TABLE gear_uniq')
    db.commit()


def checkdb(db):
    upgrades = (schema_upgrade_v1, schema_upgrade_v2)
    cur = db.cursor()
    try:
        cur.execute('SELECT value FROM meta WHERE name = ?',
                    ('schema-version',))
        db_vers = cur.fetchone()[0]
    except sqlite3.OperationalError:
        cur.execute('SELECT COUNT(*) FROM PRAGMA_TABLE_INFO(?)', ('raiders',))
        if cur.fetchone()[0] == 0:
            # empty database
            return
        # apparently valid database lacking meta table, call this version 0
        db_vers = 0

    orig_vers = db_vers
    while db_vers < schema_version and db_vers < len(upgrades):
        upgrades[db_vers](db)
        db_vers += 1
    if orig_vers != db_vers:
        cur.execute('INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)',
                    ('schema-version', schema_version))
        db.commit()
        print('note: upgraded database schema from version %d to %d' % (
            orig_vers, db_vers), file=sys.stderr)
    if db_vers != schema_version:
        raise DBVersionError(db_vers)


class GearDB():
    _dumpver = 1
    _extra_keys = set(('endless',))

    def __init__(self):
        self._rows = [None]
        self._gearids = {}
        self._extra = {}
        self._lock = threading.Lock()

    @property
    def last_local_id(self):
        nrows = len(self._rows)
        return nrows - 1 if nrows > 1 else None

    def _add_gear(self, hash, raider_id, slot, name, stats):
        self._rows.append((hash, raider_id, slot, name) + tuple(stats))
        self._gearids.setdefault(raider_id, {})[hash] = self.last_local_id

    def _get_localid(self, raider_id, hash):
        return self._gearids.get(raider_id, {}).get(hash)

    def _set_extra(self, raider_id, key, val):
        assert key in self._extra_keys
        self._extra.setdefault(raider_id, {})[key] = val

    def load(self, fh):
        data = json.load(fh)
        if data.get('version') != self._dumpver:
            print('expected gear json version %d but found %s' % (
                self._dumpver, data.get('version')))
            sys.exit(1)

        with self._lock:
            self._rows = [None]
            self._gearids = {}
            self._extra = data['raiders']
            for row in data['gear']:
                hash = hash_gear_uniq(*row[-7:])
                assert hash == row[0]
                params = [hash] + list(row[1:-6]) + [row[-6:]]
                self._add_gear(*params)

    def save(self, fh):
        with self._lock:
            data = {'version': self._dumpver,
                    'raiders': self._extra,
                    'gear': self._rows[1:]}
            json.dump(data, fh)

    def load_from_sql(self, cur):
        cur.execute('''SELECT local_id, hash, raider_id, slot, name,
            strength, intelligence, agility, wisdom, charm, luck
            FROM gear ORDER BY local_id''')
        rows = list(cur.fetchall())
        cur.execute('''SELECT raider, last_endless
            FROM raids WHERE last_endless''')
        extra = {r: {'endless': l} for r, l in cur.fetchall()}

        with self._lock:
            self._rows = [None]
            self._gearids = {}
            self._extra = extra
            next_id = 1
            for row in rows:
                local_id, hash, raider_id, slot, name = row[:5]
                stats = row[5:]
                if local_id > next_id:
                    self._rows.extend(((None,),) * (local_id - next_id))
                self._add_gear(hash, raider_id, slot, name, stats)
                assert local_id == self.last_local_id
                next_id = local_id + 1

    def save_to_sql(self, cur):
        cur.execute('SELECT MAX(local_id) FROM gear')
        next_local_id = (cur.fetchone()[0] or 0) + 1

        newrows = ()
        with self._lock:
            if next_local_id <= self.last_local_id:
                newrows = self._rows[next_local_id:]
            endless = [{'r': r, 'e': d['endless']}
                       for r, d in self._extra.items() if 'endless' in d]

        cur.executemany('''INSERT INTO gear (hash, raider_id, slot, name, %s)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''' % (
                ','.join(cr_conf.stats)), newrows)
        cur.executemany('''INSERT INTO raids (raider, last_endless)
            VALUES (:r, :e) ON CONFLICT (raider) DO UPDATE
            SET last_endless = :e WHERE raider = :r''', endless)

        return len(newrows)

    def add_multi_inventory(self, multi):
        res = []
        with self._lock:
            for raider in multi:
                raider_id = raider['tokenId']
                for inv in raider['inventory']:
                    hash, name, stats = get_item_stats(inv)
                    local_id = self._get_localid(raider_id, hash)
                    slot = inv['item']['slot']
                    wasnew = local_id is None
                    if local_id is None:
                        self._add_gear(hash, raider_id, slot, name, stats)
                        local_id = self.last_local_id
                    res.append((local_id, wasnew, inv))
                if 'lastEndless' in raider:
                    secs = iso_datetime_to_secs(raider['lastEndless'])
                    self._set_extra(raider_id, 'endless', secs)
        return res


geardb = GearDB()


@contextlib.contextmanager
def permatempfile(dest, suffix=None, mode=0o644, binary=True):
    destdir, destfile = os.path.split(dest)
    tmp = tempfile.NamedTemporaryFile(
        mode=('w+b' if binary else 'w+'), delete=False,
        dir=destdir, prefix='.tmp-', suffix=suffix)
    yield tmp
    tmp.close()
    os.chmod(tmp.name, mode)
    os.rename(tmp.name, dest)


def mv_to(srcpath, destpath):
    subprocess.run(('mv', srcpath, destpath), check=True)


def gzip_to(srcpath, destdir, destname, periodic=noop):
    with permatempfile(os.path.join(destdir, destname), mode=0o444) as tmp:
        periodic('Compressing')
        subprocess.run(('gzip', '-9cn', srcpath),
                       stdin=subprocess.DEVNULL,
                       stdout=tmp.fileno(),
                       check=True)
    periodic(message='to %s' % (destname,))


def gzip_from(srcpath, destdir, destname, periodic=noop):
    with permatempfile(os.path.join(destdir, destname), mode=0o444) as tmp:
        periodic('Decompressing')
        subprocess.run(('gzip', '-cd', srcpath),
                       stdin=subprocess.DEVNULL,
                       stdout=tmp.fileno(),
                       check=True)
    periodic(message='to %s' % (destname,))


def hash_gear_uniq(name, *stats):
    assert isinstance(name, str), (name,)
    assert len(stats) == 6, (stats,)
    assert all(isinstance(i, int) for i in stats), (stats,)
    namebuf = name.encode('utf-8')
    keybuf = struct.pack('!%ss6q' % len(namebuf), namebuf, *stats)
    pair = mmh3.hash64(keybuf)
    return pair[0] ^ pair[1]


def get_item_stats(item):
    name = item['item']['name']
    stats_dict = item['item'].get('stats', {})
    stats = tuple(stats_dict.get(i, 0) for i in cr_conf.stats)
    hash = hash_gear_uniq(name, *stats)
    return hash, name, stats


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


def get_raider_ids(periodic=noop):
    periodic('Counting raiders', 'counting owned raider NFTs on chain')
    owned = set(lookup_nft_raider_id(i)
                for i in get_owned_raider_nfts(periodic=periodic))
    periodic(message='counting questing raider NFTs on chain')
    questing = set(get_questing_raider_ids(periodic=periodic))
    periodic(message='found %d raiders total' % (len(owned) + len(questing)))
    return owned, questing


def import_all_raiders(db, periodic=noop):
    owned, questing = get_raider_ids(periodic=periodic)
    raiders = set(owned)
    raiders.update(questing)

    cur = db.cursor()
    cur.execute('SELECT id FROM raiders')
    skipped = set(i[0] for i in cur.fetchall()) - raiders
    if len(skipped):
        # XXX can the owner method show if we have sold the raider
        # or maybe raiderInfo.raidersOwnedBy
        print('Warning: skipping %d unknown raiders: %s' % (
            len(skipped), ' '.join(sorted(map(str, skipped)))),
              file=sys.stderr)

    periodic()

    cur.execute('BEGIN TRANSACTION')
    ids = tuple(sorted(raiders))
    import_raiders(cur, ids, full=owned, periodic=periodic)
    db.commit()
    periodic()
    return ids, questing


def import_some_raiders(db, rids, periodic=noop):
    periodic()
    cur = db.cursor()
    cur.execute('BEGIN TRANSACTION')
    import_raiders(cur, rids, full=True, periodic=periodic)
    db.commit()
    periodic()


def import_raiders(cur, all_ids, full=False, periodic=noop):
    last_weekly = cr_report.last_weekly_refresh(
        datetime.datetime.utcnow())
    periodic('Importing raider data from CR API')

    all_raider_meta = []
    for first in range(0, len(all_ids), 50):
        chunk_ids = all_ids[first:first+50]
        periodic(message='fetching %d raiders' % (len(chunk_ids),))
        r = requests.get('%s/raiders/' % (cf.cr_api_url,),
                         params={'ids[]': chunk_ids})
        periodic()
        data_rows = r.json()

        for idx, data in enumerate(data_rows, first):
            periodic(message='importing raider %d/%d - %d %s' % (
                idx, len(all_ids), data['id'], data['name']))
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
            if not full or (isinstance(full, set) and data['id'] not in full):
                cur.execute('''SELECT remaining, last_raid
                    FROM raids WHERE raider = ?''', (data['id'],))
                rows = tuple(cur.fetchall())
                if len(rows):
                    remaining, last_raid = rows[0]
                    if last_raid > last_weekly.timestamp() and remaining == 0:
                        continue
            r = requests.get('%s/game/raider/%s' % (cf.cr_api_url, data['id']),
                             params={'key': cf.cr_api_key})
            all_raider_meta.append(r.json())
        periodic(message='imported %d/%d' % (first + len(data_rows),
                                             len(all_ids)))
    import_raider_extended(cur, all_raider_meta, periodic=periodic)


def import_raider_extended(cur, raiders, periodic=noop):
    periodic(message='processing extended api data')

    for data in raiders:
        data['lastRaidedSecs'] = (iso_datetime_to_secs(data['lastRaided'])
                                  if 'lastRaided' in data else 0)
    cur.executemany('''INSERT INTO raids (raider, remaining, last_raid)
        VALUES (:tokenId, :raidsRemaining, :lastRaidedSecs)
        ON CONFLICT (raider) DO UPDATE
        SET remaining = :raidsRemaining, last_raid = :lastRaidedSecs
        WHERE raider = :tokenId''', raiders)
    periodic(message='updaded raiding data')

    equipped_ids = []
    for local_id, was_new, item in geardb.add_multi_inventory(raiders):
        if item.get('equipped', False):
            equipped_ids.append(local_id)
    newcount = geardb.save_to_sql(cur)
    periodic(message='added %d new item(s)' % (newcount,))
    cur.execute('UPDATE gear SET equipped = FALSE WHERE raider_id IN (%s)' % (
        ','.join(('?',) * len(raiders))),
                [r['tokenId'] for r in raiders])
    cur.executemany('UPDATE gear SET equipped = TRUE WHERE local_id = ?',
                    ((i,) for i in equipped_ids))
    periodic()


def iso_datetime_to_secs(isotime):
    dt = datetime.datetime.fromisoformat(isotime.rstrip('Z'))
    return int(dt.timestamp())


def import_raider_gear(db, periodic=noop):
    periodic('Importing guru data', message='querying API')
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

    raiders = []
    for owner in cf.nft_owners():
        periodic(message='querying guru database for %s' % (owner,))
        r = requests.post(crg_url, headers=hdr,
                          json={'data': {'id': owner}})
        if not r.ok:
            # XXX how to signal failure here and keep going
            print('failed %d %s' % (r.status_code, r.reason), file=sys.stderr)
            continue
        data = r.json()
        periodic(message='found data for %d raiders for %s' % (
            len(data['data']['data']['raiders']), owner))
        raiders.extend(data['data']['data']['raiders'])

    geardb.add_multi_inventory(raiders)
    newcount = geardb.save_to_sql(cur)
    periodic(message='added %d new item(s)' % (newcount,))
    db.commit()


def import_raider_recruitment(db, idlist, full=False, periodic=noop):
    periodic('Importing recruitment data from chain',
             message='fetching contract ABI')
    cur = db.cursor()
    recruiting = cf.get_eth_contract('recruiting').functions
    for idx, rid in enumerate(sorted(idlist)):
        periodic(message='%d/%d - raider %d' % (idx, len(idlist), rid))
        cost, next_time = None, None
        if not full:
            cur.execute('SELECT next, cost FROM recruiting WHERE raider = ?',
                        (rid,))
            rows = cur.fetchall()
            if len(rows):
                next_time, cost = rows[0]
        changed = False

        if cost is None or cost > 1000000000:
            cost = recruiting.getRaiderRecruitCost(rid).call()
            periodic()
            changed = True

        utcnow_secs = datetime.datetime.utcnow().timestamp()
        if next_time is None or next_time < utcnow_secs:
            if recruiting.canRaiderRecruit(rid).call():
                next_time = 0
            else:
                delta = recruiting.nextRecruitTime(rid).call()
                next_time = utcnow_secs + delta
            periodic()
            changed = True

        if changed:
            cur.execute('''INSERT OR REPLACE INTO recruiting (
                raider, next, cost) VALUES (?, ?, ?)''',
                        (rid, next_time, cost))
    periodic(message='%d/%d - done' % (len(idlist), len(idlist)))
    db.commit()
    periodic()


def import_raider_quests(db, idlist, questing_ids=None,
                         periodic=noop):
    import web3

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
        onquest = (rid in questing_ids) if questing_ids else \
            questing.onQuest(rid).call()
        periodic()
        params = {'raider': rid}
        if not onquest:
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
                rid, params['contract']), file=sys.stderr)
            continue

        params['status'] = myquest.raiderStatus(rid).call()
        periodic()
        returning = cf.quest_returning[params['status']]
        if returning is None:
            sql_insert(params)
            continue
        utcnow_secs = datetime.datetime.utcnow().timestamp()
        if returning:
            try:
                delta = myquest.timeTillHome(rid).call()
            except web3.exceptions.ContractLogicError:
                delta = -1
            params['returns_on'] = 0 if delta <= 0 else utcnow_secs + delta
            periodic()
        else:
            questing_secs = myquest.timeQuesting(rid).call()
            params['started_on'] = utcnow_secs - questing_secs
            periodic()
            params['return_divisor'] = myquest.returnHomeTimeDivisor().call()
            periodic()
            params['reward_time'] = myquest.calcRaiderRewardTime(rid).call()
            periodic()
        sql_insert(params)
    periodic(message='%d/%d - done' % (len(idlist), len(idlist)))
    db.commit()
    periodic()


def findraider(db, ident):
    cur = db.cursor()
    try:
        rid = int(ident)
    except ValueError:
        cur.execute('SELECT id FROM raiders WHERE lower(name) = ?',
                    (str(ident).lower(),))
        row = cur.fetchall()
        if len(row) > 0:
            return row[0][0], True
        else:
            return None, False
    cur.execute('SELECT COUNT(id) FROM raiders WHERE id = ?', (rid,))
    return rid, (cur.fetchone()[0] > 0)


def import_or_update(db, started_at=None, raiders=None, basic=True, gear=True,
                     recruiting=True, questing=True, periodic=noop):
    info = {'schema-version': schema_version}
    cur = db.cursor()
    questers = None
    need_finish = False

    if raiders is None:
        if started_at is None:
            started_at = datetime.datetime.utcnow()
        periodic('Updating all raiders')
        cur.execute('INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)',
                    ('snapshot-started', int(started_at.timestamp())))
        db.commit()
        info['snapshot-started'] = int(started_at.timestamp())
        need_finish = True
        raiders, questers = import_all_raiders(db, periodic=periodic)
    else:
        periodic('Updating raider(s) %s' % (raiders,))
        cur.execute("SELECT value FROM meta WHERE name = 'snapshot-started'")
        info['snapshot-started'] = cur.fetchone()[0]
        if basic:
            import_some_raiders(db, raiders, periodic=periodic)
    if gear:
        import_raider_gear(db, periodic=periodic)
    if recruiting:
        import_raider_recruitment(db, raiders, periodic=periodic)
    if questing:
        import_raider_quests(db, raiders, questing_ids=questers,
                             periodic=periodic)

    finished_at = datetime.datetime.utcnow()
    cur.execute('INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)',
                ('snapshot-updated', int(finished_at.timestamp())))
    info['snapshot-updated'] = int(finished_at.timestamp())
    if need_finish:
        cur.execute('INSERT OR REPLACE INTO meta (name, value) VALUES (?, ?)',
                    ('snapshot-finished', int(finished_at.timestamp())))
    db.commit()
    return info, raiders


def ensure_raider_ids(db, val, usage):
    raider, trusted = findraider(db, val)
    if raider is None:
        print('No raider named "%s" found' % (val,), file=sys.stderr)
        usage()
        sys.exit(1)
    elif not trusted:
        owned, questing = get_raider_ids(periodic=periodic_print)
        if raider not in owned and raider not in questing:
            print('raider %d not owned by %s' % (
                raider, ' '.join(cf.nft_owners())), file=sys.stderr)
            sys.exit(1)
    return raider


def request_update(raiders, basic=True, gear=True, recruiting=True,
                   questing=True, periodic=noop, forcelocal=None):
    if not cf.can_update_remote or forcelocal:
        db = cf.opendb()
        import_or_update(db, raiders=raiders, basic=basic, gear=gear,
                         recruiting=recruiting, questing=questing,
                         periodic=periodic)
        return db

    url = cf.crutil_api_url.rstrip('/')
    params = {'apikey': cf.crutil_api_key}
    if raiders is None:
        periodic('Rebuilding')
        r = requests.get(url + '/rebuild', params, stream=True)
    else:
        periodic('Updating')
        params['ids[]'] = tuple(raiders)
        r = requests.get(url + '/update', params, stream=True)
    for line in r.iter_lines():
        periodic(message=line.decode())

    maybe_download_update(periodic=periodic)
    return cf.opendb()


def download_and_install_snapshot(url, periodic=noop):
    with tempfile.TemporaryDirectory(prefix='crutil') as tmp:
        gzpath = os.path.join(tmp, 'raiders.sqlite.gz')
        dbfile = 'raiders.sqlite'
        dbpath = os.path.join(tmp, dbfile)
        periodic(message='downloading %s' % (url,))
        with open(gzpath, 'wb') as gzfh:
            r = requests.get(url)
            for data in r.iter_content(chunk_size=16384):
                gzfh.write(data)
        gzip_from(gzpath, tmp, dbfile, periodic=periodic)
        cf.opendb(dbpath)
        os.chmod(dbpath, 0o644)
        mv_to(dbpath, cf.db_path)


def maybe_download_update(periodic=noop):
    current = {'snapshot-started': 0, 'snapshot-updated': 0}
    try:
        db = cf.opendb()
        cur = db.cursor()
        cur.execute('SELECT name, value FROM meta WHERE name = ? OR name = ?',
                    ('snapshot-started', 'snapshot-updated'))
        current.update(cur.fetchall())
    except sqlite3.OperationalError:
        pass
    r = requests.get(cf.crutil_api_url.rstrip('/') + '/latest')
    latest = r.json()

    if not latest:
        return
    elif latest['schema-version'] != schema_version:
        print(schema_version_advice(latest['schema-version']),
              file=sys.stderr)
        sys.exit(1)
    elif (latest['snapshot-started'] > current['snapshot-started'] or
          (latest['snapshot-started'] == current['snapshot-started'] and
           latest['snapshot-updated'] > current['snapshot-updated'])):
        periodic('Updating database', 'from %d/%d to %d/%d' % (
            current['snapshot-started'], current['snapshot-updated'],
            latest['snapshot-started'], latest['snapshot-updated']))
        download_and_install_snapshot(latest['url'], periodic=periodic)


def schema_version_advice(version):
    if version > schema_version:
        return 'Database is too new for this code, try git pull?'
    elif version < schema_version:
        return 'Database is too old for this code, are you on a branch?'


def friendly_dbopen():
    try:
        db = cf.opendb()
    except DBVersionError as exc:
        print('%s\n%s' % (exc.args[0], schema_version_advice(exc.version)),
              file=sys.stderr)
        sys.exit(1)
    setupdb(db)
    return db


def maybe_load_geardb(db, forcelocal=None):
    if not cf.can_update_remote or forcelocal:
        geardb.load_from_sql(db.cursor())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', dest='raider',
                        help='Update only a single raider')
    parser.add_argument('-U', dest='nodownload', action='store_true',
                        help='Do not download database updates')
    parser.add_argument('-L', dest='local',
                        default=None, action='store_true',
                        help='Perform database update locally')
    parser.add_argument('-G', dest='gear', default=True, action='store_false',
                        help='Skip import from CR guru')
    parser.add_argument('-R', dest='recruiting',
                        default=True, action='store_false',
                        help='Skip retrieving recruiting information')
    parser.add_argument('-Q', dest='questing',
                        default=True, action='store_false',
                        help='Skip retrieving questing information')
    args = parser.parse_args()
    if args.local and not args.nodownload:
        args.nodownload = True

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure', file=sys.stderr)
        sys.exit(1)
    if not cf.can_update_local and args.local:
        print('error: please run ./cr-conf.py to configure local updates',
              file=sys.stderr)
        sys.exit(1)

    cf.makedirs()
    if cf.can_update_remote and not args.nodownload:
        maybe_download_update(periodic=periodic_print)
    db = friendly_dbopen()

    raiders = None
    if args.raider is not None:
        raiders = ensure_raider_ids(db, args.raider, parser.print_usage),

    maybe_load_geardb(db, forcelocal=args.local)
    request_update(raiders, gear=args.gear, recruiting=args.recruiting,
                   questing=args.questing, periodic=periodic_print,
                   forcelocal=args.local)


if __name__ == '__main__':
    main()
