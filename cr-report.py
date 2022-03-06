#!./venv/bin/python
import sqlite3
import argparse
import math
import sys
import datetime
import operator
import functools

cr_conf = __import__('cr-conf')
cf = cr_conf.conf


def derive_stats(level, base):
    str, int, dex, wis, chr, luck = base
    level_3_sqrt = math.sqrt(level * 3)
    maxhp = (str * 2.9) + (int * 1.5) + (dex * 2.1) + (chr * 3.5)
    mindam = (str * 0.35) + (int * 0.45) + (dex * 0.4) + (wis * 0.45)
    maxdam = (str * 0.45) + (int * 0.65) + (dex * 0.55) + (wis * 0.55)
    # hit chance
    hitc = math.pow(math.tanh(((int * 2) + (wis * 2)) / (level + 100)), 2) * \
        (level_3_sqrt + 35)
    # hit first
    hitf = math.pow(math.tanh((dex * 4) / (level + 100)), 2) * \
        (level_3_sqrt + 35)
    # crit damage multiplier
    cdm = (math.pow(math.tanh(((int * 2) + (luck * 2.5)) / (level + 100)), 2) *
           ((level_3_sqrt / 100) + 0.4)) + \
        (math.tanh(((int * 0.5) + (luck * 4)) / (level + 100)) *
         ((level_3_sqrt / 100) + 0.1))
    # melee crit
    mc = (math.pow(math.tanh((int + (dex * 2) + (luck * 4)) /
                             (level + 100)), 2) *
          (level_3_sqrt + 25)) + \
        (math.tanh((luck * 4) / (level + 100)) * (level_3_sqrt + 10))
    # crit resist
    cr = (math.pow(math.tanh(((str * 2) + chr) / (level + 100)), 2) *
          (level_3_sqrt + 40)) + \
        (math.tanh(((str * 0.5) + (chr * 6)) / (level + 100)) *
         (level_3_sqrt + 10))
    # evade chance
    ec = (math.pow(math.tanh(((dex * 3) + (luck * 2)) /
                             (level + 100)), 2) * 40) + \
        (math.tanh(((dex * 0.5) + (luck * 6)) / (level + 100)) * 10) + \
        level_3_sqrt
    # melee resist
    mr = math.pow(math.tanh(((str * 2) + (chr * 3)) / (level + 100)), 2) * \
        (level_3_sqrt + 33)
    return (maxhp, mindam, maxdam, hitc, hitf, cdm, mc, cr, ec, mr)


def multisub(val, indexes):
    for i in indexes:
        val = val[i]
    return val


def make_sort_keyfunc(spec, columns):
    col_idx = {}
    for idx, col in enumerate(columns):
        if isinstance(col, tuple):
            col_idx.update({k: (idx, i) for i, k in enumerate(col)})
        else:
            col_idx[col] = (idx,)
    try:
        query = tuple((col_idx[i.lstrip('-')],
                       (operator.lt, operator.gt)[int(i[0] == '-')])
                      for i in (j.lower().strip() for j in spec)
                      if len(i.lstrip('-')))
    except KeyError as err:
        raise ValueError('unknown column %r, valid columns: %s' % (
            err.args[0], ' '.join(sorted(col_idx.keys()))))

    def compare(a, b):
        for idx, ltfn in query:
            aa = multisub(a, idx)
            bb = multisub(b, idx)
            if aa != bb:
                return (1, -1)[int(ltfn(aa, bb))]
        return 0

    return functools.cmp_to_key(compare)


def last_daily_refresh(now):
    day_delta = datetime.timedelta(days=1)
    target = datetime.datetime(now.year, now.month, now.day,
                               *cf.cr_newraid_time)
    target += day_delta
    while target > now:
        target -= day_delta
    return target


def last_weekly_refresh(now):
    today = datetime.date.fromtimestamp(now.timestamp())
    if today.weekday() > cf.cr_newraid_weekday:
        future_wed_delta = 7 - (today.weekday() - cf.cr_newraid_weekday)
    else:
        future_wed_delta = 7 + (cf.cr_newraid_weekday - today.weekday())
    wed = today + datetime.timedelta(days=future_wed_delta)
    target = datetime.datetime(wed.year, wed.month, wed.day,
                               *cf.cr_newraid_time)
    week_delta = datetime.timedelta(days=7)
    while target > now:
        target -= week_delta
    return target


def get_raider_raids(cur, rid, last_daily, last_weekly):
    cur.execute('''SELECT remaining, last_raid, last_endless
        FROM raids WHERE raider = ?''', (rid,))
    rows = tuple(cur.fetchall())
    if len(rows) == 0:
        return -1, -1
    raids_left, last_raid, last_endless = rows[0]
    if last_raid < last_daily.timestamp():
        raids_left = cf.cr_weekly_raids
    endless_left = int(last_endless < last_daily.timestamp())
    return raids_left, endless_left


def get_raider_recruiting(cur, rid, now):
    cur.execute('SELECT next, cost FROM recruiting WHERE raider = ?', (rid,))
    rows = tuple(cur.fetchall())
    if len(rows) == 0:
        return -1, -1
    next, cost = rows[0]
    if next and next > now.timestamp():
        return int(next - now.timestamp()), cost
    else:
        return 0, cost


def get_raider_questing(cur, rid, now):
    cur.execute('''SELECT status, started_on, return_divisor, returns_on
        FROM quests WHERE raider = ?''', (rid,))
    rows = tuple(cur.fetchall())
    if len(rows) == 0:
        return '?', -1
    status, started_on, return_div, returns_on = rows[0]
    is_returning = cf.quest_returning[status]
    now_secs = now.timestamp()

    if is_returning is None:
        status_str = 'no'
        back_secs = 0
    elif not is_returning:
        status_str = 'yes'
        back_secs = int((now_secs - started_on) / return_div)
    elif is_returning:
        if returns_on and returns_on > now_secs:
            status_str = 'returning'
            back_secs = int(returns_on - now_secs)
        else:
            status_str = 'back'
            back_secs = 0
    return status_str, back_secs


def fmt_raider_timedelta(delta):
    if delta < 0:
        return '?'
    elif delta == 0:
        return 'now'
    parts = str(datetime.timedelta(seconds=delta)).split(',')
    return ', '.join(tuple(parts[:-1]) + ('%8s' % (parts[-1].strip(),),))


def fmt_positive_count(count):
    return '?' if count < 0 else str(count)


def fmt_positive_secs(secs):
    return 'now' if secs == 0 else str(datetime.datetime.fromtimestamp(secs))


def show_all_raiders(db, sorting=()):
    cur = db.cursor()
    cur.execute('SELECT MAX(LENGTH(id)), MAX(LENGTH(name)) FROM raiders')
    idlen, namelen = cur.fetchone()

    cur.execute('SELECT id, name, level, generation, race FROM raiders')
    rows = tuple(cur.fetchall())

    if not sorting:
        sorting = ('-level', 'id')

    now = datetime.datetime.utcnow()
    last_daily = last_daily_refresh(now)
    last_weekly = last_weekly_refresh(now)
    cols = (('id', str, '%*s'),
            (('level', 'name'), lambda v: '[%d] %s' % v, '%-*s'),
            ('gen', str, '%*s'),
            ('race', str, '%-*s'),
            ('raids', fmt_positive_count, '%*s'),
            ('endless', fmt_positive_count, '%*s'),
            ('recruit', fmt_raider_timedelta, '%*s'),
            ('cost', str, '%*s'),
            ('quest', str, '%-*s'),
            ('returns', fmt_raider_timedelta, '%*s'),
            ('wearing', str, '%-*s'))
    widths = [0 for _ in cols]
    col_labels = {cols[1][0]: lambda c: c[1]}
    print_hdr = tuple(col_labels.get(c[0], lambda i: i)(c[0]) for c in cols)
    sortkey = make_sort_keyfunc(sorting, (i[0] for i in cols))
    raw_tbl = []
    for id, name, lvl, gen, race in rows:
        raids, endless = get_raider_raids(cur, id, last_daily, last_weekly)
        recruit_time, recruit_cost = get_raider_recruiting(cur, id, now)
        quest_status, quest_back = get_raider_questing(cur, id, now)
        wear_seq = get_equipped(cur, id)
        wear_str = (', '.join(wear_seq) if wear_seq else 'nothing')
        raw_tbl.append((id, (lvl, name), gen, race, raids, endless,
                        recruit_time, recruit_cost,
                        quest_status, quest_back, wear_str))
    raw_tbl.sort(key=sortkey)

    widths = [len(i) for i in print_hdr]
    final_tbl = [print_hdr]
    for row in raw_tbl:
        vals = tuple(cols[i][1](row[i]) for i in range(len(cols)))
        for i in range(len(cols)):
            if len(vals[i]) > widths[i]:
                widths[i] = len(vals[i])
        final_tbl.append(vals)

    for row in final_tbl:
        print(' '.join(cols[i][2] % (widths[i], row[i])
                       for i in range(len(cols))))


def get_equipped(cur, rid):
    cur.execute('SELECT slot, name FROM gear WHERE owner_id = ? AND equipped',
                (rid,))
    gear = dict(cur.fetchall())
    assert not set(gear).difference(cr_conf.slots), cr_conf.slots
    return tuple(gear[i] for i in cr_conf.slots if i in gear)


def skew_stats(stats):
    stats = tuple(stats)
    s = sorted(enumerate(stats[:3]), key=lambda i: i[1])
    s = ((i[0], i[1] * j) for i, j in zip(s, (0.05, 0.7, 1.0)))
    s = (i[1] for i in sorted(s, key=lambda i: i[0]))
    return tuple(s) + stats[3:]


def remove_dups(items):
    dups = {}
    for i in items:
        if i not in dups:
            dups[i] = True
            yield i


def calc_best_gear(db, rid, count):
    cur = db.cursor()
    cur.execute('''SELECT name, level,
        strength, intelligence, agility, wisdom, charm, luck
        FROM raiders WHERE id = ?''', (rid,))
    row = cur.fetchone()
    name, lvl = row[:2]
    id_lvl_name = '%d - [%d] %s' % (rid, lvl, name)
    base_stats = row[2:]

    cur.execute('SELECT MAX(LENGTH(name)) FROM gear WHERE owner_id = ?',
                (rid,))
    namelen = cur.fetchone()[0]
    if namelen is None:
        print('Error: no gear found for [%d] %s' % (lvl, name))
        return

    cur.execute('''SELECT slot, name,
        strength, intelligence, agility, wisdom, charm, luck
        FROM gear WHERE owner_id = ? AND equipped''', (rid,))
    raw_stats = base_stats
    equipped = {i: ('nothing (%s)' % i, 0, 0, 0, 0, 0, 0)
                for i in cr_conf.slots}
    for row in cur.fetchall():
        slot = row[0]
        name_stats = row[1:]
        stats = row[2:]
        equipped[slot] = name_stats
        raw_stats = tuple(i + j for i, j in zip(raw_stats, stats))
    cur_eff_stats = skew_stats(raw_stats)
    cur_der_stats = derive_stats(lvl, cur_eff_stats)

    gear = {}
    for slot in ('dress', 'main_hand', 'finger'):
        cur.execute('''SELECT name,
            strength, intelligence, agility, wisdom, charm, luck
            FROM gear WHERE slot = ? AND owner_id = ?''', (slot, rid))
        gear[slot] = list(remove_dups(cur.fetchall()))
        if len(gear[slot]) == 0:
            gear[slot].append(('nothing (%s)' % (slot,), 0, 0, 0, 0, 0, 0))

    combos = []
    new = {'': ('',) + base_stats}
    for weap_row in gear['main_hand']:
        new['main_hand'] = weap_row
        for dress_row in gear['dress']:
            new['dress'] = dress_row
            for ring_row in gear['finger']:
                new['finger'] = ring_row
                no_more_magic = tuple(zip(*new.values()))
                raw_stats = tuple(sum(i) for i in no_more_magic[1:])
                new_eff_stats = skew_stats(raw_stats)
                new_der_stats = derive_stats(lvl, new_eff_stats)
                combos.append((sum(new_der_stats), new.copy(),
                               new_eff_stats, new_der_stats))
    combos.sort(key=lambda i: i[0], reverse=True)

    def fmtstats(s):
        return ' '.join('%7d' % i for i in s)

    print('%-*s  %s' % (namelen, '', fmt_hdr((
        'str', 'int', 'dex', 'wis', 'chr', 'luck',
        'hp', 'mindamg', 'maxdamg', 'accurac', 'hitfrst', 'critdmg', 'critrat',
        'critrst', 'evade', 'damgrst', 'total'), 7)))
    cur_stats_line = cur_eff_stats + cur_der_stats + (sum(cur_der_stats),)
    print(fmt_base('%-*s  %s\n' * 5) % (
        namelen, id_lvl_name, fmtstats(base_stats),
        namelen, equipped['main_hand'][0], fmtstats(equipped['main_hand'][1:]),
        namelen, equipped['dress'][0], fmtstats(equipped['dress'][1:]),
        namelen, equipped['finger'][0], fmtstats(equipped['finger'][1:]),
        namelen, '', fmtstats(cur_stats_line)))
    for (total, new, efstats, derstats) in combos[:count]:
        statsline = efstats + derstats + (total,)
        stats_diff = tuple(j - i for i, j in zip(cur_stats_line, statsline))
        cur_equipment = True
        for slot in ('main_hand', 'dress', 'finger'):
            stats = '%-*s  %s' % (
                namelen, new[slot][0], fmtstats(new[slot][1:]))
            if equipped[slot] == new[slot]:
                print(fmt_base(stats))
            else:
                cur_equipment = False
                print(stats)
        if cur_equipment:
            print('%-*s  %s\n' % (namelen, '', fmt_base(fmtstats(statsline))))
        else:
            print('%-*s  %s\n%-*s  %s\n' % (
                namelen, '', fmtstats(statsline),
                namelen, '', ' '.join(fmt_stat_diff(i, 7)
                                      for i in stats_diff)))


def show_raider(db, rid):
    cur = db.cursor()
    cur.execute('''SELECT name, level, generation, race,
        strength, intelligence, agility, wisdom, charm, luck
        FROM raiders WHERE id = ?''', (rid,))
    row = cur.fetchone()
    name, lvl, gen, race = row[:4]
    base_stats = row[4:]
    wearing = get_equipped(cur, rid)
    print('%d  [%d] %s  - gen %d %s wearing %s' % (
        rid, lvl, name, gen, race,
        ', '.join(wearing) if wearing else 'nothing'))

    cur.execute('SELECT MAX(LENGTH(name)) FROM gear WHERE owner_id = ?',
                (rid,))
    namelen = cur.fetchone()[0]
    if namelen is None:
        print('Error: no gear found for [%d] %s' % (lvl, name))
        return
    cur.execute('''SELECT slot,
        strength, intelligence, agility, wisdom, charm, luck
        FROM gear WHERE owner_id = ? AND equipped''', (rid,))
    gear = {}

    raw_stats = base_stats
    for row in cur.fetchall():
        gear[row[0]] = row[1:]
        raw_stats = tuple(i + j for i, j in zip(raw_stats, row[1:]))
    eff_stats = skew_stats(raw_stats)
    der_stats = derive_stats(lvl, eff_stats)
    total = sum(der_stats)

    hdr = ' '.join('%7s' % i for i in (
        'str', 'int', 'dex', 'wis', 'chr', 'luck',
        'hp', 'mindamg', 'maxdamg', 'accurac', 'hitfrst', 'critdmg', 'critrat',
        'critrst', 'evade', 'damgrst', 'total'))
    print('%-*s  %s' % (namelen, '', hdr))
    cur.execute('''SELECT name, slot,
        strength, intelligence, agility, wisdom, charm, luck
        FROM gear WHERE owner_id = ? ORDER BY name''', (rid,))
    for row in cur.fetchall():
        name = row[0]
        slot = row[1]
        item_stats = row[2:]
        slot_stats = gear.get(slot, (0, 0, 0, 0, 0, 0))
        new_raw_stats = (i - j + k for i, j, k in
                         zip(raw_stats, slot_stats, item_stats))
        new_eff_stats = skew_stats(new_raw_stats)
        new_der_stats = derive_stats(lvl, new_eff_stats)
        new_total = sum(new_der_stats)
        stats_diff = tuple(j - i for i, j in zip(
            eff_stats + der_stats + (total,),
            new_eff_stats + new_der_stats + (new_total,)))
        print('%-*s  %s' % (
            namelen, name, ' '.join(fmt_stat_diff(i, 7) for i in stats_diff)))


def fmt_stat_diff(stat, width):
    if stat > 0.005:
        color = 32
    elif stat < -0.005:
        color = 31
    else:
        return '%*.1f' % (width, stat)
    return '\033[0;%dm%s\033[0m' % (color, '%+*.2f' % (width, stat))


def fmt_hdr(names, width):
    return '\033[0;1m%s\033[0m' % ' '.join('%*s' % (width, i) for i in names)


def fmt_base(text):
    return '\033[0;1m%s\033[0m' % (text,)


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
    parser.set_defaults(cmd='list', sort='')
    subparsers = parser.add_subparsers(dest='cmd')

    p_best = subparsers.add_parser('best',
                                   help='Calculate best gear for raider')
    p_best.add_argument('raider', help='Raider name or id')
    p_best.add_argument('-u', dest='update',
                        default=False, action='store_true',
                        help='Update raider data first')
    p_best.add_argument('-c', dest='count', type=int, default=5,
                        help='Number of combinations to display')
    # XXX add -s option for best

    p_gear = subparsers.add_parser('gear',
                                   help="Show a raider's gear")
    p_gear.add_argument('raider', help='Raider name or id')
    p_gear.add_argument('-u', dest='update',
                        default=False, action='store_true',
                        help='Update raider data first')
    # XXX add -s option for gear

    p_list = subparsers.add_parser('list',
                                   help='List all raiders')
    p_list.add_argument('-s', dest='sort', default='',
                        help='Sort order')

    args = parser.parse_args()

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)
    db = sqlite3.connect(cf.db_path)

    sorting = tuple(i.strip().lower() for i in args.sort.split(',') if i)

    if args.cmd is None or args.cmd == 'list':
        show_all_raiders(db, sorting=sorting)
        return

    raider = findraider(db, args.raider)
    if raider is None:
        print('No raider named "%s" found' % (args.raider,))
        parser.print_usage()
        sys.exit(1)

    if args.update:
        __import__('cr-update').import_or_update(db, raider=raider)

    if args.cmd == 'gear':
        show_raider(db, raider)
    elif args.cmd == 'best':
        calc_best_gear(db, raider, args.count)


if __name__ == '__main__':
    main()
