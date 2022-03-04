#!./venv/bin/python
import sqlite3
import argparse
import math
import sys

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


def show_all_raiders(db):
    cur = db.cursor()
    cur.execute('SELECT MAX(LENGTH(id)), MAX(LENGTH(name)) FROM raiders')
    idlen, namelen = cur.fetchone()

    cur.execute('''SELECT id, name, level, generation, race
        FROM raiders ORDER BY level DESC, id''')
    raiders = list(cur.fetchall())
    for id, name, lvl, gen, race in raiders:
        wearing = get_equipped(cur, id)
        print('%-*d  [%d] %-#*s - gen %d %s wearing %s' % (
            idlen, id, lvl, namelen, name, gen, race,
            ', '.join(wearing) if wearing else 'nothing'))


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
    parser.add_argument('-r', dest='raider',
                        help='Raider name or id')
    parser.add_argument('-u', dest='update',
                        default=False, action='store_true',
                        help='Update raider data first')
    parser.add_argument('-b', dest='best',
                        default=False, action='store_true',
                        help='Calculate best gear for raider')
    parser.add_argument('-c', dest='count', type=int, default=5,
                        help='Number of combos to display with -d')
    args = parser.parse_args()

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)
    db = sqlite3.connect(cf.db_path)

    raider = None
    if args.raider is not None:
        raider = findraider(db, args.raider)
        if raider is None:
            print('No raider named "%s" found' % (args.raider,))
            parser.print_usage()
            sys.exit(1)

    if args.update:
        __import__('cr-import').import_or_update(db, raider=raider, gear=True)

    if args.best:
        if raider is None:
            print('Raider is required, please specify with -r')
            parser.print_usage()
            sys.exit(1)
        calc_best_gear(db, raider, args.count)
    elif raider is not None:
        show_raider(db, raider)
    else:
        show_all_raiders(db)


if __name__ == '__main__':
    main()
