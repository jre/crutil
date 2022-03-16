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
    if last_raid < last_weekly.timestamp():
        raids_left = cf.cr_weekly_raids
    if not last_endless:
        return raids_left, -1
    endless_left = int(last_endless < last_daily.timestamp())
    return raids_left, endless_left


def get_raider_recruiting(cur, rid, now):
    cur.execute('SELECT next, cost FROM recruiting WHERE raider = ?', (rid,))
    rows = tuple(cur.fetchall())
    if len(rows) == 0:
        return -2, -1
    next, cost = rows[0]
    if next and next > now.timestamp():
        return next, cost
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
    parts = str(datetime.timedelta(seconds=int(delta))).split(',')
    return ', '.join(tuple(parts[:-1]) + ('%8s' % (parts[-1].strip(),),))


def fmt_positive_count(count):
    return '?' if count < 0 else str(count)


def fmt_positive_secs(secs):
    return 'now' if secs == 0 else str(datetime.datetime.fromtimestamp(secs))


class TabularReport():
    def __init__(self, colspec):
        self.columns = tuple(c[0] for c in colspec)
        self.labels = tuple(c[1] for c in colspec)
        self.coltypes = tuple(c[2] for c in colspec)
        self.right_align = tuple(c[3] for c in colspec)
        self.col_idx = {k: i for i, k in enumerate(self.columns)}

    @property
    def colcount(self):
        return len(self.columns)


class RaiderListReport(TabularReport):
    def __init__(self):
        super().__init__((
            ('id', 'ID', 'int', True),
            ('name', 'Name', 'str', False),
            ('gen', 'Gen', 'int', True),
            ('race', 'Race', 'str', False),
            ('raids', 'Raids', 'positive_count', True),
            ('endless', 'Endless', 'positive_count', True),
            ('recruit', 'Recruit', 'epoch_seconds', True),
            ('cost', 'Cost', 'int', True),
            ('quest', 'Questing', 'str', False),
            ('returns', 'Return', 'delta_seconds', True),
            ('wearing', 'Wearing', 'str', False)))

    def fetch(self, db):
        cur = db.cursor()
        cur.execute('SELECT id, name, level, generation, race FROM raiders')
        rows = tuple(cur.fetchall())
        now = datetime.datetime.utcnow()
        last_daily = last_daily_refresh(now)
        last_weekly = last_weekly_refresh(now)

        for id, name, lvl, gen, race in rows:
            lvl_name = '[%d] %s' % (lvl, name)
            raids, endless = get_raider_raids(cur, id, last_daily, last_weekly)
            recruit_time, recruit_cost = get_raider_recruiting(cur, id, now)
            quest_status, quest_back = get_raider_questing(cur, id, now)
            wear_seq = get_equipped(cur, id)
            wear_str = (', '.join(wear_seq) if wear_seq else 'nothing')
            yield (id, lvl_name, gen, race, raids, endless,
                   recruit_time, recruit_cost,
                   quest_status, quest_back, wear_str)

    def sort(self, rows, sorting=None):
        if not sorting:
            sorting = ('id',)
        sortkey = make_sort_keyfunc(sorting, self.columns)
        rows.sort(key=sortkey)


def show_all_raiders(db, sorting=()):
    report = RaiderListReport()
    raw_tbl = list(report.fetch(db))
    report.sort(raw_tbl, sorting)
    now = datetime.datetime.utcnow()

    fmt = {
        'str': str,
        'int': str,
        'positive_count': fmt_positive_count,
        'delta_seconds': fmt_raider_timedelta,
        'epoch_seconds': lambda v: fmt_raider_timedelta(v - now.timestamp()
                                                        if v > 0 else v),
    }

    str_tbl = [report.columns]
    str_tbl.extend(tuple(fmt[report.coltypes[i]](v)
                         for i, v in enumerate(r)) for r in raw_tbl)
    widths = [len(i) for i in report.columns]
    for row in str_tbl:
        for i in range(report.colcount):
            if len(row[i]) > widths[i]:
                widths[i] = len(row[i])

    for row in str_tbl:
        print(' '.join(('%*s' if report.right_align[i] else '%-*s') % (
            widths[i], row[i])
                       for i in range(report.colcount)))


def get_raider_slots(cur, rid):
    cur.execute('''SELECT level, name,
        strength, intelligence, agility, wisdom, charm, luck
        FROM raiders WHERE id = ?''', (rid,))
    rows = tuple(cur.fetchall())
    if len(rows) == 0:
        return
    r_level = rows[0][0]
    r_name = rows[0][1]
    r_stats = rows[0][2:]

    names = {None: (r_level, r_name)}
    stats = {None: r_stats}
    cur.execute('''SELECT l.slot, u.name,
        u.strength, u.intelligence, u.agility, u.wisdom, u.charm, u.luck
        FROM gear_localid l, gear_uniq u WHERE l.dedup_id = u.dedup_id
        AND l.raider_id = ? AND l.equipped''', (rid,))
    for row in cur.fetchall():
        g_slot = row[0]
        g_name = row[1]
        g_stats = row[2:]
        assert g_slot in cr_conf.slots
        names[g_slot] = g_name
        stats[g_slot] = g_stats

    return names, stats


def get_equipped(cur, rid):
    cur.execute('''SELECT l.slot, u.name FROM gear_localid l, gear_uniq u
       WHERE l.dedup_id = u.dedup_id AND l.raider_id = ? AND l.equipped''',
                (rid,))
    gear = dict(cur.fetchall())
    assert not set(gear).difference(cr_conf.slots), cr_conf.slots
    return tuple(gear[i] for i in cr_conf.slots if i in gear)


def skew_stats(stats):
    stats = tuple(map(float, stats))
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


class RaiderComboReport(TabularReport):
    def __init__(self):
        super().__init__((
            ('name', 'Name', 'str', False),
            ('str', 'Stren.', 'float_1', True),
            ('int', 'Intel.', 'float_1', True),
            ('dex', 'Agil.', 'float_1', True),
            ('wis', 'Wis.', 'float_1', True),
            ('chr', 'Charm', 'float_1', True),
            ('luck', 'Luck', 'float_1', True),
            ('hp', 'Health', 'float_1', True),
            ('mindamg', 'MinDam', 'float_1', True),
            ('maxdamg', 'MaxDam', 'float_1', True),
            ('accurac', 'Accur', 'float_1', True),
            ('hitfrst', 'HitFrst', 'float_1', True),
            ('critdmg', 'CrtDam', 'float_1', True),
            ('critrat', 'CrtRate', 'float_1', True),
            ('critrst', 'CrtResist', 'float_1', True),
            ('evade', 'Evade', 'float_1', True),
            ('damgrst', 'DamResist', 'float_1', True),
            ('total', 'Total', 'float_1', True)))

    def fetch(self, db, rid):
        _, combos = self.fetch_more(db, rid)
        return combos

    def fetch_more(self, db, rid):
        cur = db.cursor()
        slot_names, slot_stats = get_raider_slots(cur, rid)
        level = slot_names[None][0]
        cur_raw_stats = tuple(map(sum, zip(*slot_stats.values())))
        cur_skewed_stats = skew_stats(cur_raw_stats)
        cur_derived_stats = derive_stats(level, cur_skewed_stats)
        cur_stats_all = cur_skewed_stats + cur_derived_stats + \
            (sum(cur_derived_stats),)

        equipped = {}
        gear = {}
        for slot in ('dress', 'main_hand', 'finger'):
            if slot in slot_names:
                equipped[slot] = (slot_names[slot],) + slot_stats[slot]
            cur.execute('''SELECT u.name, u.strength,
                u.intelligence, u.agility, u.wisdom, u.charm, u.luck
                FROM gear_uniq u, gear_localid l
                WHERE u.dedup_id = l.dedup_id
                AND slot = ? AND raider_id = ?''', (slot, rid))
            gear[slot] = list(remove_dups(cur.fetchall()))
            if len(gear[slot]) == 0:
                gear[slot].append(('nothing (%s)' % (slot,), 0, 0, 0, 0, 0, 0))
        combos = []
        for weap_row in gear['main_hand']:
            weap_stats = weap_row[1:]
            for dress_row in gear['dress']:
                dress_stats = dress_row[1:]
                for ring_row in gear['finger']:
                    ring_stats = ring_row[1:]
                    new_raw_stats = map(sum, zip(slot_stats[None], weap_stats,
                                                 dress_stats, ring_stats))
                    new_raw_stats = tuple(new_raw_stats)
                    new_skewed_stats = skew_stats(new_raw_stats)
                    new_derived_stats = derive_stats(level, new_skewed_stats)
                    new_stats_all = new_skewed_stats + new_derived_stats + \
                        (sum(new_derived_stats),)
                    stats_diff = tuple(n - c for n, c in
                                       zip(new_stats_all, cur_stats_all))
                    combo_row = ('',) + new_stats_all
                    diff_row = ('',) + stats_diff
                    combos.append((combo_row, diff_row,
                                   weap_row, dress_row, ring_row))

        combos.sort(key=lambda i: i[0][-1], reverse=True)
        return set(equipped.values()), combos


def calc_best_gear(db, rid, count):
    report = RaiderComboReport()
    cur = db.cursor()
    slot_names, slot_stats = get_raider_slots(cur, rid)
    lvl = slot_names[None][0]
    id_lvl_name = '%d - [%d] %s' % ((rid,) + slot_names[None])

    cur.execute('SELECT MAX(LENGTH(name)) FROM gear_uniq')
    namelen = cur.fetchone()[0]
    if namelen is None:
        print('Error: no gear found for %s' % (id_lvl_name,))
        return

    raw_stats = tuple(map(sum, zip(*slot_stats.values())))
    cur_eff_stats = skew_stats(raw_stats)
    cur_der_stats = derive_stats(lvl, cur_eff_stats)

    combos = list(report.fetch(db, rid))

    def fmtstats(s):
        return ' '.join('%7d' % i for i in s)

    print('%-*s  %s' % (namelen, report.columns[0],
                        fmt_hdr(report.columns[1:], 7)))
    cur_stats_line = cur_eff_stats + cur_der_stats + (sum(cur_der_stats),)
    print(fmt_base('%-*s  %s\n' * 5) % (
        namelen, id_lvl_name, fmtstats(slot_stats[None]),
        namelen, slot_names['main_hand'], fmtstats(slot_stats['main_hand']),
        namelen, slot_names['dress'], fmtstats(slot_stats['dress']),
        namelen, slot_names['finger'], fmtstats(slot_stats['finger']),
        namelen, '', fmtstats(cur_stats_line)))
    for combo_row, diff_row, weap_row, dress_row, ring_row in combos[:count]:
        cur_equipment = True
        for slot, row in (('main_hand', weap_row),
                          ('dress', dress_row),
                          ('finger', ring_row)):
            stats = '%-*s  %s' % (
                namelen, row[0], fmtstats(row[1:]))
            if slot_names[slot] == row[0] and \
               slot_stats[slot] == row[1:]:
                print(fmt_base(stats))
            else:
                cur_equipment = False
                print(stats)
        if cur_equipment:
            print('%-*s  %s\n' % (namelen, '',
                                  fmt_base(fmtstats(combo_row[1:]))))
        else:
            print('%-*s  %s\n%-*s  %s\n' % (
                namelen, '', fmtstats(combo_row[1:]),
                namelen, '', ' '.join(fmt_stat_diff(i, 7)
                                      for i in diff_row[1:])))


class RaiderGearReport(TabularReport):
    def __init__(self):
        super().__init__((
            ('name', 'Name', 'str', False),
            ('str', 'Stren.', 'float_1', True),
            ('int', 'Intel.', 'float_1', True),
            ('dex', 'Agil.', 'float_1', True),
            ('wis', 'Wis.', 'float_1', True),
            ('chr', 'Charm', 'float_1', True),
            ('luck', 'Luck', 'float_1', True),
            ('hp', 'Health', 'float_1', True),
            ('mindamg', 'MinDam', 'float_1', True),
            ('maxdamg', 'MaxDam', 'float_1', True),
            ('accurac', 'Accur', 'float_1', True),
            ('hitfrst', 'HitFrst', 'float_1', True),
            ('critdmg', 'CrtDam', 'float_1', True),
            ('critrat', 'CrtRate', 'float_1', True),
            ('critrst', 'CrtResist', 'float_1', True),
            ('evade', 'Evade', 'float_1', True),
            ('damgrst', 'DamResist', 'float_1', True),
            ('total', 'Total', 'float_1', True)))

    def fetch(self, db, rid):
        cur = db.cursor()

        slot_names, slot_stats = get_raider_slots(cur, rid)
        level = slot_names[None][0]
        raw_stats = tuple(map(sum, zip(*slot_stats.values())))

        combined_stats = skew_stats(raw_stats)
        full_stats = derive_stats(level, combined_stats)
        total = sum(full_stats)

        cur.execute('''SELECT u.name, l.slot,
            u.strength, u.intelligence, u.agility, u.wisdom, u.charm, u.luck
            FROM gear_uniq u, gear_localid l
            WHERE u.dedup_id = l.dedup_id AND raider_id = ?''', (rid,))
        for row in cur.fetchall():
            name = row[0]
            slot = row[1]
            item_stats = row[2:]
            old_slot_stats = slot_stats.get(slot, (0, 0, 0, 0, 0, 0))
            new_raw_stats = (i - j + k for i, j, k in
                             zip(raw_stats, old_slot_stats, item_stats))
            new_comb_stats = skew_stats(new_raw_stats)
            new_full_stats = derive_stats(level, new_comb_stats)
            new_total = sum(new_full_stats)
            diff = tuple(j - i for i, j in zip(
                combined_stats + full_stats + (total,),
                new_comb_stats + new_full_stats + (new_total,)))
            yield (name,) + diff


def show_raider(db, rid):
    report = RaiderGearReport()
    cur = db.cursor()
    cur.execute('''SELECT name, level, generation, race
        FROM raiders WHERE id = ?''', (rid,))
    row = cur.fetchone()
    name, lvl, gen, race = row
    wearing = get_equipped(cur, rid)
    print('%d  [%d] %s  - gen %d %s wearing %s' % (
        rid, lvl, name, gen, race,
        ', '.join(wearing) if wearing else 'nothing'))

    cur.execute('SELECT MAX(LENGTH(name)) FROM gear_uniq')
    namelen = cur.fetchone()[0]
    if namelen is None:
        print('Error: no gear found for [%d] %s' % (lvl, name))
        return

    gear = list(report.fetch(db, rid))
    gear.sort(key=lambda v: v[0])
    print('%-*s  %s' % (
        namelen, report.columns[0],
        ' '.join('%7s' % i for i in report.columns[1:])))
    for row in gear:
        print('%-*s  %s' % (
            namelen, row[0], ' '.join(fmt_stat_diff(i, 7) for i in row[1:])))


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
