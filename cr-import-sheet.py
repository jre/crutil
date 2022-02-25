#!./venv/bin/python
import argparse
import os
import sys
import sqlite3
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

cr_conf = __import__('cr-conf')
cf = cr_conf.CRConf()


def get_credentials(auth_scopes):
    secrets = {
        'installed': {
            'project_id': 'crutil',
            'client_id': cf.goog_client_id,
            'client_secret': cf.goog_client_secret,
            'auth_uri': 'https://accounts.google.com/o/oauth2/auth',
            'token_uri': 'https://oauth2.googleapis.com/token',
            'auth_provider_x509_cert_url':
                'https://www.googleapis.com/oauth2/v1/certs',
            'redirect_uris': ('urn:ietf:wg:oauth:2.0:oob', 'http://localhost')
        }
    }
    creds = None
    if os.path.exists(cf.auth_token_path):
        creds = Credentials.from_authorized_user_file(
            cf.auth_token_path, auth_scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_config(secrets, auth_scopes)
            creds = flow.run_local_server(port=0)
        cf.write_secrets(cf.auth_token_path, creds.to_json())
    return creds


def import_sheet(db):
    scope = ('https://www.googleapis.com/auth/spreadsheets.readonly',)

    print('importing from goog spreadsheet %s' % (cf.goog_sheet_id,))
    creds = get_credentials(scope)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    gear_range = '%s!A2:I' % (cf.goog_gear_tab,)
    raiders_range = '%s!A2:H' % (cf.goog_raider_tab,)

    cur = db.cursor()

    raiders = sheet.values().get(spreadsheetId=cf.goog_sheet_id,
                                 range=raiders_range).execute()
    print('  found %d rows of raider info in %s' % (
        len(raiders.get('values', ())), raiders_range))
    cur.execute('BEGIN TRANSACTION')
    for row in raiders['values']:
        try:
            rid, level = map(int, row[:2])
            stats = tuple(int(i) if len(i) else 0 for i in row[2:])
            stats = (stats + (0, 0, 0, 0, 0, 0))[:6]
        except ValueError:
            print('  skipping bad row in %s tab: %s' % (
                cf.goog_raider_tab, ','.join(row)))
            continue
        cur.execute('''SELECT name, level,
            strength, intelligence, agility, wisdom, charm, luck
            FROM raiders WHERE id = ?''', (rid,))
        old = cur.fetchall()
        if len(old) == 0:
            print('  skipping unknown raider id %d' % (rid,))
            continue
        old_name, old_level = old[0][:2]
        old_stats = old[0][2:]
        if old_level > level or any(o > n for o, n in zip(old_stats, stats)):
            print(('  skipping update for raider [%d] %s - ' +
                   'values on blockchain are higher than spreadsheet') % (
                       old_level, old_name))
            continue
        cur.execute('''UPDATE raiders SET strength = ?, intelligence = ?,
            agility = ?, wisdom = ?, charm = ?, luck = ?, level = ?
            WHERE id = ?''', (stats + (level, rid)))
    db.commit()

    gear = sheet.values().get(spreadsheetId=cf.goog_sheet_id,
                              range=gear_range).execute()
    print('  found %d rows of gear in %s' % (
        len(gear.get('values', ())), gear_range))
    source = 'googsheet'
    cur.execute('BEGIN TRANSACTION')
    cur.execute('DELETE FROM gear WHERE source = ?', (source,))
    for row in gear['values']:
        try:
            rid = int(row[0])
            name = row[1]
            slot = row[2].lower()
            assert len(name)
            assert slot in cr_conf.slots
            stats = tuple(int(i) if len(i) else 0 for i in row[3:])
            stats = (stats + (0, 0, 0, 0, 0, 0))[:6]
        except (ValueError, IndexError, AssertionError):
            print('skipping bad row in %s tab: %s' % (
                cf.goog_gear_tab, ','.join(row)))
            continue
        cur.execute('''INSERT INTO gear (owner_id, name, slot, source,
            strength, intelligence, agility, wisdom, charm, luck) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (rid, name, slot, source) + stats)
    db.commit()


def main():
    parser = argparse.ArgumentParser()
    parser.parse_args()

    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)
    cf.makedirs()
    db = sqlite3.connect(cf.db_path)
    import_sheet(db)


if __name__ == '__main__':
    main()
