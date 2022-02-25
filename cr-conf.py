#!./venv/bin/python
import appdirs
import configparser
import io
import os

appname = 'crutil'

slots = ('main_hand', 'dress', 'knickknack', 'finger')


class CRConf():
    def __init__(self):
        self._confdir = appdirs.user_config_dir(appname)
        self._datadir = appdirs.user_data_dir(appname)
        self._conf_path = os.path.join(self._confdir, 'crutil.ini')
        self.db_path = os.path.join(self._datadir, 'raiders.sqlite')
        self.auth_token_path = os.path.join(self._confdir,
                                            'goog-auth-token.json')
        self.nft_contract = '0xfd12ec7ea4b381a79c78fe8b2248b4c559011ffb'
        self.alchemy_api_url = 'https://polygon-mainnet.g.alchemy.com/v2'
        self.crg_domain = 'europe-west3-cryptoraiders-guru.cloudfunctions.net'
        self.crg_url = 'https://www.cryptoraiders.guru'

        self._schema = {
            'polygon': {
                'alchemy_api_key': (
                    'Alchemy API Key', 'An API key for alchemy.com'),
                'nft_owner': (
                    'Wallet Address',
                    'The Polygon wallet address owning the raider NFTs'),
            },
            'google': {
                'goog_client_id': (
                    'Client ID', 'Client identifier for Google Sheets API'),
                'goog_client_secret': (
                    'Client secret', 'Client secret for Google Sheets API'),
                'goog_sheet_id': (
                    'Spreadsheet ID', 'Google Spreadsheet ID to import from'),
                'goog_gear_tab': (
                    'Gear sheet name', 'Name of gear tab in spreadsheet'),
                'goog_raider_tab': (
                    'Raider sheet name', 'Name of raiders tab in spreadsheet'),
            }
        }
        self._loaded = {i: {} for i in self._schema.keys()}

    def makedirs(self):
        if not os.path.exists(self._confdir):
            os.makedirs(self._confdir)
        if not os.path.exists(self._datadir):
            os.makedirs(self._datadir)

    def load_config(self):
        ret = True
        cp = configparser.ConfigParser()
        cp.read(self._conf_path)
        for sect in sorted(self._schema.keys()):
            for key in sorted(self._schema[sect].keys()):
                if sect in cp and key in cp[sect]:
                    self._loaded[sect][key] = cp[sect][key]
                    setattr(self, key, cp[sect][key])
                else:
                    ret = False
        return ret

    def update(self, sect, key, val):
        assert key in self._schema[sect]
        self._loaded[sect][key] = val
        setattr(self, key, val)

    def write_secrets(self, filename, data):
        mode = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        newfile = filename + '.new'
        fd = os.open(newfile, mode, 0o600)
        try:
            with os.fdopen(fd, 'w') as fh:
                fh.write(data)
        finally:
            try:
                os.close(fd)
            except Exception:
                pass
        os.rename(newfile, filename)

    def save_config(self):
        cp = configparser.ConfigParser()
        for sect in sorted(self._schema.keys()):
            cp[sect] = {}
            for key in sorted(self._loaded[sect].keys()):
                cp[sect][key] = self._loaded[sect][key]

        cfdata = io.StringIO()
        cp.write(cfdata)
        self.makedirs()
        self.write_secrets(self._conf_path, cfdata.getvalue())

    def config_metadata(self):
        md = {}
        for sect in sorted(self._schema.keys()):
            md[sect] = {}
            for key in sorted(self._schema[sect].keys()):
                md[sect][key] = {'name': self._schema[sect][key][0],
                                 'desc': self._schema[sect][key][1]}
                if key in self._loaded[sect]:
                    md[sect][key]['value'] = self._loaded[sect][key]
        return md


def main():
    cf = CRConf()
    print('loading config...')
    cf.load_config()
    md = cf.config_metadata()
    updated = False
    try:
        for sect in sorted(md.keys()):
            print('# %s configuration:' % (sect,))
            for key in sorted(md[sect].keys()):
                if 'value' in md[sect][key]:
                    print('%s: %s' % (
                        md[sect][key]['name'], md[sect][key]['value']))
                else:
                    val = input('%s: ' % (md[sect][key]['name'],))
                    if len(val.strip()):
                        updated = True
                        cf.update(sect, key, val.strip())
    except EOFError:
        pass
    if updated:
        print('saving updated config...')
        cf.save_config()


if __name__ == '__main__':
    main()