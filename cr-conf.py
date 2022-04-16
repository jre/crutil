#!./venv/bin/python
import appdirs
import configparser
import io
import os
import requests

appname = 'crutil'

slots = ('main_hand', 'dress', 'knickknack', 'finger', 'neck', 'background')


class CRConf():
    def __init__(self):
        self._confdir = appdirs.user_config_dir(appname)
        self._datadir = appdirs.user_data_dir(appname)
        self._conf_path = os.path.join(self._confdir, 'crutil.ini')
        self._abidir = os.path.join(self._datadir, 'abi')
        self.db_path = os.path.join(self._datadir, 'raiders.sqlite')
        # note: this is returned by recruiting contract method raidersAddress
        self.nft_contract = '0xfd12ec7ea4b381a79c78fe8b2248b4c559011ffb'
        # note: mounts nft is 0x7f2e8b6c55fcc5c52df495065d2147b9eab2cc54
        self.quest_returning = (None, False, True)
        self.cr_api_url = 'https://api.cryptoraiders.xyz'
        self.alchemy_api_url = 'https://polygon-mainnet.g.alchemy.com/v2'
        self.polygonscan_api_url = 'https://api.polygonscan.com/api'
        self.crg_domain = 'europe-west3-cryptoraiders-guru.cloudfunctions.net'
        self.crg_url = 'https://www.cryptoraiders.guru'
        # raids refresh on wednesday, 6am UTC
        self.cr_newraid_weekday = 2
        self.cr_newraid_time = (6, 0, 0)
        self.cr_weekly_raids = 5

        # known verified contracts, which we can retrieve the ABI for
        self._contract_names = {
            '0x06F34105B7DfedC95125348A8349BdA209928730': 'grimweed',
            '0x98a195e3eC940f590D726557c95786C8EBb0A2D2': 'newt-quest',
            '0x5A4fCdD54D483808080e0588c1E7d73e2a8AfdA8': 'questing-raiders',
            '0x5b0e5ae346a919c39fc8553b94d67599fd5e591d': 'raider-info',
            '0x32ADBBA23B00AA40701c5423466E4E57fDb4fe32': 'recruiting',
            '0xc81f43Eb261c1708bFfA84D75DDd945341723f1F': 'sporebark-quest',
            '0xe193364370F0E2923b41a8d1850F442B45E5ccA7': 'grimweed-quest',
            '0xF001508171344A4bc90fdA37890e343749d5D216': 'recruiting-history',
        }
        self._contracts = {v: k for k, v in self._contract_names.items()}
        self._quest_names = {
            'sporebark-quest': ('Sporebark', 'Fungal Infestation'),
            'grimweed-quest': ('Grimweed', 'The Hunt for Grimweed'),
            'newt-quest': ('Newt', 'Newt Slayer'),
        }
        self._quests = {self._contracts[k]: v
                        for k, v in self._quest_names.items()}

        self._polygon_web3 = None
        self._remote_schema = {
            'crutil': {
                'crutil_api_url': (
                    'crutil update API URL',
                    'The URL for a crutil update API server'),
                'crutil_api_key': (
                    'crutil update API key',
                    'Authorization key for crutil update API server')}}
        self._local_schema = {
            'crypto-raiders': {
                'cr_api_key': (
                    'Crypto Raiders API Key',
                    'An API key for cryptoraiders.xyz'),
            },
            'polygon': {
                'alchemy_api_key': (
                    'Alchemy API Key', 'An API key for alchemy.com'),
                'polygonscan_api_key': (
                    'PolygonScan API Key', 'An API key for polygonscan.com'),
                'nft_owner': (
                    'Wallet Address',
                    'The Polygon wallet address owning the raider NFTs'),
            }
        }
        self._schema = self._remote_schema.copy()
        self._schema.update(self._local_schema)
        self._loaded = {i: {} for i in self._schema.keys()}
        self._can_update_remote = None
        self._can_update_local = None

    def nft_owners(self):
        return ' '.join(self.nft_owner.split(',')).split()

    def makedirs(self):
        if not os.path.exists(self._confdir):
            os.makedirs(self._confdir)
        if not os.path.exists(self._abidir):
            os.makedirs(self._abidir)
        if not os.path.exists(self._datadir):
            os.makedirs(self._datadir)

    def _schema_loaded(self, schema):
        for sect in schema.keys():
            if sect not in self._loaded:
                return False
            for key in schema[sect].keys():
                if key not in self._loaded[sect]:
                    return False
        return True

    @property
    def can_update_remote(self):
        if self._can_update_remote is None:
            self._can_update_remote = self._schema_loaded(self._remote_schema)
        return self._can_update_remote

    @property
    def can_update_local(self):
        if self._can_update_local is None:
            self._can_update_local = self._schema_loaded(self._local_schema)
        return self._can_update_local

    def load_config(self):
        cp = configparser.ConfigParser()
        cp.read(self._conf_path)
        for sect in sorted(self._schema.keys()):
            for key in sorted(self._schema[sect].keys()):
                if sect in cp and key in cp[sect]:
                    self._loaded[sect][key] = cp[sect][key]
                    setattr(self, key, cp[sect][key])
        self._can_update_local = None
        self._can_update_remote = None
        return self.can_update_local or self.can_update_remote

    def update(self, sect, key, val):
        assert key in self._schema[sect]
        self._loaded[sect][key] = val
        setattr(self, key, val)
        if sect in self._local_schema and key in self._local_schema[sect]:
            self._can_update_local = None
        if sect in self._remote_schema and key in self._remote_schema[sect]:
            self._can_update_remote = None

    def write_secrets(self, filename, data):
        mode = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        newfile = filename + '.new'
        # XXX not safe for multiple instances
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

    def config_metadata(self, group):
        if group is None:
            schema = self._schema
        else:
            schema = getattr(self, '_%s_schema' % group)
        md = {}
        for sect in sorted(schema.keys()):
            md[sect] = {}
            for key in sorted(schema[sect].keys()):
                md[sect][key] = {'name': schema[sect][key][0],
                                 'desc': schema[sect][key][1]}
                if key in self._loaded[sect]:
                    md[sect][key]['value'] = self._loaded[sect][key]
        return md

    def _get_eth_abi(self, name):
        addr = self._contracts[name]
        filename = os.path.join(self._abidir, addr + '.json')
        if os.path.exists(filename):
            with open(filename) as fh:
                return fh.read()
        params = {
            'module': 'contract',
            'action': 'getabi',
            'address': addr,
            'apikey': self.polygonscan_api_key,
        }
        resp = requests.get(self.polygonscan_api_url, params=params)
        data = resp.json()
        if data['message'] == 'OK':
            # XXX not safe for multiple instances
            with open(filename, 'w') as fh:
                fh.write(data['result'])
            return data['result']
        raise ValueError(data['result'])

    def get_polygon_web3(self):
        if self._polygon_web3 is None:
            from web3 import Web3
            rkw = {'timeout': 60}
            self._polygon_web3 = Web3(Web3.HTTPProvider('%s/%s' % (
                self.alchemy_api_url, self.alchemy_api_key),
                                                        request_kwargs=rkw))
        return self._polygon_web3

    def get_eth_contract(self, name=None, address=None):
        assert (name is None) != (address is None)
        if name is None:
            if address not in self._contract_names:
                raise ValueError('unknown contract address: %s' % (address,))
            name = self._contract_names[address]
        assert name in self._contracts
        w3 = self.get_polygon_web3()
        abi = self._get_eth_abi(name)
        return w3.eth.contract(address=self._contracts[name], abi=abi)

    def get_quest_name(self, name=None, address=None, short=False):
        assert (name is None) != (address is None)
        if name is None:
            if address not in self._contract_names:
                raise ValueError('unknown contract address: %s' % (address,))
            name = self._contract_names[address]
        assert name in self._contracts
        if name not in self._quest_names:
            raise ValueError('not questing contract address: %s' % (address,))
        return self._quest_names[name][0 if short else 1]

    def opendb(self, dbpath=None):
        import sqlite3
        cru = __import__('cr-update')
        db = sqlite3.connect(self.db_path if dbpath is None else dbpath)
        cru.checkdb(db)
        return db


conf = CRConf()


def update_or_dump_conf(cf, group, dump=False):
    updated = False
    md = cf.config_metadata(group)
    try:
        for sect in sorted(md.keys()):
            print('# %s configuration:' % (sect,))
            for key in sorted(md[sect].keys()):
                if 'value' in md[sect][key]:
                    print('%s: %s' % (
                        md[sect][key]['name'], md[sect][key]['value']))
                elif not dump:
                    val = input('%s: ' % (md[sect][key]['name'],))
                    if len(val.strip()):
                        updated = True
                        cf.update(sect, key, val.strip())
    except EOFError:
        pass
    return updated


def input_bool(prompt):
    yea = ('1', 'y', 'yes', 't', 'true')
    naw = ('0', 'n', 'no', 'f', 'false')
    while True:
        res = input(prompt)
        maybe = res.lower().strip()
        if maybe in yea:
            return True
        elif maybe in naw:
            return False


def main():
    cf = CRConf()
    print('loading config...')
    cf.load_config()
    updated = False

    if not cf.can_update_remote and \
       input_bool('Would you like to use a remote database update server? '):
        updated |= update_or_dump_conf(cf, 'remote')
    else:
        update_or_dump_conf(cf, 'remote', dump=True)

    if not cf.can_update_local and \
       (not cf.can_update_remote or input_bool(
           'Would you like to perform local database updates? ')):
        updated |= update_or_dump_conf(cf, 'local')
    else:
        update_or_dump_conf(cf, 'local', dump=True)

    if updated:
        print('saving updated config...')
        cf.save_config()


if __name__ == '__main__':
    main()
