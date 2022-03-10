#!./venv/bin/python
import appdirs
import configparser
import io
import os
import requests
import datetime

appname = 'crutil'

slots = ('main_hand', 'dress', 'knickknack', 'finger', 'background')


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
            '0xe193364370F0E2923b41a8d1850F442B45E5ccA7': 'grimweed-quest',
            '0xF001508171344A4bc90fdA37890e343749d5D216': 'recruiting-history',
        }
        self._contracts = {v: k for k, v in self._contract_names.items()}
        self._quest_names = {
            'grimweed-quest': 'The Hunt for Grimweed',
            'newt-quest': 'Newt Slayer',
        }
        self._quests = {self._contracts[k]: v
                        for k, v in self._quest_names.items()}

        self._polygon_web3 = None
        self._schema = {
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
        self._loaded = {i: {} for i in self._schema.keys()}

    def nft_owners(self):
        return ' '.join(self.nft_owner.split(',')).split()

    def makedirs(self):
        if not os.path.exists(self._confdir):
            os.makedirs(self._confdir)
        if not os.path.exists(self._abidir):
            os.makedirs(self._abidir)
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
            import web3
            self._polygon_web3 = web3.Web3(web3.Web3.HTTPProvider('%s/%s' % (
                self.alchemy_api_url, self.alchemy_api_key)))
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


conf = CRConf()


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
