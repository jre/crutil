import argparse
import datetime
import json
import os
import queue
import sys
import threading
import time
import traceback

import flask
from flup.server.fcgi import WSGIServer

cr_conf = __import__('cr-conf')
cf = cr_conf.conf
cru = __import__('cr-update')


latest = {}
latest_lock = threading.Lock()
latest_file = None
webapp = flask.Flask(__name__)
api_key = None
rebuilder = None
updater = None
all_raider_ids = set()
geardb = cru.geardb
geardb_file = None


def load_latest(path):
    global latest, latest_file
    latest_file = path
    if os.path.exists(path):
        with open(path) as fh:
            latest = json.load(fh)


def update_latest(info):
    global latest
    with latest_lock:
        if (info['snapshot-started'] < latest.get('snapshot-started', 0) or
            (info['snapshot-started'] == latest.get('snapshot-started', 0) and
             info['snapshot-updated'] <= latest.get('snapshot-updated', 0))):
            return
        latest = info
        if latest_file is None:
            return
        with cru.permatempfile(latest_file, suffix='.json',
                               binary=False) as fh:
            json.dump(info, fh)


def load_geardb(path, trysql=()):
    global geardb_file
    geardb_file = path
    if os.path.exists(geardb_file):
        geardb.load(open(geardb_file))
        return True
    for dbpath in trysql:
        if os.path.exists(dbpath):
            db = cf.opendb(dbpath)
            cru.setupdb(db)
            geardb.load_from_sql(db.cursor())
            return True


def save_geardb():
    with cru.permatempfile(geardb_file, suffix='.json',
                           binary=False) as fh:
        geardb.save(fh)


@webapp.route("/latest")
def handle_latest():
    info = latest.copy()
    if info.get('schema-version') != cru.schema_version:
        info = {}
    if 'path' in info:
        info['url'] = '%s/%s' % (flask.request.host_url.rstrip('/'),
                                 info['path'].lstrip('/'))
        del info['path']
    return info


@webapp.route("/rebuild")
def handle_rebuild():
    if api_key is not None and flask.request.args.get('apikey') != api_key:
        return text_response('invalid api key', 403)

    resp_code, resp_body = rebuilder.request_db_rebuild()
    if isinstance(resp_body, str):
        return text_response(resp_body, resp_code)
    return status_generator_response(resp_body, resp_code)


@webapp.route("/update")
def handle_update():
    if api_key is not None and flask.request.args.get('apikey') != api_key:
        return text_response('invalid api key', 403)
    rids = flask.request.args.getlist('ids[]', type=int)
    invalid = set(rids).difference(all_raider_ids)
    if invalid:
        return text_response('invalid raider id(s): %s' % (
            ', '.join(map(str, sorted(invalid)))), 400)
    params = {}
    for key, val in flask.request.args.items():
        if key in ('apikey', 'ids[]'):
            continue
        elif key.startswith('no-'):
            short = key.split('-', 1)[1]
            if short in ('basic', 'gear', 'recruiting', 'questing'):
                params[short] = False
                continue
        return text_response('unknown parameter: %s' % (key,), 400)

    resp_code, resp_body = updater.request_db_update(rids, params)
    if isinstance(resp_body, str):
        return text_response(resp_body, resp_code)
    return status_generator_response(resp_body, resp_code)


def text_response(body, code):
    resp = webapp.make_response((body, code))
    resp.mimetype = 'text/plain'
    return resp


def status_generator_response(status_queue, code):
    def status_generator():
        while True:
            msg = status_queue.get()
            if msg is None:
                return
            yield str(msg) + '\n'
    resp = webapp.response_class(status_generator(), code)
    resp.mimetype = 'text/plain'
    return resp


class BaseThread(threading.Thread):
    class ExitThread(Exception):
        pass

    def __init__(self, name, workdir, wwwdir, baseurlpath):
        super().__init__(name=name)
        self._workdir = workdir
        self._wwwdir = wwwdir
        self._baseurlpath = baseurlpath.rstrip('/')
        self._lastsect = ''
        self._session = cf.requests_session()
        self._exiting = False
        self._exitmsg = 'Server shutting down'

    def _yield(self):
        # yield control to signal handlers and other threads
        time.sleep(0.0001)

    def _dbdump_filename(self, when):
        when = datetime.datetime.fromtimestamp(when)
        return 'raiders-v%d-%sZ.sqlite.gz' % (
            cru.schema_version, when.isoformat(timespec='seconds'))

    def _periodic(self, section=None, message=None):
        if self._exiting:
            raise self.ExitThread()
        if section:
            self.__lastsect = section
        if message:
            self._publish_status('%s: %s' % (self.__lastsect, message))
        elif section:
            self._publish_status(section)
        self._yield()

    def run(self):
        try:
            self._run()
        except self.ExitThread:
            self._publish_eof()

    def _update_db(self, db_path, params={}):
        db = cf.opendb(db_path)
        cru.setupdb(db)
        self._periodic()

        params.update({'periodic': self._periodic, 'session': self._session})
        info, idlist = cru.import_or_update(db, **params)
        dumpfile = self._dbdump_filename(info['snapshot-updated'])
        info['path'] = '%s/%s' % (self._baseurlpath, dumpfile)
        cru.gzip_to(db_path, self._wwwdir, dumpfile, periodic=self._periodic)
        update_latest(info)
        self._session.close()
        return info, idlist

    def _publish_eof(self):
        self._publish_status(self._exitmsg)
        self._publish_status(None)


class RebuildThread(BaseThread):
    def __init__(self, **kw):
        super().__init__('rebuilder', **kw)
        self.__building = threading.Event()
        self.__sub = []
        self.__sub_lock = threading.Lock()

    def _run(self):
        global all_raider_ids
        db_path = os.path.join(self._workdir, 'new.sqlite')
        while self.__building.wait():
            try:
                self._periodic('Building new database')
                if os.path.exists(db_path):
                    os.unlink(db_path)
                _, all_rids = self._update_db(db_path)
                all_raider_ids = set(all_rids)
                os.rename(db_path, updater._new_db_path)
                save_geardb()
            except Exception:
                self._periodic(message=traceback.format_exc())
            with self.__sub_lock:
                self._publish_status(None)
                self._lastsect = ''
                self.__building.clear()
                self.__sub = []

    def request_db_rebuild(self):
        if self._exiting:
            return 503, self._exitmsg
        with self.__sub_lock:
            self.__building.set()
            q = queue.SimpleQueue()
            self.__sub.append(q)
            return 200, q

    def request_exit(self):
        with self.__sub_lock:
            self._exiting = True
            self.__building.set()

    def _publish_status(self, msg):
        for i in self.__sub:
            i.put(msg)


class UpdateThread(BaseThread):
    def __init__(self, **kw):
        super().__init__('updater', **kw)
        self.__requests = queue.SimpleQueue()
        self.__status = None
        self._base_db_path = os.path.join(self._workdir, 'update-base.sqlite')
        self._new_db_path = os.path.join(self._workdir, 'new-base-db.sqlite')

    def _run(self):
        while True:
            params, self.__status = self.__requests.get()
            try:
                self._periodic('Updating database')
                if os.path.exists(self._new_db_path):
                    os.rename(self._new_db_path, self._base_db_path)
                self._update_db(self._base_db_path, params)
                save_geardb()
            except Exception:
                self._periodic(message=traceback.format_exc())
            self._publish_status(None)
            self._lastsect = ''
            self.__status = None

    def request_db_update(self, raiders, params={}):
        if self._exiting:
            return 503, self._exitmsg
        params = params.copy()
        assert not set(raiders).difference(all_raider_ids)
        params['raiders'] = sorted(raiders)
        status = queue.SimpleQueue()
        self.__requests.put((params, status))
        return 200, status

    def _publish_eof(self):
        super()._publish_eof()
        self.__requests.put((None, None))
        while True:
            _, q = self.__requests.get_nowait()
            if q is None:
                return
            q.put(self._exitmsg)
            q.put(None)

    def request_exit(self):
        self._exiting = True
        self.__requests.put((None, None))

    def _publish_status(self, msg):
        if self.__status is not None:
            self.__status.put(msg)


class QuitterServer(WSGIServer):
    def _intHandler(self, signum, frame):
        super()._intHandler(signum, frame)
        rebuilder.request_exit()
        updater.request_exit()

    def _hupHandler(self, signum, frame):
        super()._hupHandler(signum, frame)
        rebuilder.request_exit()
        updater.request_exit()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('dbdir', help='Directory to store db files')
    parser.add_argument('dburlpath', help='URL path for db files')
    parser.add_argument('-k', dest='apikeyfile', help='API key file')
    args = parser.parse_args()

    if args.apikeyfile:
        global api_key
        api_key = open(args.apikeyfile).read().strip()
    if not cf.load_config():
        print('error: please run ./cr-conf.py to configure')
        sys.exit(1)
    cf.makedirs()

    workdir = os.path.expanduser('~/crudb-workdir')
    os.makedirs(workdir, exist_ok=True)
    load_latest(os.path.join(workdir, 'latest.json'))
    all_raider_ids.update(*cru.get_raider_ids(session=cf.requests_session()))

    global rebuilder, updater
    thrp = {'workdir': workdir, 'wwwdir': args.dbdir,
            'baseurlpath': args.dburlpath}
    rebuilder = RebuildThread(**thrp)
    updater = UpdateThread(**thrp)
    load_geardb(os.path.join(workdir, 'gear.json'),
                trysql=(updater._new_db_path, updater._base_db_path))

    rebuilder.start()
    updater.start()
    QuitterServer(webapp, multiplexed=True,
                  bindAddress='/var/www/crutil/fastcgi.sock',
                  umask=0o111).run()


if __name__ == '__main__':
    main()
