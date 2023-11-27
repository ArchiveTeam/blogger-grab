# encoding=utf8
import datetime
from distutils.version import StrictVersion
import hashlib
import os.path
import random
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import ExternalProcess
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import GetItemFromTracker, PrepareStatsForTracker, \
    UploadWithTracker, SendDoneToTracker
import shutil
import socket
import subprocess
import sys
import time
import string
import re

if sys.version_info[0] < 3:
    from urllib import unquote
else:
    from urllib.parse import unquote

import seesaw
from seesaw.externalprocess import WgetDownload
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.util import find_executable

from tornado import httpclient

import requests
import zstandard

if StrictVersion(seesaw.__version__) < StrictVersion('0.8.5'):
    raise Exception('This pipeline needs seesaw version 0.8.5 or higher.')


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_AT will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string

class HigherVersion:
    def __init__(self, expression, min_version):
        self._expression = re.compile(expression)
        self._min_version = min_version

    def search(self, text):
        for result in self._expression.findall(text):
            if result >= self._min_version:
                print('Found version {}.'.format(result))
                return True

WGET_AT = find_executable(
    'Wget+AT',
    HigherVersion(
        r'(GNU Wget 1\.[0-9]{2}\.[0-9]{1}-at\.[0-9]{8}\.[0-9]{2})[^0-9a-zA-Z\.-_]',
        'GNU Wget 1.21.3-at.20230623.01'
    ),
    [
        './wget-at',
        '/home/warrior/data/wget-at'
    ]
)

if not WGET_AT:
    raise Exception('No usable Wget+At found.')


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = '20231127.02'
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36'
TRACKER_ID = 'blogger'
TRACKER_HOST = 'legacy-api.arpa.li'
MULTI_ITEM_SIZE = 1000


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.
class CheckIP(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'CheckIP')
        self._counter = 0

    def process(self, item):
        if self._counter <= 0:
            command = [
                WGET_AT,
                '-U', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.9; rv:63.0) Gecko/20100101 Firefox/63.0',
                '--host-lookups', 'dns',
                '--hosts-file', '/dev/null',
                '--resolvconf-file', 'resolv.conf',
                '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
                '--output-document', '-',
                '--max-redirect', '0',
                '--save-headers',
                '--no-check-certificate',
                '--no-hsts'
            ]
            kwargs = {
                'timeout': 60,
                'capture_output': True
            }

            url = 'http://legacy-api.arpa.li/now'
            returned = subprocess.run(
                command+[url],
                **kwargs
            )
            assert returned.returncode == 0, 'Invalid return code {} on {}.'.format(returned.returncode, url)
            assert re.match(
                b'^HTTP/1\\.1 200 OK\r\n'
                b'Server: openresty\r\n'
                b'Date: [A-Z][a-z]{2}, [0-9]{2} [A-Z][a-z]{2} 202[0-9] [0-9]{2}:[0-9]{2}:[0-9]{2} GMT\r\n'
                b'Content-Type: text/plain\r\n'
                b'Connection: keep-alive\r\n'
                b'Content-Length: 1[0-9]\r\n'
                b'Cache-Control: no-store\r\n'
                b'\r\n'
                b'[0-9]{10}\\.[0-9]{1,3}$',
                returned.stdout
            ), 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))

            actual_time = float(returned.stdout.rsplit(b'\n', 1)[1])
            local_time = time.time()
            max_diff = 180
            diff = abs(actual_time-local_time)
            assert diff < max_diff, 'Your time {} is more than {} seconds off of {}.'.format(local_time, max_diff, actual_time)

            for url in (
                'http://domain.invalid/',
                'http://example.test/',
                'http://www/',
                'http://example.test/example',
                'http://nxdomain.archiveteam.org/'
            ):
                returned = subprocess.run(
                    command+[url],
                    **kwargs
                )
                assert len(returned.stdout) == 0, 'Bad stdout on {}, got {}.'.format(url, repr(returned.stdout))
                assert (
                    b'failed: No IPv4/IPv6 addresses for host.\n'
                    b'wget-at: unable to resolve host address'
                ) in returned.stderr, 'Bad stderr on {}, got {}.'.format(url, repr(returned.stderr))
                assert returned.returncode == 4, 'Invalid return code {} on {}.'.format(returned.returncode, url)

        # Check only occasionally
        if self._counter <= 0:
            self._counter = 50
        else:
            self._counter -= 1


class PrepareDirectories(SimpleTask):
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, 'PrepareDirectories')
        self.warc_prefix = warc_prefix

    def process(self, item):
        item_name = item['item_name']
        item_name_hash = hashlib.sha1(item_name.encode('utf8')).hexdigest()
        escaped_item_name = item_name_hash
        dirname = '/'.join((item['data_dir'], escaped_item_name))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)

        os.makedirs(dirname)

        item['item_dir'] = dirname
        item['warc_file_base'] = '-'.join([
            self.warc_prefix,
            item_name_hash,
            time.strftime('%Y%m%d-%H%M%S')
        ])

        open('%(item_dir)s/%(warc_file_base)s.warc.zst' % item, 'w').close()
        open('%(item_dir)s/%(warc_file_base)s_data.txt' % item, 'w').close()

class MoveFiles(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'MoveFiles')

    def process(self, item):
        os.rename('%(item_dir)s/%(warc_file_base)s.warc.zst' % item,
              '%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst' % item)
        os.rename('%(item_dir)s/%(warc_file_base)s_data.txt' % item,
              '%(data_dir)s/%(warc_file_base)s_data.txt' % item)

        shutil.rmtree('%(item_dir)s' % item)


def normalize_item(url):
    while True:
        temp = unquote(url).strip().lower()
        if temp == url:
            break
        url = temp
    if url.count('/') < 3:
        url += '/'
    return url


class SetBadUrls(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'SetBadUrls')

    def process(self, item):
        item['item_name_original'] = item['item_name']
        items = item['item_name'].split('\0')
        items_lower = [normalize_item(s) for s in items]
        with open('%(item_dir)s/%(warc_file_base)s_bad-items.txt' % item, 'r') as f:
            for aborted_item in f:
                aborted_item = normalize_item(aborted_item)
                index = items_lower.index(aborted_item)
                item.log_output('Item {} is aborted.'.format(aborted_item))
                items.pop(index)
                items_lower.pop(index)
        item['item_name'] = '\0'.join(items)


class MaybeSendDoneToTracker(SendDoneToTracker):
    def enqueue(self, item):
        if len(item['item_name']) == 0:
            return self.complete_item(item)
        return super(MaybeSendDoneToTracker, self).enqueue(item)


def get_hash(filename):
    with open(filename, 'rb') as in_file:
        return hashlib.sha1(in_file.read()).hexdigest()

CWD = os.getcwd()
PIPELINE_SHA1 = get_hash(os.path.join(CWD, 'pipeline.py'))
LUA_SHA1 = get_hash(os.path.join(CWD, 'blogger.lua'))

def stats_id_function(item):
    d = {
        'pipeline_hash': PIPELINE_SHA1,
        'lua_hash': LUA_SHA1,
        'python_version': sys.version,
    }

    return d


class ZstdDict(object):
    created = 0
    data = None

    @classmethod
    def get_dict(cls):
        if cls.data is not None and time.time() - cls.created < 1800:
            return cls.data
        response = requests.get(
            'https://legacy-api.arpa.li/dictionary',
            params={
                'project': TRACKER_ID
            }
        )
        response.raise_for_status()
        response = response.json()
        if cls.data is not None and response['id'] == cls.data['id']:
            cls.created = time.time()
            return cls.data
        print('Downloading latest dictionary.')
        response_dict = requests.get(response['url'])
        response_dict.raise_for_status()
        raw_data = response_dict.content
        if hashlib.sha256(raw_data).hexdigest() != response['sha256']:
            raise ValueError('Hash of downloaded dictionary does not match.')
        if raw_data[:4] == b'\x28\xB5\x2F\xFD':
            raw_data = zstandard.ZstdDecompressor().decompress(raw_data)
        cls.data = {
            'id': response['id'],
            'dict': raw_data
        }
        cls.created = time.time()
        return cls.data


class WgetArgs(object):
    post_chars = string.digits + string.ascii_lowercase

    def int_to_str(self, i):
        d, m = divmod(i, 36)
        if d > 0:
            return self.int_to_str(d) + self.post_chars[m]
        return self.post_chars[m]

    def realize(self, item):
        wget_args = [
            WGET_AT,
            '-U', USER_AGENT,
            '-nv',
            '--host-lookups', 'dns',
            '--hosts-file', '/dev/null',
            '--resolvconf-file', 'resolv.conf',
            '--dns-servers', '9.9.9.10,149.112.112.10,2620:fe::10,2620:fe::fe:10',
            '--load-cookies', 'cookies.txt',
            '--content-on-error',
            '--lua-script', 'blogger.lua',
            '-o', ItemInterpolation('%(item_dir)s/wget.log'),
            '--no-check-certificate',
            '--output-document', ItemInterpolation('%(item_dir)s/wget.tmp'),
            '--truncate-output',
            '-e', 'robots=off',
            '--rotate-dns',
            '--recursive', '--level=inf',
            '--no-parent',
            '--page-requisites',
            '--timeout', '4',
            '--tries', 'inf',
            '--domains', 'blogspot.com,blogger.com',
            '--span-hosts',
            '--waitretry', '4',
            '--warc-file', ItemInterpolation('%(item_dir)s/%(warc_file_base)s'),
            '--warc-header', 'operator: Archive Team',
            '--warc-header', 'x-wget-at-project-version: ' + VERSION,
            '--warc-header', 'x-wget-at-project-name: ' + TRACKER_ID,
            '--warc-dedup-url-agnostic',
            '--warc-compression-use-zstd',
            '--warc-zstd-dict-no-include',
            '--header', 'Accept-Language: en-US;q=0.9, en;q=0.8'
        ]
        dict_data = ZstdDict.get_dict()
        with open(os.path.join(item['item_dir'], 'zstdict'), 'wb') as f:
            f.write(dict_data['dict'])
        item['dict_id'] = dict_data['id']
        item['dict_project'] = TRACKER_ID
        wget_args.extend([
            '--warc-zstd-dict', ItemInterpolation('%(item_dir)s/zstdict'),
        ])

        if '--concurrent' in sys.argv:
            concurrency = int(sys.argv[sys.argv.index('--concurrent')+1])
        else:
            concurrency = os.getenv('CONCURRENT_ITEMS')
            if concurrency is None:
                concurrency = 4
        item['concurrency'] = str(concurrency)

        for item_name in item['item_name'].split('\0'):
            wget_args.extend(['--warc-header', 'x-wget-at-project-item-name: '+item_name])
            wget_args.append('item-name://'+item_name)
            item_type, item_value = item_name.split(':', 1)
            if item_type == 'blog':
                wget_args.extend(['--warc-header', 'blogger-blog: '+item_value])
                wget_args.append('https://{}.blogspot.com/'.format(item_value))
            elif item_type == 'url':
                wget_args.extend(['--warc-header', 'blogger-url: '+item_value])
                wget_args.append('https://'+item_value)
            elif item_type == 'profile':
                wget_args.extend(['--warc-header', 'blogger-profile: '+item_value])
                wget_args.append('https://www.blogger.com/profile/'+item_value)
            elif item_type in ('article', 'page', 'search'):
                blog, path = item_value.split(':', 1)
                wget_args.extend(['--warc-header', 'blogger-{}: {}'.format(item_type, path)])
                wget_args.append('https://{}.blogspot.com/{}'.format(blog, path))
            else:
                raise Exception('Unknown item')

        item['item_name_newline'] = item['item_name'].replace('\0', '\n')

        if 'bind_address' in globals():
            wget_args.extend(['--bind-address', globals()['bind_address']])
            print('')
            print('*** Wget will bind address at {0} ***'.format(
                globals()['bind_address']))
            print('')

        return realize(wget_args, item)

###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title=TRACKER_ID,
    project_html='''
        <img class="project-logo" alt="Project logo" src="https://wiki.archiveteam.org/images/thumb/2/2e/Blogger-icon.png/603px-Blogger-icon.png" height="50px" title=""/>
        <h2>Blogger <span class="links"><a href="https://blogger.com/">Website</a> &middot; <a href="http://tracker.archiveteam.org/blogger/">Leaderboard</a> &middot; <a href="https://wiki.archiveteam.org/index.php/Blogger">Wiki</a></span></h2>
        <p>Archiving Blogger.</p>
    '''
)

pipeline = Pipeline(
    CheckIP(),
    GetItemFromTracker('http://{}/{}/multi={}/'
        .format(TRACKER_HOST, TRACKER_ID, MULTI_ITEM_SIZE),
        downloader, VERSION),
    PrepareDirectories(warc_prefix=TRACKER_ID),
    WgetDownload(
        WgetArgs(),
        max_tries=2,
        accept_on_exit_code=[0, 4, 8],
        env={
            'item_dir': ItemValue('item_dir'),
            'item_names': ItemValue('item_name_newline'),
            'warc_file_base': ItemValue('warc_file_base'),
            'concurrency': ItemValue('concurrency'),
        }
    ),
    SetBadUrls(),
    PrepareStatsForTracker(
        defaults={'downloader': downloader, 'version': VERSION},
        file_groups={
            'data': [
                ItemInterpolation('%(item_dir)s/%(warc_file_base)s.warc.zst')
            ]
        },
        id_function=stats_id_function,
    ),
    MoveFiles(),
    LimitConcurrent(NumberConfigValue(min=1, max=20, default='20',
        name='shared:rsync_threads', title='Rsync threads',
        description='The maximum number of concurrent uploads.'),
        UploadWithTracker(
            'http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
            downloader=downloader,
            version=VERSION,
            files=[
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s.%(dict_project)s.%(dict_id)s.warc.zst'),
                ItemInterpolation('%(data_dir)s/%(warc_file_base)s_data.txt')
            ],
            rsync_target_source_path=ItemInterpolation('%(data_dir)s/'),
            rsync_extra_args=[
                '--recursive',
                '--partial',
                '--partial-dir', '.rsync-tmp',
                '--min-size', '1',
                '--no-compress',
                '--compress-level', '0'
            ]
        ),
    ),
    MaybeSendDoneToTracker(
        tracker_url='http://%s/%s' % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue('stats')
    )
)
