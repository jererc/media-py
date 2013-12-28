import httplib2
import imaplib
from email import message_from_string
import time
from calendar import timegm
from urlparse import urlparse
import re
import logging

import dateutil.parser

from lxml import html

logging.getLogger('apiclient').setLevel(logging.ERROR)
logging.getLogger('oauth2client').setLevel(logging.ERROR)

from apiclient import errors
from apiclient.discovery import build
from apiclient.http import MediaInMemoryUpload
from oauth2client.client import Credentials, AccessTokenRefreshError

from systools.system import loop, timeout, timer

from transfer import Transfer

from mediacore.model.search import Search
from mediacore.model.settings import Settings
from mediacore.utils.query import get_searches, QueryError


MODIFIED_DELTA = 30     # drive file modified delta in seconds

logger = logging.getLogger(__name__)


class DriveError(Exception): pass


class DriveClient(object):

    def __init__(self, credentials):
        credentials_ = Credentials.new_from_json(credentials)
        http = httplib2.Http()
        http = credentials_.authorize(http)
        self.service = build('drive', 'v2', http=http)

    def get_file_by_id(self, file_id):
        return self.service.files().get(fileId=file_id).execute()

    def get_file_by_title(self, title):
        res = self.service.files().list(q="title='%s'" % title).execute()
        return res['items'][0] if res['items'] else None

    def get_file_content(self, file, mime_type='text/html'):
        url = file.get('exportLinks', {}).get(mime_type)
        if not url:
            raise DriveError('missing file url for mime type %s: %s' % (mime_type, file))
        resp, content = self.service._http.request(url)
        if resp.status != 200:
            raise DriveError('google api error: %s' % resp)
        return content.decode('utf-8-sig')

    def set_file_content(self, file, body, mime_type='text/html'):
        media = MediaInMemoryUpload(body, mimetype=mime_type)
        try:
            return self.service.files().update(fileId=file['id'],
                    newRevision=True, media_body=media).execute()
        except errors.HttpError, e:
            raise DriveError('google api error: %s' % str(e))


class FileLine(object):

    def __init__(self, element):
        self.element = element
        self.spans = self.element.cssselect('span')

    def _get_line(self):
        res = ''
        for el in self.spans:
            links = el.cssselect('a')
            res += links[0].get('href') if links else el.text
        return res

    def _get_element(self):
        return html.tostring(self.element)

    def _get_sub_pattern(self):
        return re.sub(r'class\\=\\"[^\"]*\\"', r'class="([^\"]*)"',
                re.escape(self._get_element()))

    def _replace(self, matchobj):
        self.group_num += 1
        return 'class="\\%d"' % self.group_num

    def _get_sub_repl(self):
        self.group_num = 0
        return re.sub(r'class="([^\"]*)"', self._replace, self._get_element())

    def get_query(self):
        if self.spans:
            begin = self.spans[0].text
            if begin and begin.startswith('?'):
                return self._get_line().lstrip('?').strip()

    def get_patterns(self):
        pattern = self._get_sub_pattern()
        self.spans[0].text = self.spans[0].text.lstrip('?')
        repl = self._get_sub_repl()
        return pattern, repl


class GmailClient(object):

    def __init__(self, host, port, username, password):
        self.client = imaplib.IMAP4_SSL(host, port)
        self.client.login(username, password)
        self.client.select()

    def __del__(self):
        try:
            self.client.close()
        except:
            pass
        self.client.logout()

    def extract_body(self, payload):
        if not isinstance(payload, basestring):
            return '\n'.join([self.extract_body(p.get_payload()) for p in payload])
        return payload

    def iter_messages(self, from_email):
        args = (None,)
        args += ('FROM', from_email) if from_email else ('UNSEEN',)
        typ, data = self.client.search(*args)
        for num in data[0].split():
            typ, msg_data = self.client.fetch(num, '(RFC822)')
            for response_part in msg_data:
                if isinstance(response_part, tuple):
                    msg = message_from_string(response_part[1])
                    payload = msg.get_payload()
                    yield {
                        'num': num,
                        'subject': msg['subject'],
                        'body': self.extract_body(payload),
                        }

    def delete(self, num):
        typ, response = self.client.store(num, '+FLAGS', r'(\Deleted)')


def check_modified(date, delta):
    modified = dateutil.parser.parse(date)
    elapsed = int(time.time()) - timegm(modified.utctimetuple())
    return elapsed > delta

def process_query(query):
    if urlparse(query).scheme:
        dst = Settings.get_settings('paths')['finished_download']
        try:
            Transfer.add(query, dst)
        except Exception, e:
            logger.error('failed to create transfer for %s: %s', query, str(e))
            return False

    else:
        try:
            searches = get_searches(query)
        except QueryError, e:
            logger.info(str(e))
            return False

        if not searches:
            logger.info('no result for query "%s', query)
        else:
            for search in searches:
                if Search.add(**search):
                    logger.info('created search %s', search)

    return True

def process_file_queries(body):
    '''Process the file queries and return a list of patterns and replacement strings.
    '''
    res = []
    tree = html.fromstring(body)
    for element in tree.cssselect('body p'):
        line = FileLine(element)
        query = line.get_query()
        if query is None:
            continue
        if not process_query(query):
            continue
        res.append(line.get_patterns())

    return res

@timeout(minutes=10)
@timer(60)
def process_drive():
    file_title = Settings.get_settings('google_drive').get('file_title')
    if not file_title:
        return
    credentials = Settings.get_settings('google_api_credentials').get('credentials')
    if not credentials:
        return

    drive = DriveClient(credentials)
    try:
        file_ = drive.get_file_by_title(file_title)
        if not file_:
            return
        if not check_modified(file_['modifiedDate'], delta=MODIFIED_DELTA):
            return

        body = drive.get_file_content(file_, mime_type='text/html')
        patterns = process_file_queries(body)
        if patterns:
            body = drive.get_file_content(file_, mime_type='text/html')
            for pattern, repl in patterns:
                body = re.sub(pattern, repl, body, 1)
            drive.set_file_content(file_, body=body, mime_type='text/html')

    except AccessTokenRefreshError:
        Settings.set_settings('google_api_credentials', {'credentials': None})
        logger.error('revoked or expired credentials')
    except DriveError, e:
        logger.error(str(e))

@timeout(minutes=10)
@timer(60)
def process_email():
    email_ = Settings.get_settings('email')
    if not email_.get('host') \
            or not email_.get('port') \
            or not email_.get('username') \
            or not email_.get('password') \
            or not email_.get('from_email'):
        return

    client = GmailClient(host=email_['host'],
            port=email_['port'],
            username=email_['username'],
            password=email_['password'])
    for message in client.iter_messages(email_['from_email']):
        if process_query(message['subject']):
            client.delete(message['num'])

@loop(60)
def run():
    process_drive()
    process_email()
