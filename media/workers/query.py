import imaplib
from email import message_from_string
import re
import logging

from systools.system import loop, timeout, timer

from filetools.title import clean

from mediacore.model.search import Search, add_movies, add_music
from mediacore.model.settings import Settings


CAT_DEF = {
    'anime': re.compile(r'\banime\b', re.I),
    'apps': re.compile(r'\bapplications?\b', re.I),
    'books': re.compile(r'\be?books?\b', re.I),
    'games': re.compile(r'\bgames?\b', re.I),
    'movies': re.compile(r'\b(movies?|video)\b', re.I),
    'music': re.compile(r'\b(audio|music)\b', re.I),
    'tv': re.compile(r'\b(tv|tv\s*shows?)\b', re.I),
    }
RE_ARTIST = {
    'movies': re.compile(r'\b(artist|director|actor)\b', re.I),
    'music': re.compile(r'\b(artist|band)\b', re.I),
    }

logger = logging.getLogger(__name__)


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


def get_category_info(val):
    for category, re_cat in CAT_DEF.items():
        if re_cat.search(val):
            re_ = RE_ARTIST.get(category)
            return category, re_ and re_.search(val)
    return None, False

def process_query(query):
    parts = [v.strip() for v in query.split(',')]
    if len(parts) < 2:
        return False
    category, is_artist = get_category_info(clean(parts.pop(0)))
    if category is None:
        return False
    name = clean(parts.pop(0), 1)
    if not name:
        return False
    artist = name if is_artist else None

    logger.info('processing query "%s"', query)

    langs = Settings.get_settings('media_langs').get(category, [])
    search = {
        'name': name,
        'category': category,
        'mode': 'once',
        'langs': langs,
        }

    if category == 'music':
        if not parts:
            artist = name
        if artist:
            return add_music(artist)

        search['album'] = clean(parts.pop(0), 1)
        if not search['album']:
            return False

    elif category == 'movies':
        if artist:
            return add_movies(artist, langs=langs)

    elif category in ('tv', 'anime'):
        search['mode'] = 'inc'
        search['season'] = 1
        search['episode'] = 1

    if Search.add(**search):
        logger.info('created %s search %s', category, search)
    return True

@loop(60)
@timeout(minutes=10)
@timer(60)
def run():
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
