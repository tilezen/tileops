import requests
from HTMLParser import HTMLParser
import re
from datetime import datetime


planet_file_pattern = re.compile('^planet-([0-9]{6}).osm.pbf$')


class LatestLinkFinder(HTMLParser, object):
    def __init__(self):
        super(LatestLinkFinder, self).__init__()
        self.date = None

    def handle_starttag(self, tag, attrs):
        dict_attrs = dict(attrs)
        if tag == 'a' and 'href' in dict_attrs:
            match = planet_file_pattern.match(dict_attrs['href'])
            if match:
                date = datetime.strptime(match.groups()[0], '%y%m%d').date()
                if self.date is None or date > self.date:
                    self.date = date


def latest_planet_date():
    link_finder = LatestLinkFinder()
    req = requests.get('https://planet.openstreetmap.org/pbf/')
    assert req.status_code == 200
    link_finder.feed(req.text)
    assert link_finder.date
    return link_finder.date
