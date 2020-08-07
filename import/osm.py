import requests
import re
from datetime import datetime


planet_file_pattern = re.compile('^https://planet.openstreetmap.org/pbf/planet-([0-9]{6}).osm.pbf$')


def latest_planet_date():
    # Fetch the latest planet redirect
    req = requests.get('https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf', allow_redirects=False)
    assert req.status_code == 302
    assert req.next
    assert req.next.url

    # Match the redirect URL to extract the date
    match = planet_file_pattern.match(req.next.url)
    assert match

    # Parse the date and return it
    date = datetime.strptime(match.groups(1), '%y%m%d').date()
    assert date

    return date
