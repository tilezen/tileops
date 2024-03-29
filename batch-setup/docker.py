import os.path
from contextlib import contextmanager
from subprocess import check_call

from urlparse import urlparse


@contextmanager
def change_dir(path):
    import os

    oldwd = os.getcwd()
    os.chdir(path)

    try:
        yield
    finally:
        os.chdir(oldwd)


def build_image(path, registry_url):
    registry = urlparse('http://' + registry_url)

    image_name = registry.path
    assert image_name.startswith('/')
    image_name = image_name[1:]

    with change_dir('../docker/%s' % path):
        check_call([
            'make', 'clean', 'image', 'push',
            'REGISTRY=%s' % registry.netloc,
            'IMAGE=%s' % image_name,
        ])


def build_and_upload_images(repo_uris):
    script_dir = os.path.dirname(os.path.realpath(__file__))
    # change directory to be the same as the script, so that the relative paths work.
    with change_dir(script_dir):
        # TODO: this is pretty horrible. perhaps we can specify a git URL + tag to
        # download and use?
        for repo in ('vector-datasource', 'tilequeue', 'raw_tiles'):
            if not os.path.isdir('../../%s' % repo):
                raise RuntimeError('You must check out the %s repository '
                                   'alongside tzops to build Docker images.'
                                   % repo)

        for path, registry_url in repo_uris.items():
            build_image(path, registry_url)
