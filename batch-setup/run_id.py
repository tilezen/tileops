import re
import sys


def assert_run_id_format(run_id):
    """
    Checks that the run ID has a format that means we can use it in resource
    names, directory and file names, etc... without any problems. Many AWS
    resources are quite restrictive in terms of the characters that they can
    contain.
    """

    m = re.match('^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$', run_id)
    if m is None:
        print('Run ID %r is badly formed. Run IDs may only contain ASCII '
              'letters, numbers and dashes. Dashes may not appear at the '
              'beginning or end of the run ID.' % (run_id,))
        sys.exit(1)
