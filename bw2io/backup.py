# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals
from eight import *

from bw2data import projects
import datetime
import os
import tarfile


def backup_data_directory():
    """Backup data directory to a ``.tar.gz`` (compressed tar archive).

    Backup archive is saved to the user's home directory.

    Restoration is done manually. Returns the filepath of the backup archive."""
    fp = os.path.join(
        os.path.expanduser("~"),
        u"brightway2-data-backup.{}.tar.gz".format(
            datetime.datetime.now().strftime("%d-%B-%Y-%I-%M%p")
        )
    )
    print(u"Creating backup archive - this could take a few minutes...")
    with tarfile.open(fp, "w:gz") as tar:
        tar.add(projects.dir, arcname=os.path.basename(projects.dir))
