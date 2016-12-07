#!/usr/bin/env python

from __future__ import division
import sys
import logging
import os
from os import O_RDONLY, O_WRONLY, O_CREAT, O_EXCL, O_SYNC
from zlib import adler32
from os.path import basename, dirname
import string
import time
from math import floor, log
from datetime import datetime, timedelta
import errno


LOG = logging.getLogger("dsync")
LOG.setLevel(logging.INFO)

IO_SIZE = 1024*1024

CSUM_PREFIX = "ADLER32:"
CSUM_PREFIX_LEN = len(CSUM_PREFIX)

SIZE_SUFFIX = ["", "KB", "MB", "GB", "TB"]

def main():
    
    if len(sys.argv) != 3:
        LOG.error("Usage: dsync <local> <remote>")
        sys.exit(1)
    
    src = sys.argv[1]
    dest = sys.argv[2]

    LOG.info("Starting backup of %s" % src)
    start = datetime.now()

    try:
        inFd = os.open(src, O_RDONLY)
        lsize = os.stat(src).st_size
    except OSError as e:
        LOG.error("Failed to open source file: %s" % e.strerror)
        sys.exit(2)

    try:
        outFd = os.open(dest, O_WRONLY | O_CREAT | O_EXCL | O_SYNC, 0600)
    except OSError as e:
        LOG.error("Failed to create destination file: %s" % e.strerror)
        sys.exit(3)

    try:
        lsum = copy(inFd, outFd)
    except OSError as e:
        LOG.error("Failed to copy: %s" % e.strerror)
        sys.exit(4)

    try:
        os.close(outFd)
    except OSError as e:
        LOG.error("Failed to close destination: %s" % e.strerror)
        sys.exit(5)

    waitForSize(dest, lsize, 10)

    rsum = getSumFromPnfs(dest)
    if lsum != rsum:
        LOG.error("Checksum mismatch: <expected/actual> %s/%s" % (lsum, rsum))
        sys.exit(6)

    end = datetime.now()
    elapsed = end - start

    speed = lsize/elapsed.total_seconds()

    LOG.info("Copy of %s to %s complete in %s (%s/s)" % (src, dest, elapsed, to_size_string(speed)))
    sys.exit(0)

def to_size_string(n):

    """Returs file size in human readabe form, e.g. 8192 => 8Kb"""

    f = int(floor(log(n, 1024)))
    return "%d%s" % (int(n/1024**f), SIZE_SUFFIX[f])

def getSumFromPnfs(path):

    """Get file's checksum by accessing magic file"""

    d = dirname(path)
    b = basename(path)

    checksum_file = "%s/.(get)(%s)(checksum)" % (d, b)
    with open(checksum_file) as f:
        for l in f.readlines():
            i = string.find(l, CSUM_PREFIX)
            if i != -1:
                return l[i+CSUM_PREFIX_LEN:].strip()


def waitForSize(path, size, timeout):

    """Wait until file gets specified size. Wait timeout seconds before retry"""

    while True:
        stat = os.stat(path)
        if stat.st_size == size:
            break
        LOG.info("File size is not ready yet, waiting")
        time.sleep(timeout)

def copy(src, dest):

    """Copy data from src file to dest. Return on-the-fligh calculated local checksum"""

    asum = 1
    while True:
       data = os.read(src, IO_SIZE)
       if len(data) == 0:
            return hex(asum)[2:10].zfill(8).lower()

       asum = adler32(data, asum)
       if asum < 0:
            asum += 2**32
       n = os.write(dest, data)
       if n != len(data):
            raise  OSError(errno.EIO, "Short write")


if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)-15s %(levelname)s: %(message)s")
    main()
