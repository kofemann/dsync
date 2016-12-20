#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
import sys
import logging
import logging.handlers
import os
from os import O_RDONLY, O_WRONLY, O_CREAT, O_EXCL, O_SYNC
from zlib import adler32
from os.path import basename, dirname
import string
import time
from math import floor, log
from datetime import datetime, timedelta
import errno
import getopt


LOG = logging.getLogger("dsync")
LOG.setLevel(logging.INFO)

IO_SIZE = 1024*1024

CSUM_PREFIX = "ADLER32:"
CSUM_PREFIX_LEN = len(CSUM_PREFIX)

SIZE_SUFFIX = ["", "KB", "MB", "GB", "TB"]

def main():
    OUT_LOG = LOG
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'o:')
        for o, a in opts:
            if o == '-o':
                OUT_LOG = logging.getLogger("sdync.out")
                OUT_LOG.setLevel(logging.INFO)
                out_handler = logging.handlers.WatchedFileHandler(a)
                out_formater = logging.Formatter('%(asctime)s - %(message)s', '%Y%m%d%H%M%S')
                out_handler.setFormatter(out_formater)
                OUT_LOG.addHandler(out_handler)

        if len(args) != 2:
            raise getopt.GetoptError("Invalid number of arguments")

        src = args[0]
        dest = args[1]

    except getopt.GetoptError as e:
        sys.stderr.write("Failed to process command line arguments: %s\n" % e)
        sys.stderr.write("\n")
        sys.stderr.write("Usage: dsync [-o <out log>] <local> <remote>\n")
        sys.exit(1)

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
        os.remove(dest)
        sys.exit(4)

    try:
        os.close(inFd)
    except OSError as e:
        LOG.warn("Failed to close source: %s, ignoring" % e.strerror)

    try:
        os.close(outFd)
    except OSError as e:
        LOG.error("Failed to close destination: %s" % e.strerror)
        os.remove(dest)
        sys.exit(5)

    waitForSize(dest, lsize, 10)

    rsum = getSumFromPnfs(dest)
    if lsum != rsum:
        LOG.error("Checksum mismatch: <expected/actual> %s/%s" % (lsum, rsum))
        os.remove(dest)
        sys.exit(6)

    end = datetime.now()
    elapsed = end - start

    pnfsid = getPnfsId(dest)
    speed = lsize/to_seconds(elapsed)

    OUT_LOG.info("%s => %s %s %s %d (%s) %s (%s/s)" % \
        (os.path.realpath(src), dest, pnfsid, lsum, lsize, to_size_string(lsize), elapsed, to_size_string(speed)))
    sys.exit(0)

def to_seconds(t):

    """converts timedelta into total seconds"""
    return (t.microseconds + (t.seconds + t.days * 24 * 3600) * 10**6) / 10**6

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

def getPnfsId(path):

    """Get file's pnfsid by accessing magic file"""

    d = dirname(path)
    b = basename(path)

    pnfsid_file = "%s/.(id)(%s)" % (d, b)
    with open(pnfsid_file) as f:
        id = f.read().strip()
        return id

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
