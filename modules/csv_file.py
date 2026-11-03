# (c) Bluemark Innovations BV
# MIT license
"""Append rows to CSV log files without opening the file for every row.

Files stay open with line buffering, so every row is handed to the OS right
away, as before. When a file has been moved or deleted it is opened again.
"""

import csv
import os

_open_files = {}   # filename -> (file, csv writer, inode)


def append_row(filename, row):
    entry = _open_files.get(filename)
    if entry is not None:
        try:
            if os.stat(filename).st_ino != entry[2]:
                raise FileNotFoundError(filename)      # replaced by another file
        except FileNotFoundError:
            entry[0].close()
            entry = None
    if entry is None:
        f = open(filename, 'a', newline='', encoding='utf-8', buffering=1)
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        entry = (f, writer, os.fstat(f.fileno()).st_ino)
        _open_files[filename] = entry
    entry[1].writerow(row)


def close_all():
    for f, _, _ in _open_files.values():
        f.close()
    _open_files.clear()
