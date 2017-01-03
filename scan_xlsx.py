"""
@auto scan_xlsx.py - find XLSX fields

Terry N. Brown, terrynbrown@gmail.com, Tue Jan 03 12:11:30 2017
"""

import datetime
import json
import os
import sys
import zipfile
from collections import namedtuple, defaultdict

from openpyxl import load_workbook

if os.path.exists("xlsx.json"):
    xlsx = json.load(open("xlsx.json"))
else:
    xlsx = {}
    
for line_n, line in enumerate(open("xlsx.lst")):
    break
    # xlsx.lst list of all .xlsx files
    if False and line_n > 10:
        break
    if line_n % 10 == 0:
        json.dump(xlsx, open("xlsx.json", 'w'))
    line = line.strip()
    print("%4d %s" % (line_n, line))
    if line in xlsx:
        continue
    try:
        book = load_workbook(line, read_only=True)
        sheets = book.get_sheet_names()
        sheet = book[sheets[0]]
        try:
            row0 = next(sheet.rows)
            xlsx[line] = [i.value for i in row0]
            xlsx[line] = [
                str(i) if isinstance(i, datetime.datetime) else i
                for i in xlsx[line]
            ]
            xlsx[line] = [
                i.strip() if isinstance(i, (str, unicode)) else i
                for i in xlsx[line]
            ]
        except StopIteration:
            xlsx[line] = ["NO ROWS IN FILE"]
    except zipfile.BadZipfile:
        xlsx[line] = ["BAD ZIPFILE ERROR ON LOAD"]
    except IOError:
        xlsx[line] = ["FILE REMOVED"]

json.dump(xlsx, open("xlsx.json", 'w'))

        
count = defaultdict(lambda: 0)

for fields in xlsx.values():
    for field in fields:
        count[unicode(field).lower().strip()] += 1

results = sorted(count.items(), reverse=True, key=lambda x:(x[1],x[0]))
for result in results:
    print("%4d %s" % (result[1], result[0]))
