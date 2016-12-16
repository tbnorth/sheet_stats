"""
sheet_stats.py - report column stats for spreadsheets

requires openpyxl and numpy

Terry N. Brown, terrynbrown@gmail.com, Fri Dec 16 13:20:47 2016
"""

import csv
import argparse
import glob
import multiprocessing
import os
import sys
from collections import namedtuple, defaultdict

from openpyxl import load_workbook

import numpy as np

def make_parser():
    """build an argparse.ArgumentParser, don't call this directly,
       call get_options() instead.
    """
    parser = argparse.ArgumentParser(
        description="""Report column stats for spreadsheets""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('files', type=str, nargs='+',
        help="Files to process, '*' patterns expanded."
    )

    return parser

def get_options(args=None):
    """
    get_options - use argparse to parse args, and return a
    argparse.Namespace, possibly with some changes / expansions /
    validatations.

    Client code should call this method with args as per sys.argv[1:],
    rather than calling make_parser() directly.

    :param [str] args: arguments to parse
    :return: options with modifications / validations
    :rtype: argparse.Namespace
    """
    opt = make_parser().parse_args(args)

    # modifications / validations go here

    return opt

def proc_file(filepath):

    print filepath

    wb = load_workbook(filename=filepath, read_only=True)
    sheets = wb.get_sheet_names()
    ws = wb[sheets[0]]
    row0 = next(ws.rows)
    fields = [i.value for i in row0]

    cols = len(fields)

    n = np.zeros(cols, dtype=int)
    sums = np.zeros(cols)
    sumssq = np.zeros(cols)
    blank = np.zeros(cols, dtype=int)
    bad = np.zeros(cols, dtype=int)
    mins = np.zeros(cols) + np.nan
    maxs = np.zeros(cols) + np.nan

    rows = 0

    for row in ws.rows:
        if rows % 1000 == 0:
            print rows
            if os.path.exists("STOP"):
                return
        rows += 1

        for cell_n, cell in enumerate(row):
            if cell.value is None or str(cell.value).strip() == '':
                blank[cell_n] +=1
            else:
                try:
                    x = float(cell.value)
                    sums[cell_n] += x
                    sumssq[cell_n] += x*x
                    n[cell_n] += 1
                    if np.isnan(mins[cell_n]):
                        mins[cell_n] = x
                    else:
                        mins[cell_n] = min(mins[cell_n], x)
                    if np.isnan(maxs[cell_n]):
                        maxs[cell_n] = x
                    else:
                        maxs[cell_n] = max(maxs[cell_n], x)
                except ValueError:
                    bad[cell_n] += 1

    assert sum(n) + sum(blank) + sum(bad) == rows * len(fields)

    ans = []
    for i in range(len(fields)):
        ans.append([
            filepath, fields[i], sums[i], sumssq[i],
            mins[i], maxs[i], n[i], blank[i], bad[i]
        ])
    return ans

def main():

    opt = get_options()

    # pass filenames through glob() to expand "2017_*.xlsx" etc.
    files = []
    for filepath in opt.files:
        files.extend(glob.glob(filepath))

    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)

    answers = pool.map(proc_file, files)

    fields = [
        'file', 'field', 'sum', 'sumsq', 'min', 'max', 'n', 'blank', 'bad'
    ]

    writer = csv.writer(open("stats.csv", 'wb'))
    writer.writerow(fields)
    for answer in answers:
        assert len(answer[0]) == len(fields), (len(answer[0]), len(fields))
        writer.writerows(answer)

if __name__ == '__main__':
    main()


