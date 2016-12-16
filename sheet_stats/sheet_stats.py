"""
sheet_stats.py - report column stats for spreadsheets

requires 'openpyxl'

Terry N. Brown, terrynbrown@gmail.com, Fri Dec 16 13:20:47 2016
"""

import argparse
import glob
import os
import sys
from collections import namedtuple, defaultdict

from openpyxl import load_workbook

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

def proc_row(row):
    """proc_row - process a row to floats, or explain why not

    :param spreadsheet row row: row to process
    :return: [floats], [0/1 floats], [0/1 blanks], [0/1 bad]
    :rtype: <|return type|>
    """

    floats = []
    counts = []
    blanks = []
    bad = []
    
    for cell in row:
        if cell.value is None or str(cell.value).strip() == '':
            floats.append(None)
            counts.append(0)
            blanks.append(1)
            bad.append(0)
        else:
            try:
                x = float(cell.value)
                floats.append(x)
                counts.append(1)
                blanks.append(0)
                bad.append(0)
            except ValueError:
                floats.append(None)
                counts.append(0)
                blanks.append(0)
                bad.append(1)
    return floats, counts, blanks, bad
                

def main():

    opt = get_options()

    # pass filenames through glob() to expand "2017_*.xlsx" etc.
    files = []
    for filepath in opt.files:
        files.extend(glob.glob(filepath))

    for filepath in files:
        print filepath
        wb = load_workbook(filename=filepath, read_only=True)
        sheets = wb.get_sheet_names()
        ws = wb[sheets[0]]
        row0 = next(ws.rows)
        fields = [i.value for i in row0]
        zeros = lambda: [0]*len(fields)
        
        n = zeros()
        sums = zeros()
        sumsq = zeros()
        blanks = zeros()
        nonfloat = zeros()
        rows = 0

        for row in ws.rows:
            rows += 1
            if rows % 1000 == 0:
                print rows
            x, floats, blank, bad = proc_row(row)
            if rows > 3000:
                break

if __name__ == '__main__':
    main()
