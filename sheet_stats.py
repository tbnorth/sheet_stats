"""
sheet_stats.py - report column stats for spreadsheets

requires openpyxl and numpy

Terry N. Brown, terrynbrown@gmail.com, Fri Dec 16 13:20:47 2016
2016-12-26 Henry Helgen added average, variance, standard deviation,
                        coefficient of variation to output
2016-12-23 Henry Helgen updated to Python 3.5 syntax including print() and
                        writer = csv.writer(open(opt.output, 'w', newline=''))
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

PYTHON_2 = sys.version_info[0] < 3
if not PYTHON_2:
    unicode = str

FIELDS = [  # fields in outout table
    'file', 'field', 'n', 'blank', 'bad', 'min', 'max', 'mean', 'std',
    'sum', 'sumsq', 'variance', 'coefvar'
]
INT_FIELDS = [ 'n', 'blank', 'bad' ]
STR_FIELDS = [ 'file', 'field' ]
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

    requiredNamed = parser.add_argument_group('required named arguments')

    requiredNamed.add_argument("--output", nargs=1,
        help="Path to .csv file for output, will be overwritten",
        metavar='FILE'
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

    if not opt.output:
        print ("No --output supplied")
        exit(10)
    opt.output = opt.output[0]

    return opt

def get_aggregate(psumsqn, psumn, pcountn):
    """
    get_aggregate - compute mean, variance, standard deviation, coefficient of variation
    This function is used instead of numpy.mean, numpy.var, numpy.std since the sum, sumsq, and count
    are available when the function is called. It avoids an extra pass through the list.
    # note pcountn means the full list n,  not a sample n - 1

    :param sum of squares, sum, count
    :return: a tuple of floats mean, variance, standard deviation, coefficient of variation
    """

    Agg = namedtuple("Agg", "mean variance std coefvar")

    # validate inputs check for count == 0
    if pcountn == 0:
        result = Agg(np.nan, np.nan, np.nan, np.nan)
    else:

        mean = psumn / pcountn # mean

        # compute variance from sum squared without knowing mean while summing
        variance = (psumsqn - (psumn * psumn) / pcountn ) / pcountn

        #compute standard deviation
        if variance < 0:
            std = np.nan
        else:
            std = np.sqrt(variance)

        # compute coefficient of variation
        if mean == 0:
            coefvar = np.nan
        else:
            coefvar = std / mean

        result = Agg(mean, variance, std, coefvar)

    return result


def proc_file(filepath):
    """
    proc_file - process one .xlsx file

    :param str filepath: path to file
    :return: list of lists, rows of info. as expected in main()
    """

    print (filepath)

    # get the first sheet
    book = load_workbook(filename=filepath, read_only=True)
    sheets = book.get_sheet_names()
    sheet = book[sheets[0]]
    row_source = sheet.rows
    row0 = next(row_source)
    # get field names from the first row
    fields = [i.value for i in row0]
    cols = len(fields)

    # pre-allocate vectors to store sums / counts
    data = {'_FILEPATH': filepath, 'field': fields}
    for field in FIELDS:
        if field in INT_FIELDS:
            data[field] = np.zeros(cols, dtype=int)
        elif field not in STR_FIELDS:
            data[field] = np.zeros(cols, dtype=float)
    # init. mins/maxs with invalid value for later calc.
    data['min'] += np.nan
    data['max'] += np.nan

    rows = 0
    for row in row_source:

        if rows % 1000 == 0:  # feedback every 1000 rows
            print (rows)
            # Much cleaner to exit by creating a file called "STOP" in the
            # local directory than to try and use Ctrl-C, when using
            # multiprocessing.  Save time by checking only every 1000 rows.
            if os.path.exists("STOP"):
                return

        rows += 1

        for cell_n, cell in enumerate(row):
            if cell.value is None or unicode(cell.value).strip() == '':
                data['blank'][cell_n] +=1
            else:
                try:
                    x = float(cell.value)
                    data['sum'][cell_n] += x
                    data['sumsq'][cell_n] += x*x
                    data['n'][cell_n] += 1
                    # min is x if no value seen yet, else min(prev-min, x)
                    if np.isnan(data['min'][cell_n]):
                        data['min'][cell_n] = x
                    else:
                        data['min'][cell_n] = min(data['min'][cell_n], x)
                    # as for min
                    if np.isnan(data['max'][cell_n]):
                        data['max'][cell_n] = x
                    else:
                        data['max'][cell_n] = max(data['max'][cell_n], x)
                except ValueError:
                    data['bad'][cell_n] += 1

    assert sum(data['n']) + sum(data['blank']) + sum(data['bad']) == rows * len(fields)

    # compute the derived values
    for i in range(len(fields)):
        for k, v in get_aggregate(data['sumsq'][i], data['sum'][i], data['n'][i])._asdict().items():
            data[k][i] = v

    return data
def main():

    opt = get_options()

    # pass filenames through glob() to expand "2017_*.xlsx" etc.
    files = []
    for filepath in opt.files:
        files.extend(glob.glob(filepath))

    # create a pool of processors
    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)

    # process file list with processor pool
    answers = pool.map(proc_file, files)

    # csv.writer does its own EOL handling,
    # see https://docs.python.org/3/library/csv.html#csv.reader
    if PYTHON_2:
        output = open(opt.output, 'wb')
    else:
        output = open(opt.output, 'w', newline='')

    with output as out:
        writer = csv.writer(out)
        writer.writerow(FIELDS)
        for answer in answers:
            for row in answer:
                out = [[answer['_FILEPATH']] + [answer[k][i] for k in FIELDS[1:]]
                       for i in range(len(answer['n']))]
            if PYTHON_2:
                writer.writerows(
                    [unicode(col).encode('utf-8') for col in row]
                    for row in out
                )
            else:
                writer.writerows(out)

if __name__ == '__main__':
    main()

