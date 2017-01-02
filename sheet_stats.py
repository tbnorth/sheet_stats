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
from collections import namedtuple
from math import sqrt, isnan
NAN = float('NAN')

from openpyxl import load_workbook

PYTHON_2 = sys.version_info[0] < 3
if not PYTHON_2:
    unicode = str

class AttrDict(dict):
    """allow d.attr instead of d['attr']
    http://stackoverflow.com/a/14620633
    """
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

FIELDS = [  # fields in outout table
    'file', 'field', 'n', 'blank', 'bad', 'min', 'max', 'mean', 'std',
    'sum', 'sumsq', 'variance', 'coefvar'
]
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

    required_named = parser.add_argument_group('required named arguments')

    required_named.add_argument("--output",
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
        print("No --output supplied")
        exit(10)

    return opt

def get_aggregate(psumsqn, psumn, pcountn):
    """
    get_aggregate - compute mean, variance, standard deviation,
    coefficient of variation This function is used instead of
    numpy.mean, numpy.var, numpy.std since the sum, sumsq, and count are
    available when the function is called. It avoids an extra pass
    through the list.

    # note pcountn means the full list n,  not a sample n - 1

    :param sum of squares, sum, count
    :return: a tuple of floats mean, variance, standard deviation, coefficient of variation
    """

    Agg = namedtuple("Agg", "mean variance std coefvar")

    # validate inputs check for count == 0
    if pcountn == 0:
        result = Agg(NAN, NAN, NAN, NAN)
    else:

        mean = psumn / pcountn # mean

        # compute variance from sum squared without knowing mean while summing
        variance = (psumsqn - (psumn * psumn) / pcountn ) / pcountn

        #compute standard deviation
        if variance < 0:
            std = NAN
        else:
            std = sqrt(variance)

        # compute coefficient of variation
        if mean == 0:
            coefvar = NAN
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

    print(filepath)

    # get the first sheet
    book = load_workbook(filename=filepath, read_only=True)
    sheets = book.get_sheet_names()
    sheet = book[sheets[0]]
    row_source = sheet.rows
    row0 = next(row_source)
    # get field names from the first row
    fields = [i.value for i in row0]

    data = {
        'filepath': filepath,
        'fields': {field:AttrDict({f:0 for f in FIELDS}) for field in fields}
    }

    for field in fields:
        # init. mins/maxs with invalid value for later calc.
        data['fields'][field].update(dict(
            min=NAN,
            max=NAN,
            field=field,
            file=filepath,
        ))

    rows = 0
    for row in row_source:

        if rows % 1000 == 0:  # feedback every 1000 rows
            print(rows)
            # Much cleaner to exit by creating a file called "STOP" in the
            # local directory than to try and use Ctrl-C, when using
            # multiprocessing.  Save time by checking only every 1000 rows.
            if os.path.exists("STOP"):
                return

        rows += 1

        for cell_n, cell in enumerate(row):
            d = data['fields'][fields[cell_n]]
            if cell.value is None or unicode(cell.value).strip() == '':
                d.blank += 1
            else:
                try:
                    x = float(cell.value)
                    d.sum += x
                    d.sumsq += x*x
                    d.n += 1
                    # min is x if no value seen yet, else min(prev-min, x)
                    if isnan(d.min):
                        d.min = x
                    else:
                        d.min = min(d.min, x)
                    # as for min
                    if isnan(d.max):
                        d.max = x
                    else:
                        d.max = max(d.max, x)
                except ValueError:
                    d.bad += 1

    assert sum(d.n+d.blank+d.bad for d in data['fields'].values()) == rows * len(fields)

    # compute the derived values
    for field in data['fields']:
        d = data['fields'][field]
        d.update(get_aggregate(d.sumsq, d.sum, d.n)._asdict().items())

    return data
def get_answers(opt):
    """get_answers - process files

    :param argparse.Namespace opt: options
    :return: list of answers from proc_file
    """

    # pass filenames through glob() to expand "2017_*.xlsx" etc.
    files = []
    for filepath in opt.files:
        files.extend(glob.glob(filepath))

    # create a pool of processors
    pool = multiprocessing.Pool(multiprocessing.cpu_count()-1)

    # process file list with processor pool
    return pool.map(proc_file, files)
def get_table_rows(answers):
    """get_table_rows - generator - convert get_answers() output to table format

    :param list answers: output from get_answers()
    :return: list of rows suitable for csv.writer
    """
    yield FIELDS
    for answer in answers:
        for field in answer['fields']:
            row = [answer['fields'][field][k] for k in FIELDS]
            if PYTHON_2:
                yield [unicode(col).encode('utf-8') for col in row]
            else:
                yield row

def main():
    """main() - when invoked directly"""
    opt = get_options()

    # csv.writer does its own EOL handling,
    # see https://docs.python.org/3/library/csv.html#csv.reader
    if PYTHON_2:
        output = open(opt.output, 'wb')
    else:
        output = open(opt.output, 'w', newline='')

    with output as out:
        writer = csv.writer(out)
        for row in get_table_rows(get_answers(opt)):
            writer.writerow(row)

if __name__ == '__main__':
    main()

