"""
sheet_stats.py - report column stats for spreadsheets

requires openpyxl and numpy

Terry N. Brown, terrynbrown@gmail.com, Fri Dec 16 13:20:47 2016
2016-12-26 Henry Helgen added average to output 
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
    :return: a tuple of floats   average, variance, standard deviation, coefficient of variation
    """
    # validate inputs check for count == 0
    if pcountn == 0:
        avg, var, std, coefvar = np.nan, np.nan, np.nan, np.nan
    else:
        
        avg = psumn / pcountn # average

        # compute variance from sum squared without knowing mean while summing
        var = (psumsqn - (psumn * psumn) / pcountn ) / pcountn # variance

        #compute standard deviation
        if var < 0:
            std = np.nan
        else:
            std = np.sqrt(var) 

        # compute coefficient of variation
        if avg == 0:
            coefvar = np.nan
        else:
            coefvar = std / avg
        
        
    return avg, var, std, coefvar


def proc_file(filepath):
    """
    proc_file - process one .xlsx file

    :param str filepath: path to file
    :return: list of lists, rows of info. as expected in main()
    """

    print (filepath)

    # get the first sheet
    wb = load_workbook(filename=filepath, read_only=True)
    sheets = wb.get_sheet_names()
    ws = wb[sheets[0]]
    row0 = next(ws.rows)
    # get field names from the first row
    fields = [i.value for i in row0]

    cols = len(fields)

    # pre-allocate vectors to store sums / counts
    n = np.zeros(cols, dtype=int) #count
    sums = np.zeros(cols) #sum
    sumssq = np.zeros(cols) #sum of squares
    blank = np.zeros(cols, dtype=int) #count of blank cells
    bad = np.zeros(cols, dtype=int) #count of non-numeric cells
    # init. mins/maxs with invalid value for later calc.
    mins = np.zeros(cols) + np.nan
    maxs = np.zeros(cols) + np.nan

    rows = 0

    for row in ws.rows:

        if rows % 1000 == 0:  # feedback every 1000 rows
            print (rows)
            # Much cleaner to exit by creating a file called "STOP" in the
            # local directory than to try and use Ctrl-C, when using
            # multiprocessing.  Save time by checking only every 1000 rows.
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
                    # min is x if no value seen yet, else min(prev-min, x)
                    if np.isnan(mins[cell_n]):
                        mins[cell_n] = x
                    else:
                        mins[cell_n] = min(mins[cell_n], x)
                    # as for min
                    if np.isnan(maxs[cell_n]):
                        maxs[cell_n] = x
                    else:
                        maxs[cell_n] = max(maxs[cell_n], x)
                except ValueError:
                    bad[cell_n] += 1

    assert sum(n) + sum(blank) + sum(bad) == rows * len(fields)


    # stddev   =  sqrt(variance)

    # rearrange vectors into table form
    # compute the derived values
    ans = []
    for i in range(len(fields)):
        avg, var, std, coefvar = get_aggregate(sumssq[i], sums[i], n[i])
        ans.append([
            filepath, fields[i], avg, sums[i], sumssq[i],
            mins[i], maxs[i], n[i], var, std, coefvar, blank[i], bad[i]
        ])
    return ans
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

    fields = [
        'file', 'field', 'average', 'sum', 'sumsq', 'min', 'max', 'n', 'variance', 'std', 'coefvar', 'blank', 'bad'
    ]

    # dump results, file open mode 'wb' (write binary) to avoid blank lines
    # when Excel reads .csv
    # wb confuses Python 2.7 versus Python 3.5 TypeError: a bytes-like object is required, not 'str'
    # changed back to 'w'. added newline='' to avoid blank lines
    writer = csv.writer(open(opt.output, 'w', newline=''))
    writer.writerow(fields)
    for answer in answers:
        assert len(answer[0]) == len(fields), (len(answer[0]), len(fields))
        writer.writerows(answer)

if __name__ == '__main__':
    main()

