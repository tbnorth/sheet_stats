"""
tests.py - unit tests for ../sheet_stats.py

Terry N Brown, Brown.TerryN@epa.gov, Sun Jan  1 10:51:59 2017
"""

import csv
import os
import shutil
import sys
import tempfile
import unittest

from contextlib import contextmanager

import numpy as np

from openpyxl import load_workbook

PYTHON_2 = sys.version_info[0] < 3

def get_answers(filepath):
    """get_answers - get answers for field parameters (mean, min, max etc.)
    from a test spreadsheet.  Result looks like:

        {'fieldA': {
            'min': 0.123,
            'max': ... },
         'fieldB': {
            'min': 0.123,
            'max': ... },
         ...
        }

    :param str filepath: path to spreadsheet
    :return: {field: {min/mean/max/etc: value}}
    """

    # get the field name from the first sheet
    book = load_workbook(filename=filepath, read_only=True, data_only=True)
    sheets = book.get_sheet_names()
    sheet = book[sheets[0]]
    row0 = next(sheet.rows)
    fields = [i.value for i in row0]

    # empty dict for each field
    result = {k:{} for k in fields}

    # get the results from the second sheet
    sheet = book[sheets[1]]
    count = 0
    for row in sheet.rows:
        count += 1
        values = [i.value for i in row]
        parameter = values[-1]  # min, mean etc.
        values = values[:-1]
        for field, value in zip(fields, values):
            result[field][parameter] = value

    return result

@contextmanager
def mk_temp_dir():
    """context manager for tempfile.mkdtemp()"""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)
class TestSheetStats(unittest.TestCase):
    """Test(s) for sheet_stats.py"""
    @classmethod
    def setUpClass(cls):
        """Work out file locations"""
        cls.test_file_dir = os.path.dirname(__file__)
        sheet_stats_dir = os.path.dirname(cls.test_file_dir)
        # make sure import works
        if sheet_stats_dir not in sys.path:
            sys.path.append(sheet_stats_dir)

    def test_sheet_stats(self):
        """Test output from sheet_stats.py

        Bad form to test so much in one "unit" test, but sheet_stats.py
        (a) runs on streams of data and (b) uses multiprocessing, so
        easier to do it this way.
        """

        with mk_temp_dir() as temp_dir:

            # build command line and run sheet_stats.main()
            temp_file = os.path.join(temp_dir, "sheet_stats.csv")
            command_line = [
                '--output', temp_file,
                os.path.join(self.test_file_dir, "*.xlsx")
            ]
            sys.argv[1:] = command_line  # bad API
            import sheet_stats
            sheet_stats.main()

            checks = 0
            with open(temp_file) as result:
                # open results, read fields from first line
                reader = csv.reader(result)
                fields = next(reader)
                # iterate through results, reading model answers as needed
                current_file = None
                for row in reader:
                    if row[0] != current_file:  # read new model answers
                        current_file = row[0]
                        results = get_answers(current_file)
                        parameters = list(results[next(iter(results))])
                    # check variance etc. only when blank == bad == 0
                    chk_variance = int(row[fields.index('blank')]) == 0 and \
                                   int(row[fields.index('bad')]) == 0
                    # skip path and field name
                    for parameter, value in zip(fields[2:], row[2:]):
                        field = row[1].decode('utf-8') if PYTHON_2 else row[1]
                        check = parameter in parameters  # skip blank, bad, etc.
                        check = check and (  # Excel include blanks in variance calc.,
                            chk_variance or  # so skip those cases
                            parameter not in ('std', 'variance', 'coefvar')
                        )
                        if check:
                            checks += 1
                            self.assertTrue(
                                np.isclose(results[field][parameter], float(value)),
                                msg="%s %s %s %s" % (
                                    current_file, parameter,
                                    results[field][parameter], value
                                )
                            )
            self.assertEqual(checks, 90, "Expected 90 comparisons")


if __name__ == '__main__':
    unittest.main()
