"""
db2xlsx_compare.py - compare DB data to XLS files.

Terry N. Brown, Brown.TerryN@epa.gov, Tue Jan 03 14:49:44 2017
"""

import csv
import json
import os
import sys
import textwrap
from collections import namedtuple, defaultdict
from hashlib import sha1

QUERIES_DIR = 'queries'
SHEET_STATS = 'l_priv_dba.csv'

LEG_TO_XLSX = {  # map survey legs to XLSX files
    (13, 1): r"L:\Priv\DBA\nearshore\Data\GB_Leg1.xlsx",
}

DB_FIELDS = [
    'SpCond', 'Fluor',
]

XLSX_TO_FIELD = {
    'SpCond': 'SpCond',
    'Fluor': 'Fluor',
}

# compare 'n', 'mean' etc., but not these:
SKIP_FIELDS = [
    'field',
]

def run_query(sql):
    """run_query - request running of SQL, cache results

    :param str sql: SQL to execute
    :return: Oracle JSON export structure
    """

    if not os.path.exists(QUERIES_DIR):
        os.mkdir(QUERIES_DIR)
    sql_hash = sha1(sql).hexdigest()
    json_path = os.path.join(QUERIES_DIR, sql_hash+'.json')
    json_path = os.path.abspath(json_path)
    if not os.path.exists(json_path):
        print("\n\n%s\n\n" % sql)
        print("Execute SQL and save as '%s'" % json_path)
        print("Press return to continue")
        raw_input()
    return json.load(open(json_path))

def main():

    # read sheet stats
    reader = csv.reader(open(SHEET_STATS))
    fields = next(reader)
    # CSV as list of dicts
    stats_in = [{k:v for k,v in zip(fields, row)} for row in reader]
    # reform to file -> field -> stats keyed dicts
    stats = defaultdict(lambda: dict())
    for stat in stats_in:
        stats[stat['file']][stat['field']] = stat

    indent = '    '

    for survey, leg in LEG_TO_XLSX:
        # FIXME, should check some list for things already QA'ed

        file_ = LEG_TO_XLSX[survey, leg]
        print ("%s%s" % (indent*0, file_))

        # first, check only one of each measure in this leg
        sql = """
with measures as (
select distinct {survey} as survey, {leg} as leg, measure_name, measure_id
  from nearshore.survey
       join nearshore.tow using (survey_id)
       join nearshore.tow_measurement using (tow_id)
       join nearshore.measurement using (measure_id)
 where survey_id = 13 and
       leg_loop = 1 and
       measure_name in ({fields})
)
select /*json*/ survey, leg, measure_name, count(*) as n from measures
 group by survey, leg, measure_name
;""".format(survey=survey, leg=leg, fields=','.join("'%s'" % i for i in DB_FIELDS))

        measures = run_query(sql)

        for measure in measures['items']:
            if measure['n'] != 1:
                raise Exception()

        fields = [i['measure_name'] for i in measures['items']]

        # now get field stats
        sql = """
select /*json*/ distinct {survey} as survey, {leg} as leg, measure_name as field,
       count(*) as n, avg(measure_value) as mean,
       min(measure_value) as min, max(measure_value) as max
  from nearshore.survey
       join nearshore.tow using (survey_id)
       join nearshore.tow_measurement using (tow_id)
       join nearshore.measurement using (measure_id)
 where survey_id = 13 and
       leg_loop = 1 and
       measure_name in ({fields})
 group by measure_name
;""".format(survey=survey, leg=leg, fields=','.join("'%s'" % i for i in DB_FIELDS))

        dbstats = run_query(sql)
        dbstats = {i['field']:i for i in dbstats['items']}

        xl_file = LEG_TO_XLSX[(survey, leg)]
        missing = []
        for xl_field in stats[xl_file]:
            db_field = XLSX_TO_FIELD.get(xl_field)
            if db_field is None:
                missing.append(xl_field)
                continue
            print("%s'%s':" % (indent*1, xl_field))
            for stat in dbstats[db_field]:
                if stat in SKIP_FIELDS:
                    continue
                if stat in stats[xl_file][xl_field]:
                    print "%s%s: %s vs %s" % (
                        indent*2, stat,
                        dbstats[db_field][stat], stats[xl_file][xl_field][stat])
        if missing:
            missing.sort()
            print("%sMissing from db:" % indent*1)
            print('\n'.join(textwrap.wrap(
                ' '.join(missing),
                initial_indent=indent*2,
                subsequent_indent=indent*2
            )))


if __name__ == '__main__':
    main()
