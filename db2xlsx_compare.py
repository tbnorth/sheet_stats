"""
db2xlsx_compare.py - compare DB data to XLS files.

Terry N. Brown, Brown.TerryN@epa.gov, Tue Jan 03 14:49:44 2017
"""

import csv
import json
import os
import sys
import textwrap
import time
from collections import namedtuple, defaultdict
from hashlib import sha1

CLIPBOARD_SQL = True
if CLIPBOARD_SQL:
    from PyQt4 import QtGui, QtCore, Qt
    __app = Qt.QApplication(sys.argv)

QUERIES_DIR = 'queries'
SHEET_STATS = 'l_priv_dba.csv'

LEG_TO_XLSX = {  # map survey legs to XLSX files
    (13, 1): r"L:\Priv\DBA\nearshore\Data\GB_Leg1.xlsx",
}

XLSX_TO_FIELD = {
    'Accnt/Dcnt':      'accnt/dcnt',
    'BAttn':           ['BAttn_370', 'BAttn_660', 'Battn_370', 'Battn_660'],
    'DDLat':           'DDLat',
    'DDLong':          'DDLong',
    'Depth':           'Depth',
    'Design_km':       'Design_km',
    'Fluor':           'Fluor',
    'SpCond':          'SpCond',
    'Temp':            'Temp',
    'UTC':             'UTC_Time',
    'Zdens':           'Zdens',
    'ZugDW':           ['ZugDW', 'Zug_DW'],
    '%Xmiss':          'Xmiss_660',
    'mM/L_NO3':        'NO3',
    'ShpSpd_Cmputd':   'ship_spd',
    'TOF_speed':       'tof_spd',
    'GO_flwmtr_speed': 'flwmtr_spd',
}
# add size bins and oversize bins
for um in range(105, 1925, 5):
    name = "%dum" % um
    XLSX_TO_FIELD[name] = name
for n in range(1, 11):
    XLSX_TO_FIELD["OVR%d" % n] = "Ovr%d_ESD" % n
# turn all entries into a list
XLSX_TO_FIELD = {k:(v if isinstance(v, list) else [v])
                 for k,v in XLSX_TO_FIELD.items()}

# compare 'n', 'mean' etc., but not these:
SKIP_STATS = [
    'field',
]

EXTRA_FIELDS = [
    'DDLat', 'DDLong', 'Depth', 'Design_km', 'UTC_Time',
]

def run_query(sql):
    """run_query - request running of SQL, cache results

    :param str sql: SQL to execute
    :return: Oracle JSON export structure
    """

    clipboard = Qt.QApplication.clipboard()

    if not os.path.exists(QUERIES_DIR):
        os.mkdir(QUERIES_DIR)
    sql_hash = sha1(sql).hexdigest()
    json_path = os.path.join(QUERIES_DIR, sql_hash+'.json')
    json_path = os.path.abspath(json_path)
    open(r"d:\scratch\delete\sql.sql", 'w').write(sql)
    if not os.path.exists(json_path):
        sql_path = os.path.join(QUERIES_DIR, sql_hash+'.sql')
        open(sql_path, 'w').write(sql)
        if CLIPBOARD_SQL:
            Qt.QApplication.processEvents()
            print("Execute SQL on clipboard, then copy JSON output")
            clipboard.setText(sql)
            json_txt = sql
            while json_txt == sql:
                Qt.QApplication.processEvents()
                json_txt = str(clipboard.text())
                time.sleep(0.5)
            json_txt = str(clipboard.text())
            json.loads(json_txt)  # check it loads
            open(json_path, 'wb').write(json_txt)
        else:
            print("\n\n%s\n\n" % sql)
            print("Execute SQL and save as '%s'" % json_path)
            print("Press return to continue")
            raw_input()
    return json.load(open(json_path))

def get_measures(survey, leg):
    """get_measures - get measures in DB for a leg

    Checks that for measures with non-unique names, only one
    is used within a leg (doesn't mean it's the right one, but
    the case where more than one is present is not handled).

    :param int survey: survery id
    :param int leg: leg id
    :return: dict of dicts
    """

    sql = """
with measures as (
select distinct {survey} as survey, {leg} as leg,
       measure_name, measure_id, sort_order
  from nearshore.survey
       join nearshore.tow using (survey_id)
       join nearshore.tow_measurement using (tow_id)
       join nearshore.measurement using (measure_id)
 where survey_id = {survey} and leg_loop = {leg}
)
select /*json*/ survey, leg, measure_name, sort_order,
       count(*) as n
  from measures
 group by survey, leg, measure_name, sort_order
;""".format(survey=survey, leg=leg)

    measures = run_query(sql)

    # first, check only one of each measure in this leg
    for measure in measures['items']:
        if measure['n'] != 1:
            raise Exception()

    # now index by name
    return {i['measure_name']:i for i in measures['items']}

def get_db_stats(survey, leg):
    """get_db_stats - get field stats from the DB

    :param int survey: survery id
    :param int leg: leg id
    :return: dict of dicts
    """

    sql = """
select /*json*/ {survey} as survey, {leg} as leg,
       measure_name as field,
       count(*) as n, avg(measure_value) as mean,
       min(measure_value) as min, max(measure_value) as max
  from nearshore.survey
       join nearshore.tow using (survey_id)
       join nearshore.tow_measurement using (tow_id)
       join nearshore.measurement using (measure_id)
 where survey_id = {survey} and
       leg_loop = {leg}
 group by measure_name
"""

    for extra in EXTRA_FIELDS:
        sql += """
union all
select /*json*/ {{survey}} as survey, {{leg}} as leg,
       '{extra}' as field,
       count(*) as n, avg({extra}) as mean,
       min({extra}) as min, max({extra}) as max
  from nearshore.survey
       join nearshore.tow using (survey_id)
 where survey_id = {{survey}} and
       leg_loop = {{leg}}
""".format(extra=extra)

    sql = sql.format(survey=survey, leg=leg)

    return {i['field']:i for i in run_query(sql)['items']}

def main():

    # read sheet stats
    reader = csv.reader(open(SHEET_STATS))
    fields = next(reader)
    # CSV as list of dicts
    stats_in = [{k:v for k,v in zip(fields, row)} for row in reader]
    # reform to file -> field -> stats keyed dicts
    xlstats = defaultdict(lambda: dict())
    for stat in stats_in:
        xlstats[stat['file']][stat['field']] = stat

    indent = '    '

    for survey, leg in LEG_TO_XLSX:

        xl_file = LEG_TO_XLSX[(survey, leg)]

        # FIXME, should check some list for things already QA'ed

        print ("%s%s" % (indent*0, xl_file))

        measures = get_measures(survey, leg)

        dbstats = get_db_stats(survey, leg)

        xlstats = xlstats[xl_file]

        # find *one* db field for each xl field
        x2d = {}
        available = list(measures) + EXTRA_FIELDS
        for xl_field in xlstats:
            candidates = XLSX_TO_FIELD.get(xl_field, [])
            present = [i for i in candidates if i in available]
            if len(present) > 1:
                raise Exception()
            elif len(present) == 1:
                x2d[xl_field] = present[0]

        missing = []

        # pre-pass to get sort order
        ordered = []
        for xl_field in xlstats:
            db_field = x2d.get(xl_field)
            if db_field is not None and db_field in measures:
                ordered.append((measures[db_field]['sort_order'], xl_field))
            else:
                ordered.append((-1, xl_field))
        ordered.sort()

        for xl_field in [i[1] for i in ordered]:
            db_field = x2d.get(xl_field)
            if db_field is None:
                missing.append(xl_field)
                continue
            print("%s %s -> %s:" % (indent*1, xl_field, db_field))
            for stat in dbstats[db_field]:
                if stat in xlstats[xl_field] and \
                   stat not in SKIP_STATS:
                    print "%s%s: %s vs %s" % (
                        indent*2, stat,
                        xlstats[xl_field][stat], dbstats[db_field][stat])

        # things in DB not in Excel
        missed = [i for i in available if i not in x2d.values()]
        # show things missing on either end
        for miss, name in (missing, 'db'), (missed, 'Excel file'):
            if miss:
                missing.sort()
                print("%sMissing from %s:" % (indent*1, name))
                print('\n'.join(textwrap.wrap(
                    ' '.join(miss),
                    initial_indent=indent*2,
                    subsequent_indent=indent*2
                )))

if __name__ == '__main__':
    main()


