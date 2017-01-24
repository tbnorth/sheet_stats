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

MatchError = namedtuple("MatchError",
    "survey leg xl_file xl_field db_field stat xl_val db_val")

CLIPBOARD_SQL = True
if CLIPBOARD_SQL:
    from PyQt4 import QtGui, QtCore, Qt
    __app = Qt.QApplication(sys.argv)

QUERIES_DIR = 'queries'
SHEET_STATS = 'd_dba.csv'

LEG_TO_XLSX = {  # map survey legs to XLSX files
    (13, 1): r"d:\large\dba_nearshore_data\GB_Leg1.xlsx",
    (11, (1,9)): r"d:\large\dba_nearshore_data\LE_Aug_Leg%d.xlsx",
    (12, (1,10)): r"d:\large\dba_nearshore_data\LE_Sep_Leg%d.xlsx",
}
"""
select distinct begin_date, end_date, lake_cd, survey_id, leg_loop
  from nearshore.tow
       join nearshore.survey using (survey_id)
 order by lake_cd, begin_date
"""

for k in list(LEG_TO_XLSX):  # expand leg ranges
    if isinstance(k[1], tuple):
        for i in range(k[1][0], k[1][1]+1):
            LEG_TO_XLSX[(k[0], i)] = LEG_TO_XLSX[k] % i
        del LEG_TO_XLSX[k]

XLSX_TO_FIELD = {
    'AvgSmplVol':        'avg_smpl_vol',
    'Accnt/Dcnt':        'accnt/dcnt',
    'BAttn':             ['BAttn_370', 'BAttn_660', 'Battn_370', 'Battn_660'],
    'DDLat':             'DDLat',
    'DDLong':            'DDLong',
    'Depth':             'Depth',
    'Design_km':         'Design_km',
    'Design Km':         'Design_km',
    'Fluor':             'Fluor',
    'SpCond':            'SpCond',
    'Temp':              'Temp',
    'UTC':               'UTC_Time',
    'Zdens':             'Zdens',
    'ZugDW':             ['ZugDW', 'Zug_DW'],  # ZugDW = 10*Zug
    '%Xmiss':            'Xmiss_660',
    'mM/L_NO3':          'NO3',
    'ShpSpd_Cmputd':     'ship_spd',
    'TOF_speed':         'tof_spd',
    'GO_flwmtr_speed':   'flwmtr_spd',
    'SmplVol(TOF_spd)':  'smplvol_tof',
    'SmplVol(Ship_spd)': 'smplvol_shpd',
    'Avg_Smpl_Vol':      'avg_smpl_vol',
    'TOF Speed':         'tof_spd',
}
# add size bins and oversize bins
FIELD_PREC = {}
for um in range(105, 1925, 5):
    name = "%dum" % um
    XLSX_TO_FIELD[name] = name
    FIELD_PREC[name] = 3
for n in range(1, 11):
    XLSX_TO_FIELD["OVR%d" % n] = "Ovr%d_ESD" % n
    FIELD_PREC["Ovr%d_ESD" % n] = 3
unknowns = [
    'Distance',
    'Leg 1 Dist',
    'Leg 1 Dist.',
    'Leg 1 Distance',
    'Leg 2 Dist',
    'Leg 3 Dist.',
    'Leg 4 Dist',
    'Leg 4 Dist.',
    'Leg 5 Dist',
    'Leg 5 Dist.',
    'Leg 6 Dist.',
    'Leg 7 Dist.',
    'Leg 7 Dist',
    'Leg 8 Dist.',
    'Leg 9 Dist.',
    'Leg 10 Dist.',
    'None',
    'SmplVol(GO_spd)',
    'Vlts_NOx',
    'WtrClmn_crrctd',
    'WtrClmn_m',
    'WtrClmn_m_Corrected',
    'WtrClmn_m_correctd',
    'WtrClmn_m_corrected',
    'Zug3#318_net',
    'ZugDW_2#170_net',
    'Zug_2#170_net',
    'etime',
]
for unknown in unknowns:
    assert unknown not in XLSX_TO_FIELD, unknown
    XLSX_TO_FIELD[unknown] = '_NO_CORRESPONDING_MEASURE_'
# turn all entries into a list
XLSX_TO_FIELD = {k:(v if isinstance(v, list) else [v])
                 for k,v in XLSX_TO_FIELD.items()}

FIELD_PREC.update({
    'accnt/dcnt': 2,
    'avg_smpl_vol': 2,
    'BAttn_370': 2,
    'DDLat': 2,
    'DDLong': 2,
    'Depth': 2,
    'Design_km': 2,
    'Fluor': 2,
    'SpCond': 2,
    'Temp': 2,
    'UTC_Time': 2,
    'Zdens': 1,
    'ZugDW': 2,
    'Xmiss_660': 2,
    'NO3': 2,
    'ship_spd': 2,
    'tof_spd': 2,
    'flwmtr_spd': 2,

    'DDLat': 5,
    'DDLong': 5,
    'Depth': 2,
    'Design_km': 2,
    'UTC_Time': 2,
})
# add entries for variants
for variants in XLSX_TO_FIELD.values():
    FIELD_PREC.update({
        k:FIELD_PREC[variants[0]]
        for k in variants[1:]
    })

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

def prec_match(a, b, prec, stat):
    """
    prec_match - check a == b at prec decimal places

    :param float a: first value to check
    :param float b: second value to check
    :param int prec: number of decimal places to match at
    :param str stat: stat (n, mean, etc.) being compared
    :return: bool
    """

    a = float(a)
    b = float(b)

    if stat == 'n':
        return a == b

    return abs(a - b) <= pow(10., -prec)

def main():

    # read sheet stats
    reader = csv.reader(open(SHEET_STATS))
    fields = next(reader)
    # CSV as list of dicts
    stats_in = [{k:v for k,v in zip(fields, row)} for row in reader]
    # reform to file -> field -> stats keyed dicts
    xlstats_all = defaultdict(lambda: dict())
    for stat in stats_in:
        xlstats_all[stat['file']][stat['field']] = stat

    indent = '    '
    match_errors = []

    for survey, leg in LEG_TO_XLSX:

        xl_file = LEG_TO_XLSX[(survey, leg)]

        # FIXME, should check some list for things already QA'ed

        print ("%s%s" % (indent*0, xl_file))

        measures = get_measures(survey, leg)

        dbstats = get_db_stats(survey, leg)

        xlstats = xlstats_all[xl_file]

        # find *one* db field for each xl field
        x2d = {}
        available = list(measures) + EXTRA_FIELDS
        for xl_field in xlstats:
            candidates = XLSX_TO_FIELD.get(xl_field, [])
            if not candidates:
                raise Exception("No candidates for %s" % xl_field)
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
            if db_field == 'Depth':
                a, b = dbstats[db_field]['min'], dbstats[db_field]['max']
                dbstats[db_field]['min'], dbstats[db_field]['max'] = -b, -a
                dbstats[db_field]['mean'] *= -1
            if xlstats[xl_field]['n'] == 1:
                # work around for sheet_stats.py bug
                xlstats[xl_field]['mean'] == xlstats[xl_field]['min']
            for stat in dbstats[db_field]:
                if stat in xlstats[xl_field] and \
                   stat not in SKIP_STATS:
                    a = xlstats[xl_field][stat]
                    b = dbstats[db_field][stat]
                    prec = FIELD_PREC[db_field]
                    text = "%s%s: %s vs %s" % (indent*2, stat, a, b)
                    # neg_b = -b if db_field == 'Depth' else b
                    if not prec_match(a, b, prec, stat):
                        text = 'X'+text[1:]
                        match_errors.append(MatchError(
                            survey, leg, xl_file, xl_field, db_field,
                            stat, a, b
                        ))
                    print(text)

        # things in DB not in Excel
        missed = [i for i in available if i not in x2d.values()]
        # show things missing on either end
        for miss, name in (missing, 'db'), (missed, 'Excel file'):
            if miss:
                missing.sort()
                print("X%sMissing from %s:" % (indent[:-1], name))
                print('\n'.join(textwrap.wrap(
                    ' '.join(miss),
                    initial_indent=indent*2,
                    subsequent_indent=indent*2
                )))
                for i in miss:
                    match_errors.append(MatchError(
                        survey, leg, xl_file,
                        i if name == 'db' else '',
                        i if name != 'db' else '',
                        "missing in other", '', ''
                    ))

    print len(match_errors), "mismatches"
    writer = csv.writer(open("match_errors.csv", 'wb'))
    writer.writerow(MatchError._fields)
    writer.writerows(match_errors)

if __name__ == '__main__':
    main()


