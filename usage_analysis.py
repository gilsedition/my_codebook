"""Hey mak."""
import os
import shutil
import pandas as pd
from urlparse import urlparse, parse_qs


def hey_mak(datafile):
    """Wow its mak."""
    try:
        data = pd.read_csv(datafile, delimiter='"', compression='gzip')
    except:
        data = pd.DataFrame()

    if len(data):
        # giving readable names to columns
        columns = ['ip', 'time_stamp', 'client-link', 'dashboard_link',
                   'req_type', 'browser_type', 'empty']
        data.columns = columns
        # specifying columns to be dropped
        _drop = ['time_stamp', 'client-link', 'req_type',
                 'browser_type', 'empty']
        data.drop(_drop, axis=1, inplace=True)

        # selecting only mondelez dashboard logs
        data = data[data['dashboard_link'].str.startswith(
            'https://dashboards.nielsen.com/mondelez')]

        # getting the time alone
        data['time'] = data['ip'].apply(
            lambda x: x.split(' - - ')[1].replace(']', '').replace(
                '[', '').replace(' +0000', '').strip())

        data['time'] = pd.to_datetime(data['time'], format='%d/%b/%Y:%H:%M:%S')
        data['date'] = data['time'].apply(lambda x: x.strftime('%d/%b/%Y'))
        data['time'] = data['time'].apply(lambda x: x.strftime('%H:%M:%S'))
        data['ip'] = data['ip'].apply(lambda x: x.split(' - - ')[0])
        data['user_type'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get('ut', [''])[0])
        data['branch_name'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get('branch', [''])[0])
        data['state_name'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get('select_state', [''])[0])
        data['bsm_area'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get('bsm_area', [''])[0])
        data['asm_area'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get('asm_area', [''])[0])
        data['sales_territory'] = data['dashboard_link'].apply(
            lambda x: parse_qs(urlparse(x).query).get(
                'sales_territory', [''])[0])

    return data

data = pd.DataFrame()

# the folder in which it reads all files
for root, _dirs, data_files in os.walk('logs_feb2018'):
    for _file in data_files:
        _data = hey_mak(os.path.join(root, _file))
        if len(_data):
            data = data.append(_data)
# create the folder if it doesn't exists
directory = 'reports/'
if os.path.exists(directory):
    shutil.rmtree(directory)
    os.makedirs(directory)
if not os.path.exists(directory):
    os.makedirs(directory)
data = data[data['user_type'].notnull()]
data.to_csv(os.path.join('reports', 'consolidated_report.csv'), index=False)

data['date'] = pd.to_datetime(data['date'], format='%d/%b/%Y')
data['count'] = 1
data = data.set_index('date')
usages = ['sales_territory', 'user_type', 'state_name', 'branch_name',
          'bsm_area', 'asm_area']

for usage in usages:
    report = data.groupby(usage).resample(
        'M', how={'count': 'sum'}).fillna(0).reset_index()
    report['date'] = report['date'].apply(lambda x: x.strftime('%b-%Y'))
    report.to_csv(
        os.path.join('reports', '{}_report.csv'.format(usage)), index=False)
