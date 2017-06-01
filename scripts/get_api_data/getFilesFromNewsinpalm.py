import arrow
import subprocess
from dashboard.config import HOST
from dashboard.config import PASSWORD
from dashboard.config import USERNAME


JOBS_FILES = [
    'ctr_ads_{date}.log',
    'ctr_news_{date}.log',
    'ctr_trending_{date}.log',
    'fetches_news_without_relative_{date}.log',
    'fetches_user_news_{date}.log',
    'fetch_read_by_category_{date}.log',
    'fetch_read_by_newstype_{date}.log',
    'fetch_read_by_tag_{date}.log',
    'fetch_read_by_tag_hour_{date}.log',
    'fetch_read_by_tag_index_{date}.log',
    'fresh_fetch_read_by_tag_{date}.log',
    'fresh_push_success_{date}.log',
    'freshuser_count_{date}.log',
    'hot_fetch_read_by_tag_{date}.log',
    'login_user_total_{date}.log',
    'pull_total_{date}.log',
    'push_ctr_news_{date}.log',
    'push_ctr_news_hour_{date}.log',
    'push_real_ctr_{date}.log',
    'push_success_{date}.log',
    'reads_news_index_{date}.log',
    'reads_news_time_statistics_{date}.log',
    'reads_user_news_{date}.log',
]

TINY_WORK_FILES = [
    '/home/renning/tiny_work/search_keywords_summary/{country}/keywordSearchCountDesc_{date}.log',
    '/home/renning/tiny_work/search_keywords_summary/{country}/push_arrival_click_rate_{date}.log',
    '/home/renning/tiny_work/new_users_ctr/{country}/output/new_users_arrival_click_rate_{date}.log',
]


# get br JOBs logs
dates = arrow.now().replace(days=-2).format('YYYYMMDD')
for date in [dates, ]:

    user = USERNAME
    host = HOST

    for name in JOBS_FILES:
        name = name.format(date=date)

        position = '/home/statistics/result-br/{name}'.format(name=name)
        command = """
            sshpass -p {password} scp {user}@{host}:{position} ../../hive/br/
        """.format(
            user=user,
            host=host,
            password=PASSWORD,
            position=position
        )
        print('getting br ', name)
        subprocess.call(command, shell=True)

dates = arrow.now().replace(days=-2).format('YYYYMMDD')
for date in [dates, ]:

    user = USERNAME
    host = HOST

    for name in TINY_WORK_FILES:
        position = name.format(country='br', date=date)

        command = """
            sshpass -p {password} scp {user}@{host}:{position} ../../hive/br/
        """.format(
            user=user,
            host=host,
            password=PASSWORD,
            position=position
        )
        print('getting br', position)
        subprocess.call(command, shell=True)

# get id JOBs logs
dates = arrow.now().replace(days=-1).format('YYYYMMDD')
for date in [dates, ]:

    user = USERNAME
    host = HOST

    for name in JOBS_FILES:
        name = name.format(date=date)

        position = '/home/statistics/result-id/{name}'.format(name=name)
        command = """
            sshpass -p {password} scp {user}@{host}:{position} ../../hive/id/
        """.format(
            user=user,
            host=host,
            password=PASSWORD,
            position=position
        )
        print('getting id ', name)
        subprocess.call(command, shell=True)

for name in TINY_WORK_FILES:

    user = USERNAME
    host = HOST

    dates = ''
    if name.endswith('keywordSearchCountDesc_{date}.log'):
        dates = arrow.now().replace(days=-1).format('YYYYMMDD')
    else:
        dates = arrow.now().replace(days=-2).format('YYYYMMDD')

    for date in [dates, ]:

        position = name.format(country='id', date=date)

        command = """
            sshpass -p {password} scp {user}@{host}:{position} ../../hive/id/
        """.format(
            user=user,
            host=host,
            password=PASSWORD,
            position=position
        )
        print('getting id ', position)
        subprocess.call(command, shell=True)
