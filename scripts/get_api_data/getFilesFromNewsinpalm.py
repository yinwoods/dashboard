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
    ('/home/renning/tiny_work/search_keywords_summary/'
     '{country}/keywordSearchCountDesc_{date}.log'),
    ('/home/renning/tiny_work/search_keywords_summary/'
     '{country}/push_arrival_click_rate_{date}.log'),
    ('/home/renning/tiny_work/new_users_ctr/'
     '{country}/output/new_users_arrival_click_rate_{date}.log'),
]


def get_data(country, jobs_delay, tiny_work_delay):
    # get JOBs logs
    dates = arrow.now().replace(days=jobs_delay).format('YYYYMMDD')

    user = USERNAME
    host = HOST

    for name in JOBS_FILES:
        name = name.format(date=dates)

        name = '/home/statistics/result-{country}/{name}'.format(
                    name=name, country=country)
        command = ('sshpass -p {password} scp {user}@{host}:{name} '
                   '../../hive/{country}/').format(
                        user=user,
                        host=host,
                        country=country,
                        password=PASSWORD,
                        name=name
                    )
        print('getting {country} log from {name}'.format(
                country=country, name=name))
        subprocess.call(command, shell=True)

    # get tiny_work logs
    for name in TINY_WORK_FILES:
        # 印尼、中东的keywordsearch延迟一天
        if (country == 'id' or country == 'me') and\
                'keywordSearchCountDesc_' in str(name):

            dates = arrow.now().replace(
                    days=tiny_work_delay + 1).format('YYYYMMDD')

        else:
            dates = arrow.now().replace(
                    days=tiny_work_delay).format('YYYYMMDD')

        name = name.format(country=country, date=dates)

        command = ('sshpass -p {password} scp {user}@{host}:{name} '
                   '../../hive/{country}/').format(
                        user=user,
                        host=host,
                        password=PASSWORD,
                        name=name,
                        country=country,
                    )
        print('getting {country} log from {name}'.format(
                country=country, name=name))
        subprocess.call(command, shell=True)


if __name__ == '__main__':
    get_data('br', -2, -2)
    get_data('id', -1, -2)
    get_data('me', -1, -2)
