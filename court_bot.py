# -*- coding: utf-8 -*-
"""part2. Парсер судебных дел РФ с сайта sudrf.ru
"""

# pip install grab
# pip install psycopg2-binary

import csv
import logging
import itertools
import argparse

from grab import Grab, proxylist
from grab.spider import Spider, Task


_DB_NAME = 'd9gh4k8vaf70js'
_DSN = (
    'postgres://fivostozwliibo:e64c8130b9e976c592ffdbaf605e0433973f4250cd9a098196a5116c2b8b45e4'
    '@ec2-52-208-175-161.eu-west-1.compute.amazonaws.com:5432')

_COURTS_URL = (
    'https://sudrf.ru/index.php?id=300'
    '&act=go_ms_search&searchtype=ms&var=true&ms_type=ms'
    '&court_subj={region}')

_CASES_URL = (
    '{domain}/modules.php?name=sud_delo'
    '&{delo_table}_DOCUMENT__RESULT_DATE1D={start_date}'
    '&{delo_table}_DOCUMENT__RESULT_DATE2D={end_date}'
    '&op=rd'
    '&delo_table={delo_table}_DOCUMENT'
    '&delo_id={delo_id}')

_BASE_CONFIG = {'start_date': '01.01.{}', 'end_date': '31.12.{}', 'region': '{}'}

_PROXY_URL = (
    'https://www.proxyscan.io/api/proxy'
    '?last_check=6400'
    '&uptime=50'
    '&ping=500'
    '&limit=100'
    '&type={}'
    '&format=txt')

logging.basicConfig(
    datefmt='%:H%M:%S',
    filemode='a',
    filename='courts.log',
    level=logging.INFO)

parser = argparse.ArgumentParser()
parser.add_argument(
    '-v', '--verbose',
    type=str,
    default='INFO',
    help='Logging level, for example: DEBUG or WARNING.')
parser.add_argument(
    '-r', '--region',
    type=str,
    required=True,
    help='Provides region number to parse.')
parser.add_argument(
    '-y', '--year',
    type=int,
    dest='years',
    action='append',
    required=True,
    help='Provides year to parse.')

parser.add_argument(
    '-p', '--proxytype',
    type=str,
    default='http',
    choices=['http', 'socks4', 'socks5'],
    help='Set of proxy type.')


class CourtSpider(Spider):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.counter = 0
        self.cases_file = None
        self.case_detail_file = None

    @property
    def _region(self):
        return self.config.get('region', 35)

    @property
    def _court_limit(self):
        return self.config.get('court_limit', 0)

    @property
    def _start_date(self):
        return self.config.get('start_date', '01.01.2020')

    @property
    def _end_date(self):
        return self.config.get('end_date', '01.10.2020')

    @property
    def _case_types(self):
        return [
            (1540005, 'G1'),  # гражданские
            (1540006, 'U1'),  # уголовные
            (1500001, 'ADM')  # административные
        ]

    def prepare(self):
        self._case_detail_file = open('case_detail.csv', 'w')
        self.case_detail_file = csv.writer(self._case_detail_file)

        self._cases_file = open(
            'cases_{}_{}.csv'.format(self._region, self._start_date), 'w')
        self.cases_file = csv.writer(self._cases_file)

    def shutdown(self):
        self._cases_file.close()
        self._case_detail_file.close()

    def task_generator(self):
        url = _COURTS_URL.format(region=self._region)
        yield Task('courts', url)

    def task_courts(self, grab, _):
        links = grab.doc.select('//div[@class="courtInfoCont"]/div/a')
        limit = self._court_limit or len(links)
        for link in links[:limit]:
            link = link.text()
            court_id, _ = link.strip('http://').split('.', 1)
            for delo_id, delo_table in self._case_types:
                task_kwargs = {
                    'domain': link,
                    'delo_id': delo_id,
                    'court_id': court_id,
                    'delo_table': delo_table}
                url = _CASES_URL.format(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    **task_kwargs)
                yield Task('first_page', url, **task_kwargs)
        logging.debug('Done courts task[region:%s]', self._region)

    def task_first_page(self, grab, task):
        self._write_cases(grab.doc, task)
        pages = grab.doc.select(
            '//ul[@class="paging"]/li[last()]/a[text()]').text_list()
        num_pages = int(pages[0]) if pages else 1
        task_kwargs = {
            'num_pages': num_pages,
            'court_id': task.court_id,
            'delo_table': task.delo_table,
            'domain': task.domain}
        for page in range(1, num_pages):
            task_kwargs['page'] = page
            url = '{}&pageNum_Recordset1={}'.format(task.url, page)
            yield Task('cases', url, **task_kwargs)
        msg = (
            'Done first page[region:{}][court:{}][type:{}][total_pages:{}]'
        ).format(self._region, task.court_id, task.delo_table, num_pages)
        logging.debug(msg)

    def task_cases(self, grab, task):
        self._write_cases(grab.doc, task)
        msg = ('Done cases[region:{}][court:{}][type:{}][page:{}/{}]').format(
            self._region, task.court_id, task.delo_table, task.page, task.num_pages)
        logging.debug(msg)

    def task_case_detail(self, _, task):
        act = ''
        solution = ''
        case_id = task.url
        self.case_detail_file.writerow([case_id, solution, act])
        logging.debug('Done case detail[%s]', task.number)

    def _write_cases(self, doc, task):
        rows = []
        nodes = doc.select('//table[@id="tablcont"]//tr').node_list()
        for node in nodes:
            number, text, judge, category, date = node.getchildren()
            case_link = number.xpath('a/@href')[0] if number.xpath('a/@href') else None
            solution_link = text.xpath('a/@href')[0] if text.xpath('a/@href') else None
            if case_link or solution_link:
                rows.append([
                    self._region,
                    number.text_content(),
                    judge.text_content(),
                    category.text_content(),
                    date.text_content(),
                    task.delo_table,
                    task.court_id,
                    task.domain,
                    case_link,
                    solution_link,
                ])
                self.counter += 1
        self.cases_file.writerows(rows)
        logging.debug('Saved cases: [%s]', len(rows))


if __name__ == '__main__':
    args = parser.parse_args()
    proxytype = args.proxytype
    proxyurl = _PROXY_URL.format(
            'http,https' if proxytype == 'http' else proxytype)
    logging.root.setLevel(args.verbose)
    for region, year in itertools.product([args.region], args.years):
        config = _BASE_CONFIG.copy()
        config['start_date'] = config['start_date'].format(year)
        config['end_date'] = config['end_date'].format(year)
        config['region'] = config['region'].format(region)

        bot = CourtSpider(network_try_limit=5, thread_number=25, config=config)
        logging.info('Bot initialzed with config: %s', bot.config)
        bot.setup_cache('postgresql', 'crm', dsn='postgres://zaebee@')
        bot.load_proxylist(proxyurl, 'url', proxytype)

        bot.run()

        logging.info(bot.render_stats())
        message = (
            'Total cases[region:{region}][dates:{start_date}-{end_date}]: '
            '{total}').format(total=bot.counter, **config)
        logging.info(message)
