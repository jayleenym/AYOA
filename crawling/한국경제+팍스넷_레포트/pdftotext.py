import requests
from requests.compat import urlparse, urljoin
from requests.exceptions import HTTPError
from requests import Session
from bs4 import BeautifulSoup
from tika import parser

import time
import datetime
import pandas as pd
import tika
from tqdm import tqdm
import re
import os
os.chdir('./crawling/한국경제+팍스넷_레포트/')

tickers = {
    '친환경' : ['336260', '009830', '086520', '011930', '018000'],
    '서비스업' : ['035720', '035420', '036570', '041140', '018260'],
    '반도체' : ['005930', '000660', '011790', '014680', '000990'],
    '유통업' : ['023530', '004170', '008770', '028260', '026960'],
    '금융' : ['105560', '071050', '055550', '316140', '086790'],
    '메타버스' : ['047080', '089890', '030350', '230980', '069410'],
}
sdate = '2021-08-19'
edate = '2021-11-15'

def download(url, params={}, headers={}, method='GET', limit=3):
    try:
        session = Session()
        resp = session.request(method, url,
                               params=params if method.upper() == 'GET' else '',
                               data=params if method.upper() == 'POST' else '',
                               headers=headers)
        resp.raise_for_status()
    except HTTPError as e:
        if limit > 0 and e.response.status_code >= 500:
            print(limit)
            time.sleep(60)  # Server Error이기 때문에 delay를 두고 실행하기 위해서 사용한다.
            # 보통은 5분에 한 번꼴로 random하게 되도록 설정한다.
            download(url, params, headers, method, limit - 1)
        else:
            print('[{}] '.format(e.response.status_code) + url)
            print(e.response.reason)
            print(e.response.headers)

    return resp

def pdf_to_text(path, filename):
    parsed = parser.from_file(path)
    txt = open(f"{path.replace('pdf', 'txt')}", 'w', encoding = 'utf8')
    print(parsed['content'].strip(), file = txt)
    txt.close()
    # pdf file delete
    os.remove(filename)