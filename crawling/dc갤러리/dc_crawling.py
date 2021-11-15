import requests
from urllib import request
from bs4 import BeautifulSoup
import time
import pandas as pd
from pymongo import MongoClient


ACODE = {"호텔신라":"008770","두산퓨얼셀":"336260","한화솔루션":"009830",
            "에코프로":"086520","신성이엔지":"011930","유니슨":"018000",
            "카카오":"035720","네이버":"035420","엔씨소프트":"036570",
            "sds":"018260","삼성전자":"005930","SK하이닉스":"000660",
            "skc":"011790","한솔케미칼":"014680","DB하이텍":"000990",
            "롯데쇼핑":"023530","신세계":"004170","삼성물산":"028260",
            "동서":"026960","kb금융":"105560","한국금융지주":"071050","신한지주":"055550",
            "우리금융지주":"316140","하나금융지주":"086790"}

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36'}

client = MongoClient('localhost', 27017)
MongoDB = client['DC']
collection = MongoDB['DC_title_crawl']

#멈출 날짜
DATE_STOP = '2021-09-20'

def title_crawl(ls, ticker):
    for i in range(0,len(ls)-1):
        response = requests.get('https://gall.dcinside.com' + str(ls[i]),headers = HEADERS)
        soup = BeautifulSoup(response.content,'html.parser')
        items = soup.find_all("table",{'class':'gall_list'})
        contents = soup.find('tbody').find_all('tr')
      
        for j in contents:         
            if j.find('td',{'class':'gall_writer ub-writer'}).text=='운영자':
                pass
            else:
                #날짜
                date_dict = j.find('td',{'class':'gall_date'}).attrs

                if date_dict['title'][:10] <= DATE_STOP: 
                    return -1
                else:
                    # Date.append(date_dict['title'])
                    #제목
                    title = j.find('a').text
                    #추천수
                    recommend_tag = j.find('td', class_='gall_recommend')
                    recommend = recommend_tag.text
                    # Rec.append(recommend)

                    #조회수
                    views_tag = j.find('td', class_='gall_count')
                    views = views_tag.text
                    # View.append(views)

                    put_data = {
                        'code': ACODE[ticker],
                        'title' : title,
                        'date' : date_dict['title'],
                        'view' : views,
                        'recommend' : recommend
                    }

                    # Mongodb에 넣는 코드
                    MongoDB.DC_title_crawl.insert_one(put_data)


# keyword 바꾸기
def DC(ticker, page):

    url = f"https://gall.dcinside.com/board/lists?id=neostock&s_type=search_subject_memo&s_keyword={ticker}"

    a = f'/board/lists?id=neostock&s_type=search_subject_memo&s_keyword={ticker}'
    response = requests.get(url,headers=HEADERS)
    soup = BeautifulSoup(response.text,'html.parser')
    items = soup.find('div',{'class':"bottom_paging_box"})

    #리스트 만들기 url
    url_list=[a]
    for i in items.find_all('a'):
        url_url = i['href']
        url_list.append(url_url)

    #  첫페이지
    title_crawl(url_list, ticker)
    
    k = 0
    while k < page :
        response = requests.get('https://gall.dcinside.com' + str(url_list[-1]),headers=HEADERS)
        soup = BeautifulSoup(response.content,'html.parser')
        items = soup.find('div',{'class':"bottom_paging_box"})
        url_list=[str(url_list[-1])]

        for i in items.find_all('a'):
            url_url = i['href']
            url_list.append(url_url)
        url_list.pop(1) 
        
        if title_crawl(url_list, ticker) == -1:
            break
        else:
            k += 1



if __name__ == "__main__":
    for t in ACODE.keys():
        DC(t, 40)
