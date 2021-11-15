from pdftotext import *

headers={ 
    'Host': 'www.paxnet.co.kr',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.82 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Referer': 'http://www.paxnet.co.kr/stock/report/reportView?menuCode=2222&currentPageNo=1&reportId=132756&searchKey=stock&searchValue=',
    'Accept-Encoding': 'gzip, deflate',
    'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
    'Cookie': 'FBA=163283664230886271; WMONID=5MpxAuP9Taq; _ga=GA1.3.2121813615.1632836643; _gid=GA1.3.298923493.1632992503; __gpi=00000000-0000-0000-0000-000000000000; __igaw__adid=MDAwPTExYWEwODRlLTIxY2QtMTFlYy04NTc2LTAyNDJhYzExMDAwMg==; _pubcid=9cc20c7b-4429-40ff-a605-989ab725a29f; JSESSIONID=03121F2E6A923A7E1C83338B64134FAC; __gads=ID=e98871ef624ba1af-22375635d8ca00a9:T=1632836643:RT=1632992523:S=ALNI_MbMTfoxnsX9QIcZqShvNlWMQjWqrA; wcs_bt=aac9f038ec2a5:1632992895'
}

paxnet = pd.DataFrame(columns = ['Type','TypeName', 'Date', 'Title', 'AppropriatePrice','Investment opinion', 'Offer', 'File'])

for key in tickers.keys():
    for tk in tqdm(tickers[key]):
        p = 1
        rid = 0
        while 1:            
            url = f"http://www.paxnet.co.kr/stock/report/report?menuCode=2222&currentPageNo={p}&reportId={rid}&searchKey=stock&searchValue={tk}"
            html = requests.get(url, headers = headers).content
            soup = BeautifulSoup(html,'lxml')
            if soup.find('div','message-wrap') :
                break
            
            report = soup.find_all('ul')[1]
            type_name = [i.text for i in report.select('strong > a')]
            title = [i.text for i in report.select('p > a')]
         
            #가격
            price = [i.text.replace('적정가격', '').strip() for i in soup.select('ul > li > div:nth-of-type(3)')][1:]
            #투자의견
            opinion = [i.text.replace('투자의견', '').strip() for i in soup.select('ul > li > div:nth-of-type(4)')][1:]
            #제공출처
            offer = [i.text.replace('제공출처', '').strip() for i in soup.select('ul > li > div:nth-of-type(5)')][1:]
            #작성일
            date = [i.text.replace('작성일', '').strip() for i in soup.select('ul > li > div:nth-of-type(6)')][1:]
        
            for i in range(len(title)):
                #pdf 
                rid = re.findall('[0-9]+', report.select('p > a')[i]['href'])[0]
                url =  f"http://www.paxnet.co.kr/stock/report/reportView?menuCode=2222&currentPageNo={p}&reportId={rid}&searchKey=stock&searchValue={tk}"
                report_html = requests.get(url, headers=headers).content
                rsoup = BeautifulSoup(report_html, 'lxml')
                link = rsoup.select('#contents > div.cont-area > div.report-view > div.report-view-file > span > a')
                file = rsoup.select('#contents > div.cont-area > div.report-view > div.report-view-file > span > a')
                try:
                    link = link[0]['href']
                    file = file[0].text.replace("\n","")
                except:
                    continue
                
                paxnet.loc[len(paxnet)] = [key, type_name[i], date[i], title[i], price[i], opinion[i], offer[i], file]
                
                # pdf download
                resp = download(f"{link}", headers=headers)
                pdf_path = f'./{key}/{file}'
                pdf = open(pdf_path, 'wb')
                pdf.write(resp.content)
                pdf.close()

                # pdf to text
                parsed = parser.from_file(pdf_path)
                txt = open(f"{pdf_path.replace('pdf', 'txt')}", 'w', encoding = 'utf8')
                print(parsed['content'].strip(), file = txt)
                txt.close()
                # pdf file delete
                os.remove(pdf_path)
            p += 1

paxnet.to_csv('./팍스넷_crawling.csv', encoding = 'utf8', index=False)