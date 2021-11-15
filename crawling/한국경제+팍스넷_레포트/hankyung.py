from pdftotext import *

hankyung = pd.DataFrame(columns = ['Type', 'Date', 'Title', 'Sub', 'Writer', 'Publish', 'File'])

headers = {
'Host': 'consensus.hankyung.com',
'Connection': 'keep-alive',
'Cache-Control': 'max-age=0',
'Upgrade-Insecure-Requests': '1',
'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/93.0.4577.63 Safari/537.36',
'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
'Referer': 'http://consensus.hankyung.com/apps.analysis/analysis.list',
'Accept-Encoding': 'gzip, deflate',
'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7,nl;q=0.6',
'Cookie': '__gads=ID=f5a863ed700b4520:T=1631066526:S=ALNI_Mad7WRSCfwnX8gtMztqLddH1Pj8Tw; _cc_id=d3261baa3344347b84e99257789ef9f8; panoramaId=0ea0fef749750c998e4a4b928a2f16d539385ff296a095f34b7cd45ab244bf23; panoramaId_expiry=1631671332082; cto_bundle=W5aFNF9aV0NGQTlmOU90ZUFSQkhLR3pmSVozNTAxQnNieTFEOExXcWI1cXhVVmhjRWlBTnVBSk5FU1JWaEdkYURXJTJGVm1URmg5U2x3cGVnMmJMS0VRJTJGZnhuSlJpeSUyQlZ1clR4MUJIZ3EyQjZvNm5IdkZDSDl6dWVUdFkzQkJDRnRSUVk0ViUyRkFmbVkwWTlWWEZ4ZUVUV01nOWl0QSUzRCUzRA; _ga_Y2DVPQKG52=GS1.1.1631066707.1.1.1631066783.0; gtmdlkr=; _gid=GA1.2.1755634602.1631539164; _ga=GA1.2.792891774.1628520499; _ga_SK6NCMLNS5=GS1.1.1631539163.7.1.1631539241.60'
}

for key in tickers.keys():
    
    os.makedirs(key)
    for tk in tqdm(tickers[key]):
        p = 1
        while 1:
            url = f"http://consensus.hankyung.com/apps.analysis/analysis.list?sdate={sdate}&edate={edate}&now_page={p}\
            &search_value=&report_type=&pagenum=20&search_text={tk}&business_code="
            html = requests.get(url, headers=headers).content
            soup = BeautifulSoup(html, 'lxml')

            # 각 페이지
            try:
                for idx in range(1, 21):
                    report = soup.find_all('tr')[idx] # Error : 마지막 페이지 or 한 페이지에 20개 미만
                    date = report.find_all('td', text = True)[0].text
                    writer = report.find_all('td', text = True)[2].text
                    publish = report.find_all('td', text = True)[3].text
                    title = report.find('td', 'text_l').strong.text
                    sub = "\n".join(map(lambda x: x.text, report.find('td', 'text_l').find_all('li')))
                    file = report.find('a', title = True).get('title')
                    link = report.find('a', title = True).get('href') 

                    hankyung.loc[len(hankyung)] = [key, date, title, sub, writer, publish, file]

                    # pdf download
                    resp = download(f"http://consensus.hankyung.com{link}", headers=headers)
                    pdf_path = f"./{key}/{file}"
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
            except Exception:
                break

hankyung.to_csv('./한국경제_crawling.csv', encoding = 'utf8', index=False)