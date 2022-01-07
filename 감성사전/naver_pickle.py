import pandas as pd
import json
from pororo import Pororo
import os
from tqdm import tqdm

s_nm = Pororo(task = 'sentiment',model = 'brainbert.base.ko.nsmc', lang = 'ko')
s_sh = Pororo(task = 'sentiment', model = 'brainbert.base.ko.shopping', lang = 'ko')

for f in os.listdir('Naver종토방')[1:]:
  NAVER = pd.DataFrame(columns = ['_id', 'code', 'date', 'title', 'views', 'pos', 'neg', 'nsmc', 'shop'])
  if f.endswith('json'):
    print(f)
    for line in tqdm(open('Naver종토방/'+ f, 'r', encoding = 'utf8').readlines()):
      j = json.loads(line)
      nm_sa = s_nm(j['title'], show_probs = True)['positive']
      sh_sa = s_sh(j['title'], show_probs = True)['positive']
      j.update(nsmc = nm_sa, shop = sh_sa),
      NAVER = NAVER.append(j, ignore_index = True)
      NAVER.to_pickle(f"./naver_all_{f}.pkl")

  # pd.read_pickle('./naver_all.pkl').append(pd.read_pickle('./naver_all2.pkl'), ignore_index= True).to_pickle('./naver_all.pkl')
