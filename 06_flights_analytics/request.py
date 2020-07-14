import requests
import re
from bs4 import BeautifulSoup
import pandas as pd
URL = 'https://code.s3.yandex.net/learning-materials/data-analyst/festival_news/index.html'
req = requests.get(URL)
soup = BeautifulSoup(req.text, 'lxml')
table = soup.find('table', attrs = {"id": "best-festivals"})
heading_table = []
for row in soup.find_all('th'):
    heading_table.append(row.text)
content = []
for row in soup.find_all('tr'):
    if not row.find_all('th'):
        content.append([element.text for element in row.find_all('td')])
festivals = pd.DataFrame(content, columns=heading_table)
print(festivals)