import urllib.request
from bs4 import BeautifulSoup

code =['096530']
for c in code:
    url="http://comp.fnguide.com/SVO2/ASP/SVD_Invest.asp?pGB=1&gicode=A" + c + "&cID=&MenuYn=Y&ReportGB=&NewMenuID=105&stkGb=701"
    f = urllib.request.urlopen(url).read()
    soup=BeautifulSoup(f, 'html.parser')
    cells = soup.find('tr', {'id': "p_grid1_10"}).find_all('td')
    for cell in cells:
        print (cell.string)