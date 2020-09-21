from bs4 import BeautifulSoup  # for html parser
from urllib.request import urlopen  # for html request/respone
import pandas as pd  # for DataFrame
from html_table_parser import parser_functions as parser

API_KEY = ""  # API 이용에 필요한 인증번호
COMPANY_CODE = input("기업종목코드 6자리 : ")


def searching_report():  # 기업종목번호 기반 전체 사업보고서 검색 함수
    SEARCH_URL = "http://dart.fss.or.kr/api/search.xml?auth=" + API_KEY + "&crp_cd=" + COMPANY_CODE
    SEARCH_URL = SEARCH_URL + "&start_dt=19990101&bsn_tp=A001&fin_rpt=Y"  # 검색날짜 범위 / 사업보고서 / 최종보고서만
    XML_RESULT = BeautifulSoup(urlopen(SEARCH_URL).read(), 'html.parser')

    print(SEARCH_URL)

    find_list = XML_RESULT.findAll("list")  # list태그를 모두 탐색
    data = pd.DataFrame()  # 데이터를 저장할 프레임 선언
    for t in find_list:  # 나열번호, 기업명, 기업번호, 보고서명, 보고서번호, 제출인,  접수일자, 비고
        temp = pd.DataFrame(([[t.crp_cls.string, t.crp_nm.string, t.crp_cd.string, t.rpt_nm.string,
                               t.rcp_no.string, t.flr_nm.string, t.rcp_dt.string, t.rmk.string]]),
                            columns=["crp_cls", "crp_nm", "crp_cd", "rpt_nm", "rcp_no", "flr_nm", "rcp_dt", "rmk"])
        data = pd.concat([data, temp])

    if len(data) < 1:  # 검색 결과가 없을 시.
        print("### Failed. (There's no report.)")
        return None

    del data['crp_cls']
    del data['crp_cd']  # 나열번호, 기업번호 제거 (불필요)
    data = data.reset_index(drop=True)

    return data


def fs_table():  # 검색된 전체 사업보고서 기반 재무제표 추출 함수
    data = searching_report()
    document_count = 0

    for i in range(len(data)):
        MAIN_URL = "http://dart.fss.or.kr/dsaf001/main.do?rcpNo=" + data['rcp_no'][document_count]
        print(MAIN_URL)

        page = BeautifulSoup(urlopen(MAIN_URL).read(), 'html.parser')
        body = str(page.find('head'))

        if len(body.split('연결재무제표",')) <= 1:  # 연결재무제표 & 재무제표 탐색 시작
            if len(body.split('연 결 재 무 제 표",')) >= 2:
                body = body.split('연 결 재 무 제 표",')[1]  # "연 결 재 무 제 표" 로 발견
                print_page_1 = '연 결 재 무 제 표'
            else:
                if len(body.split('재무제표",')) <= 1:  # 연결재무제표가 없다면 재무제표 로 탐색 시작
                    if len(body.split('재 무 제 표",')) >= 2:
                        body = body.split('재 무 제 표",')[1]  # "재 무 제 표" 로 발견
                        print_page_1 = '재 무 제 표'
                    else:
                        print("### Failed. (연결재무제표/재무제표 페이지 탐색 실패.)")
                        return 0  # 아무것도 발견하지 못할 시 프로그램 종료.
                else:
                    body = body.split('재무제표",')[1]  # "재무제표" 로 발견
                    print_page_1 = '재무제표'
        else:
            body = body.split('연결재무제표",')[1]  # "연결재무제표" 로 발견
            print_page_1 = '연결재무제표'

        body = body.split('cnt++')[0].split('viewDoc(')[1].split(')')[0].split(', ')
        body = [body[i][1:-1] for i in range(len(body))]  # 찾아낸 재무제표 페이지로 이동하기 위한 url구성 번호 파싱
        VIEWER_URL = "http://dart.fss.or.kr/report/viewer.do?rcpNo=" + body[0] \
                     + '&dcmNo=' + body[1] + '&eleId=' + body[2] + '&offset=' + body[3] \
                     + '&length=' + body[4] + '&dtd=dart3.xsd'
        print(VIEWER_URL)

        page = BeautifulSoup(urlopen(VIEWER_URL).read(), 'html.parser')
        if len(str(page.find('body')).split('재 무 상 태 표')) == 1:  # 재무상태표 탐색 시작
            if len(str(page.find('body')).split('재무상태표')) <= 1:  # 재무상태표를 찾아내지 못한다면 프로그램 종료
                print("### Failed. (재무상태표 탐색 실패.)")
                return 0
            else:
                body = str(page.find('body')).split('재무상태표')[1]  # "재무상태표" 로 발견
                print_page_2 = '재무상태표'
        else:
            body = str(page.find('body')).split('재 무 상 태 표')[1]  # "재 무 상 태 표" 로 발견
            print_page_2 = '재 무 상 태 표'

        body = BeautifulSoup(body, 'html.parser')  # 찾아낸 재무상태표를 읽어내기 위해 파싱

        print(print_page_1 + " - " + print_page_2)
        print(body.find(align='RIGHT').text)

        table = body.find_all('table')  # table 태그 탐색
        if len(table) <= 1:  # 탐색 실패시 프로그램 종료
            print("### Failed. (there's no table.)")
            return 0

        p = parser.make2d(table[0])
        table = pd.DataFrame(p[1:], columns=p[0])
        table = table.set_index(p[0][0])

        table.to_csv('C:\\Users\\admin\\Desktop\\Test_Result\\' + print_page_1 + "_" + print_page_2 + '_'
                     + str(document_count) + '.csv', encoding='cp949')
        document_count += 1

    return table


print(fs_table())  # for testing.
