from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *

import psycopg2


class Foreigner(QAxWidget):
    def __init__(self):
        super().__init__()
        print(">> class Foreigner(QAxWidget) start.")

        # 전역변수 모음
        self.account_number = None
        self.total_days = 0
        self.stock_day_list = []
#         self.target_code_list = ["032190","263750","091990","056190","032500","034950","225190","049520","225330","040420"]
        self.target_code_list = []
        self.market_type = None
        self.stock_day_table = "stock_day_foreigner"

        # 종목 일봉 데이터 수집 화면번호 지정
        self.stock_day_screen_number = "4000"
        
        # 짤려서 현재 실행할 순번 1001, 569
        # 2020-10-05 23:27
        # 2020-10-06 22:16 ~
        # 2020-10-08 22:58 ~ 2020-10-09 00:30 완료
        # 2020-10-12 MON
        # 2020-10-13 TUE 20:01 ~
        self.current_day = 0

        # 수집 대상 기간 일수
        self.days = 0

        # 로그인 Event Loop 실행
        self.login_event_loop = QEventLoop()

        
        self.day_info_event_loop = QEventLoop()

        # Kiwoom OCX 사용 함수 호출
        self.get_ocx_instance()

        # Event Slots
        self.event_slots()

        # 로그인 signal 함수 호출
        self.signal_login_commConnect()

        # 계좌정보 가져오기
        self.get_login_info()

        # 전체 종목 수집 및 분석을 위한 전체 종복 정보 가져오기
        self.get_day_info_by_stock(sPrevNext="0")

    def get_ocx_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")
        print(">> Kiwoom OCX KHOPENAPI.KHOpenAPICtrl.1 setControl")

    def event_slots(self):
        self.OnEventConnect.connect(self.login_event_slot)
        self.OnReceiveTrData.connect(self.trdata_slot)

    def signal_login_commConnect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_event_slot(self, error_code):
        print(">> 로그인 결과: [%s]%s" % (errors(error_code)[0], errors(error_code)[1]))
        self.login_event_loop.exit()

    def get_login_info(self):
        accounts = self.dynamicCall("GetLoginInfo(QString)", "ACCNO")
        self.account_number = accounts.split(";")[0]
        print(">> 계좌번호: %s" % self.account_number)

    def get_day_info_by_stock(self, sPrevNext="0"):
        QTest.qWait(1000) # 3600 Delay Time

        ############################## opt30005 : ELW조건검색요청 ##############################
        # 000000000000:전체, 3:한국투자증권, 5:미래대우, 6:신영, 12:NK투자, 17:KB증권
        self.dynamicCall("SetInputValue(QString,QString)", "발행사코드", "000000000000")

        # 000000000000:전체, 201:KOSPI200, 150:KOSDAQ150, 005930:삼성전자, ...
        self.dynamicCall("SetInputValue(QString,QString)", "기초자산코드", "201")

        # 0:전체, 1:콜, 2:풋, 3:DC, 4:DP, 5:EX, 6:조기종료콜, 7:조기종료풋
        self.dynamicCall("SetInputValue(QString,QString)", "권리구분", "0")

        # 000000000000:전체, 3:한국투자증, 5:미래대우, 6:신영, 12:NK투자, 17:KB증권
        self.dynamicCall("SetInputValue(QString,QString)", "LP코드", "000000000000")
        
        # 0:정렬없음, 1:상승율, 2:상승폭, 3:하락율, 4:하락폭, 5:거래량, 6:거래대금, 7:잔존일
        self.dynamicCall("SetInputValue(QString,QString)", "정렬구분", "5")

        self.dynamicCall("CommRqData(QString,QString,int,QString)", "ELW조건검색요청", "opt30005", sPrevNext, self.stock_day_screen_number)
        self.day_info_event_loop.exec_()

    def trdata_slot(self, screen_number, tr_name, tr_code, record_name, sPrevNext):

        # 멀티데이터 일봉 차트 가져오기
        multi_cnt = self.dynamicCall("GetRepeatCnt(QString,QString)", tr_code, tr_name)

        for i in range(multi_cnt):
            data = []
            output01 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종목코드").strip()
            output02 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "발행사명").strip()
            output03 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "회차").strip()
            output04 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "기초자산명").strip()
            output05 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "권리구분").strip()
            output06 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "만기일").strip()
            output07 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "현재가").strip()
            output08 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "대비구분").strip()
            output09 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "전일대비").strip()
            output10 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "등락율").strip()
            output11 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량").strip()
            output12 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량대비").strip()
            output13 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래대금").strip()
            output14 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "전일거래량").strip()
            output15 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "매도호가").strip()
            output16 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "매수호가").strip()
            output17 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "패리티").strip()
            output18 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "기어링비율").strip()
            output19 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "손일분기율").strip()
            output20 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "자본지지점").strip()
            output21 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "이론가").strip()
            output22 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "내재변동성").strip()
            output23 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "델타").strip()
            output24 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "레버리지").strip()
            output25 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "행사가격").strip()
            output26 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "전환비율").strip()
            output27 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "LP보유비율").strip()
            output28 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "손익분기점").strip()
            output29 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "최종거래일").strip()
            output30 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "상장일").strip()
            output31 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "LP초종공급일").strip()
            output32 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종목명").strip()
            output33 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "잔존일수").strip()
            output34 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "괴리율").strip()
            output35 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "LP회원사명").strip()
            output36 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "LP회원사명1").strip()
            output37 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "LP회원사명2").strip()
            output38 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "Xray순간체결량정리매매구분").strip()
            output39 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "Xray순간체결량증거금100구분").strip()

            self.total_days = self.total_days + 1

            data.append("")
            data.append(output01)
            data.append(output02)
            data.append(output03)
            data.append(output04)
            data.append(output05)
            data.append(output06)
            data.append(output07)
            data.append(output08)
            data.append(output09)
            data.append(output10)
            data.append(output11)
            data.append(output12)
            data.append(output13)
            data.append(output14)
            data.append(output15)
            data.append(output16)
            data.append(output17)
            data.append(output18)
            data.append(output19)
            data.append(output20)
            data.append(output21)
            data.append(output22)
            data.append(output23)
            data.append(output24)
            data.append(output25)
            data.append(output26)
            data.append(output27)
            data.append(output28)
            data.append(output29)
            data.append(output30)
            data.append(output31)
            data.append(output32)
            data.append(output33)
            data.append(output34)
            data.append(output35)
            data.append(output36)
            data.append(output37)
            data.append(output38)
            data.append(output39)
            data.append("")

            self.stock_day_list.append(data.copy())

            if output06 == '20201214' and int(output11) > 10000:
                print("종목코드:%s, 종목명:%s, 발행사명:%s, 회차:%s, 기초자산명:%s, 권리구분:%s, 만기일:%s, 행사가격:%s, 현재가:%s, 이론가:%s, 대비구분:%s, 전일대비:%s, 등락율:%s, 거래량:%s " % (output01, output32, output02, output03, output04, output05, output06, output25, output07, output21, output08, output09, output10, output11))

            if self.total_days == self.days:
#                 print(">> %s,%s,%s,%s,%s,%s,%s,%s,%s " % ((i+1),stock_code,date,close_price,open_price,high_price,low_price,volume,trading_value))
                sPrevNext = "0"
                break

        if sPrevNext == "2":
            self.get_day_info_by_stock(sPrevNext=sPrevNext)
        else:
            self.day_info_event_loop.exit()

    def insert_stock_day_list(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        day_num = 0
        for stock_day in self.stock_day_list:
            
            delete_day_sql = "delete from "+self.stock_day_table+" where stock_code = %s and dt = %s"
            cur.execute(delete_day_sql, (stock_day[1], stock_day[3],))
            
            day_num = day_num + 1
            sql = "insert into "+self.stock_day_table+" (stock_code,stock_name,day_num,dt,volume,trading_value,close_price,open_price,high_price,low_price,market_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(sql,(stock_day[1],stock_day[2],day_num,stock_day[3],stock_day[4],stock_day[5],stock_day[6],stock_day[7],stock_day[8],stock_day[9],stock_day[10]))

        conn.commit()
        cur.close()
        conn.close()
        
    def update_day_num(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        # 시장유형의 모든 종목 입력 후 시장유형 별 일봉 순번 갱신
        update_day_sql  = " update "+self.stock_day_table+" s              "
        update_day_sql += "    set day_num = a.day_num                     "
        update_day_sql += "   from                                         "
        update_day_sql += "      (                                         "
        update_day_sql += "        select row_number() over(partition by stock_code order by dt desc) as day_num  "
        update_day_sql += "             , stock_code                       "
        update_day_sql += "             , dt                               "
        update_day_sql += "             , market_type                      "
        update_day_sql += "          from "+self.stock_day_table+"         "
        update_day_sql += "         where market_type = %s                 "
        update_day_sql += "      ) as a                                    "
        update_day_sql += "  where s.stock_code = a.stock_code             "
        update_day_sql += "    and s.dt = a.dt                             "
        update_day_sql += "    and s.market_type = a.market_type           "
        update_day_sql += "    and s.market_type = %s                      "
        
        cur.execute(update_day_sql, (self.market_type, self.market_type,))

        conn.commit()
        cur.close()
        conn.close()        