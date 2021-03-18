from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest

from config.errorCode import *

import psycopg2
import time


class Week(QAxWidget):
    def __init__(self):
        super().__init__()
        print(">> class Week(QAxWidget) start.")

        # 전역변수 모음
        now = time.localtime()
        self.today = str(now.tm_year) + str(now.tm_mon) + str(now.tm_mday)

        print('>> Today is %s' % self.today)

        self.account_number = None
        self.total_weeks = 0
        self.stock_week_list = []
        #         self.target_code_list = ['092220','071970','142280','047920','009680','053060','003415','238090','007575','144620','114120','053610','006200']
        self.target_code_list = []
        self.market_type = None
        self.stock_week_table = "stock_week_kosdaq"

        # 종목 주봉 데이터 수집 화면번호 지정
        self.stock_week_screen_number = "4000"

        # 짤려서 현재 실행할 순번
        self.current_week = 1475

        # 수집 대상 기간 일수
        self.weeks = 0

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
        self.get_code_list_by_market()

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

    def get_code_list_by_market(self):
        # 수집 대상 시장 구분(0:장내, 10:코스닥, 3:ELW, 8:ETF, 50:KONEX, 30:K-OTC, 4: 뮤추얼펀드, 5:신주인수권, 6:리츠, 9:하이얼펀드)
        # "8", "50", "30", "0","10"
        market_type = ["0"]
        for i in range(len(market_type)):
            print("\n>> 시장구분[%s]의 종목 가져오기 실행" % market_type[i])
            self.market_type = market_type[i]
            self.set_table()

            stock_codes = self.dynamicCall("GetCodeListByMarket(QString)", market_type[i])
            stock_code_list = stock_codes.split(";")[:-1]

            cnt = 0
            for stock_code in stock_code_list:

                # 스크린 연결 끊기(200개의 스크린번호와 스크린번호 당 100개의 요청 가능)
                self.dynamicCall("DisconnectRealData(QString)", self.stock_week_screen_number)

                # 종목건수 계산
                cnt = cnt + 1

                # 종목명 가져오기
                stock_code_name = self.get_master_code_name(stock_code)

                # 1106번 부터 시작(새벽 시스템점검으로 인한 접속 불가)
                if cnt < self.current_week:
                    print(">> 번호.%s 종목[%s][%s] 수집 및 저장 Skip 처리함" % (cnt, stock_code, stock_code_name))
                    continue

                # 종목의 주봉 데이터 가져오기
                self.total_weeks = 0

                if self.check_the_stock(stock_code) == True:
                    self.get_week_info_by_stock(stock_code=stock_code)
                    print(">> %s/%s번째 종목[%s][%s] [%s]주봉 수집완료" % (
                    cnt, len(stock_code_list), stock_code, stock_code_name, self.total_weeks))

                    # 하나의 종목이 완료되면 데이터베이스에 저장한다.
                    self.insert_stock_week_list()
                    print(">> %s/%s번째 종목[%s][%s] [%s]주봉 데이터베이스 저장 완료" % (
                    cnt, len(stock_code_list), stock_code, stock_code_name, len(self.stock_week_list)))

                    # 초기화
                    self.stock_week_list = []
                    # print(">> self.stock_week_list 초기화(%s))" % len(self.stock_week_list))

                if cnt % 100 == 0:
                    self.stock_week_screen_number = str(int(self.stock_week_screen_number) + 1)
                    print(">> self.stock_week_screen_number = %s" % self.stock_week_screen_number)

            self.update_week_num()
            self.current_week = 0
            print("------------------------------------- end ------------------------------------------")

    def check_the_stock(self, stock_code=None):
        if len(self.target_code_list) == 0:
            return True

        chk = False
        for target_code in self.target_code_list:
            if target_code == stock_code:
                chk = True
        return chk

    def set_table(self):
        if self.market_type == '0':
            self.stock_week_table = 'stock_week_kospi'
        elif self.market_type == '3':
            self.stock_week_table = 'stock_week_elw'
        elif self.market_type == '8':
            self.stock_week_table = 'stock_week_etf'
        elif self.market_type == '10':
            self.stock_week_table = 'stock_week_kosdaq'
        elif self.market_type == '30':
            self.stock_week_table = 'stock_week_kotc'
        elif self.market_type == '50':
            self.stock_week_table = 'stock_week_konex'
        else:
            self.stock_week_table = 'stock_week_info'

    def get_master_code_name(self, stock_code):
        return self.dynamicCall("GetMasterCodeName(QString)", stock_code)

    def get_week_info_by_stock(self, stock_code=None, date=None, sPrevNext="0"):
        QTest.qWait(1000)  # 3600 Delay Time

        self.dynamicCall("SetInputValue(QString,QString)", "종목코드", stock_code)
        self.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")
        self.dynamicCall("SetInputValue(QString,QString)", "기준일자", self.today)
        self.dynamicCall("SetInputValue(QString,QString)", "끝일자", date)

        #         if date != None:
        #             self.dynamicCall("SetInputValue(QString,QString)", "기준일자", date)

        self.dynamicCall("CommRqData(QString,QString,int,QString)", "주식주봉차트조회요청", "opt10082", sPrevNext,
                         self.stock_week_screen_number)
        self.day_info_event_loop.exec_()

    def trdata_slot(self, screen_number, tr_name, tr_code, record_name, sPrevNext):
        # 싱글데이터[주식주봉차트] 가져오기
        stock_code = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "종목코드").strip()

        # 종목명 가져오기
        stock_name = self.get_master_code_name(stock_code)

        # 멀티데이터 주봉 차트 가져오기
        multi_cnt = self.dynamicCall("GetRepeatCnt(QString,QString)", tr_code, tr_name)

        for i in range(multi_cnt):
            data = []
            close_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i,
                                           "현재가").strip()
            volume = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량").strip()
            trading_value = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i,
                                             "거래대금").strip()
            date = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "일자").strip()
            open_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "시가").strip()
            high_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "고가").strip()
            low_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "저가").strip()

            if close_price == '0':
                volume = '0'

            #             print(">> %s,%s,%s,%s,%s,%s,%s,%s,%s " % ((i+1),stock_code,date,close_price,open_price,high_price,low_price,volume,trading_value))
            self.total_weeks = self.total_weeks + 1

            data.append("")
            data.append(stock_code)
            data.append(stock_name)
            data.append(date)
            data.append(volume)
            data.append(trading_value)
            data.append(close_price)
            data.append(open_price)
            data.append(high_price)
            data.append(low_price)
            data.append(self.market_type)
            data.append("")

            self.stock_week_list.append(data.copy())

            if self.total_weeks == self.weeks:
                #                 print(">> %s,%s,%s,%s,%s,%s,%s,%s,%s " % ((i+1),stock_code,date,close_price,open_price,high_price,low_price,volume,trading_value))
                sPrevNext = "0"
                break

        if sPrevNext == "2":
            self.get_week_info_by_stock(stock_code=stock_code, sPrevNext=sPrevNext)
        else:
            self.day_info_event_loop.exit()

    def insert_stock_week_list(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        week_num = 0
        for stock_week in self.stock_week_list:
            delete_day_sql = "delete from " + self.stock_week_table + " where stock_code = %s and dt = %s"
            cur.execute(delete_day_sql, (stock_week[1], stock_week[3],))

            week_num = week_num + 1
            sql = "insert into " + self.stock_week_table + " (stock_code,stock_name,week_num,dt,volume,trading_value,close_price,open_price,high_price,low_price,market_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(sql, (
            stock_week[1], stock_week[2], week_num, stock_week[3], stock_week[4], stock_week[5], stock_week[6],
            stock_week[7], stock_week[8], stock_week[9], stock_week[10]))

        conn.commit()
        cur.close()
        conn.close()

    def update_week_num(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # 시장유형의 모든 종목 입력 후 시장유형 별 주봉 순번 갱신
        update_week_sql = " update " + self.stock_week_table + " s                                                     "
        update_week_sql += "    set week_num = a.week_num                                                            "
        update_week_sql += "   from                                                                                "
        update_week_sql += "      (                                                                                "
        update_week_sql += "        select row_number() over(partition by stock_code order by dt desc) as week_num  "
        update_week_sql += "             , stock_code                                                              "
        update_week_sql += "             , dt                                                                      "
        update_week_sql += "             , market_type                                                             "
        update_week_sql += "          from " + self.stock_week_table + "                                                "
        update_week_sql += "         where market_type = %s                                                        "
        update_week_sql += "      ) as a                                                                           "
        update_week_sql += "  where s.stock_code = a.stock_code                                                    "
        update_week_sql += "    and s.dt = a.dt                                                                    "
        update_week_sql += "    and s.market_type = a.market_type                                                  "
        update_week_sql += "    and s.market_type = %s                                                             "

        cur.execute(update_week_sql, (self.market_type, self.market_type,))

        conn.commit()
        cur.close()
        conn.close()        