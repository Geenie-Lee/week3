from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *

import psycopg2

class Minute(QAxWidget):
    def __init__(self):
        super().__init__()
        print(">> class Minute(QAxWidget) start.")

        # 전역변수 모음
        self.account_number = None
        self.total_minutes = 0
        self.stock_minute_list = []
        self.target_code_list = []
        self.market_type = None
        self.minute_type = "5"
        self.stock_minute_table = "stock_"+self.minute_type+"min_kosdaq"

        self.stock_minute_screen_number = "5000"
        self.current_order = 0

        # 20일 기준(1hour = 60(minutes) * 6(hours) + 30(minutes) = 390 * 20(days) = 7800)
        # 30분봉 = 1일(14봉)
        # 05분봉 = 1일(78봉)
        self.minutes = 78

        self.login_event_loop = QEventLoop()
        self.minute_info_event_loop = QEventLoop()

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
        # 수집 대상 시장 구분(0:장내 (1562), 10:코스닥 (1434), 3:ELW (3335), 8:ETF (450), 50:KONEX (145), 30:K-OTC (139), 4: 뮤추얼펀드, 5:신주인수권, 6:리츠, 9:하이얼펀드)
        # "8", "50", "30", "10", "0"
        market_type = ["3"]
        for i in range(len(market_type)):
            print("\n>> 시장구분[%s]의 종목 가져오기 실행" % market_type[i])
            self.market_type = market_type[i]
            self.set_table()

            stock_codes = self.dynamicCall("GetCodeListByMarket(QString)", market_type[i])
            stock_code_list = stock_codes.split(";")[:-1]

            cnt = 0
            for stock_code in stock_code_list:

                # 스크린 연결 끊기(200개의 스크린번호와 스크린번호 당 100개의 요청 가능)
                self.dynamicCall("DisconnectRealData(QString)", self.stock_minute_screen_number)

                # 종목건수 계산
                cnt = cnt + 1

                # 종목명 가져오기
                stock_code_name = self.get_master_code_name(stock_code)

                if self.market_type == '3' and (not (stock_code_name.startswith('한국G1') or stock_code_name.startswith('한국FP')) or stock_code_name.find('KOSPI200') == -1):
                    continue

                # 0000번 부터 시작(새벽 시스템점검으로 인한 접속 불가)
                if cnt < self.current_order:
                    print(">> 번호.%s 종목[%s][%s] 수집 및 저장 Skip 처리함" % (cnt, stock_code, stock_code_name))
                    continue

                # 종목의 분봉 데이터 가져오기
                self.total_minutes = 0

                if self.check_the_stock(stock_code) == True:
                    self.get_day_info_by_stock(stock_code=stock_code)
                    # print(">> %s/%s번째 종목[%s][%s] [%s]분봉 수집완료" % (cnt,len(stock_code_list),stock_code,stock_code_name,self.total_minutes))

                    # 하나의 종목이 완료되면 데이터베이스에 저장한다.
                    self.insert_stock_minute_list()
                    print(">> %s/%s번째 종목[%s][%s] [%s]%s분봉 데이터베이스 저장 완료" % (cnt, len(stock_code_list), stock_code, stock_code_name, len(self.stock_minute_list), self.minute_type))

                    # 초기화
                    self.stock_minute_list = []
                    # print(">> self.stock_minute_list 초기화(%s))" % len(self.stock_minute_list))

                if cnt % 100 == 0:
                    self.stock_minute_screen_number = str(int(self.stock_minute_screen_number) + 1)
                    # print(">> self.stock_minute_screen_number = %s" % self.stock_minute_screen_number)

            self.update_day_num()
            self.current_order = 0
            print("------------------------------------- end ------------------------------------------[%s]" % market_type)

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
            self.stock_minute_table = "stock_"+self.minute_type+"min_kospi"
        elif self.market_type == '3':
            self.stock_minute_table = "stock_"+self.minute_type+"min_elw"
        elif self.market_type == '8':
            self.stock_minute_table = "stock_"+self.minute_type+"min_etf"
        elif self.market_type == '10':
            self.stock_minute_table = "stock_"+self.minute_type+"min_kosdaq"
        elif self.market_type == '30':
            self.stock_minute_table = "stock_"+self.minute_type+"min_kotc"
        elif self.market_type == '50':
            self.stock_minute_table = "stock_"+self.minute_type+"min_konex"
        else:
            self.stock_minute_table = "stock_"+self.minute_type+"min_info"

        print(">> self.market_type = %s" % self.market_type)
        print(">> self.stock_minute_table = %s" % self.stock_minute_table)

    def get_master_code_name(self, stock_code):
        return self.dynamicCall("GetMasterCodeName(QString)", stock_code)

    def get_day_info_by_stock(self, stock_code=None, date=None, sPrevNext="0"):
        QTest.qWait(1000)  # 3600 Delay Time

        self.dynamicCall("SetInputValue(QString,QString)", "종목코드", stock_code)
        self.dynamicCall("SetInputValue(QString,QString)", "틱범위", self.minute_type)
        self.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")

        if date != None:
            self.dynamicCall("SetInputValue(QString,QString)", "기준일자", date)

        self.dynamicCall("CommRqData(QString,QString,int,QString)", "주식분봉차트조회요청", "opt10080", sPrevNext, self.stock_minute_screen_number)
        self.minute_info_event_loop.exec_()

    def trdata_slot(self, screen_number, tr_name, tr_code, record_name, sPrevNext):
        # 싱글데이터[주식분봉차트] 가져오기
        stock_code = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "종목코드").strip()

        # 종목명 가져오기
        stock_name = self.get_master_code_name(stock_code)

        # 멀티데이터 분봉 차트 가져오기
        multi_cnt = self.dynamicCall("GetRepeatCnt(QString,QString)", tr_code, tr_name)

        for i in range(multi_cnt):
            data = []
            close_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "현재가").strip()
            volume = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량").strip()
            conclusion_time = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "체결시간").strip()
            open_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "시가").strip()
            high_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "고가").strip()
            low_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "저가").strip()
            prev_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "전일종가").strip()
            adjusted_stock_price_type = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "수정주가구분").strip()
            adjusted_rate = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "수정비율").strip()
            major_industry_type = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "대업종구분").strip()
            minor_industry_type = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "소업종구분").strip()
            stock_info = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종목정보").strip()

            if close_price == '0':
                volume = '0'
            if open_price == '':
                open_price = '0'
            if high_price == '':
                high_price = '0'
            if low_price == '':
                low_price = '0'
            if close_price == '':
                close_price = '0'
            if prev_price == '':
                prev_price = '0'
            if volume == '':
                volume = '0'

            open_price = abs(int(open_price))
            high_price = abs(int(high_price))
            low_price = abs(int(low_price))
            close_price = abs(int(close_price))
            prev_price = abs(int(prev_price))
            volume = abs(int(volume))

            self.total_minutes = self.total_minutes + 1

            data.append("")
            data.append(stock_code)
            data.append(stock_name)
            data.append(conclusion_time)
            data.append(volume)
            data.append(close_price)
            data.append(open_price)
            data.append(high_price)
            data.append(low_price)
            data.append(self.market_type)
            data.append("")

            self.stock_minute_list.append(data.copy())

            # if int(volume) > 10000000:
            # print("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s " % (conclusion_time,stock_code,stock_name,open_price,high_price,low_price,close_price,volume,prev_price,adjusted_stock_price_type,adjusted_rate,major_industry_type,minor_industry_type,stock_info))

            # 지정한 분봉 건수에 도달하면 중지하고 다음 종목으로 넘어간다.
            if self.total_minutes == self.minutes:
                #  print(">> %s,%s,%s,%s,%s,%s,%s,%s,%s " % ((i+1),stock_code,date,close_price,open_price,high_price,low_price,volume,trading_value))
                sPrevNext = "0"
                break

        if sPrevNext == "2":
            self.get_day_info_by_stock(stock_code=stock_code, sPrevNext=sPrevNext)
        else:
            self.minute_info_event_loop.exit()

    def insert_stock_minute_list(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        day_num = 0
        for stock_day in self.stock_minute_list:
            delete_day_sql = "delete from " + self.stock_minute_table + " where stock_code = %s and conclusion_time = %s"
            cur.execute(delete_day_sql, (stock_day[1], stock_day[3],))

            day_num = day_num + 1
            sql = "insert into " + self.stock_minute_table + " (stock_code,stock_name,min_num,conclusion_time,volume,close_price,open_price,high_price,low_price,market_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(sql, (
            stock_day[1], stock_day[2], day_num, stock_day[3], stock_day[4], stock_day[5], stock_day[6], stock_day[7],
            stock_day[8], stock_day[9]))

        conn.commit()
        cur.close()
        conn.close()

    def update_day_num(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        # 시장유형의 모든 종목 입력 후 시장유형 별 분봉 순번 갱신
        update_day_sql = " update " + self.stock_minute_table + " s                                               "
        update_day_sql += "    set min_num = a.min_num                                                            "
        update_day_sql += "   from                                                                                "
        update_day_sql += "      (                                                                                "
        update_day_sql += "        select row_number() over(partition by stock_code order by conclusion_time desc) as min_num  "
        update_day_sql += "             , stock_code                                                              "
        update_day_sql += "             , conclusion_time                                                         "
        update_day_sql += "             , market_type                                                             "
        update_day_sql += "          from " + self.stock_minute_table + "                                         "
        update_day_sql += "         where market_type = %s                                                        "
        update_day_sql += "      ) as a                                                                           "
        update_day_sql += "  where s.stock_code = a.stock_code                                                    "
        update_day_sql += "    and s.conclusion_time = a.conclusion_time                                          "
        update_day_sql += "    and s.market_type = a.market_type                                                  "
        update_day_sql += "    and s.market_type = %s                                                             "

        cur.execute(update_day_sql, (self.market_type, self.market_type,))

        conn.commit()
        cur.close()
        conn.close()