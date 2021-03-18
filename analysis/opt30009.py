from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *

import psycopg2


class OPT30009(QAxWidget):
    def __init__(self):
        super().__init__()

        # 전역변수 모음
        self.account_number = None
        self.total_days = 0
        self.stock_day_list = []
        self.target_code_list = []
        self.market_type = None
        self.stock_day_table = "stock_day_opt30009"

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

    def event_slots(self):
        self.OnEventConnect.connect(self.login_event_slot)
        self.OnReceiveTrData.connect(self.trdata_slot)

    def signal_login_commConnect(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_event_slot(self, error_code):
        self.login_event_loop.exit()

    def get_login_info(self):
        accounts = self.dynamicCall("GetLoginInfo(QString)", "ACCNO")
        self.account_number = accounts.split(";")[0]

    def get_day_info_by_stock(self, sPrevNext="0"):
        QTest.qWait(1000) # 3600 Delay Time

        if(sPrevNext == '0'):
            print("순위,종목코드,종목명,현재가,대비기호,전일대비,등락률,매도잔량,매수잔량,거래량,거래대금")

        ############################## opt30009 : ELW등락율순위요청 ##############################
        # 정렬구분 = 1:상승률, 2:상승폭, 3:하락률, 4:하락폭
        self.dynamicCall("SetInputValue(QString,QString)", "정렬구분", "1")

        # 권리구분 = 000:전체, 001:콜, 002:풋, 003:DC, 004:DP, 006:조기종료콜, 007:조기종료풋
        self.dynamicCall("SetInputValue(QString,QString)", "권리구분", "000")

        # 거래종료제외 = 1:거래종료제외, 0:거래종료포함
        self.dynamicCall("SetInputValue(QString,QString)", "거래종료제외", "1")

        self.dynamicCall("CommRqData(QString,QString,int,QString)", "ELW등락율순위요청", "opt30009", sPrevNext, self.stock_day_screen_number)
        self.day_info_event_loop.exec_()

    def trdata_slot(self, screen_number, tr_name, tr_code, record_name, sPrevNext):

        # 멀티데이터 일봉 차트 가져오기
        multi_cnt = self.dynamicCall("GetRepeatCnt(QString,QString)", tr_code, tr_name)

        for i in range(multi_cnt):
            data = []
            output01 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "순위").strip()
            output02 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종목코드").strip()
            output03 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종목명").strip()
            output04 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "현재가").strip()
            output05 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "대비기호").strip()
            output06 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "전일대비").strip()
            output07 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "등락률").strip()
            output08 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "매도잔량").strip()
            output09 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "매수잔량").strip()
            output10 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량").strip()
            output11 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래대금").strip()

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
            data.append("")

            self.stock_day_list.append(data.copy())

            if int(output11) > 0:
                print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (output01, output02, output03, output04, output05, output06, output07, output08, output09, output10, output11))

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