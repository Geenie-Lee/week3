from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *

import psycopg2
from datetime import datetime

class StockBaseInfo(QAxWidget):

    def __init__(self):
        super().__init__()
        print(">> class StockBaseInfo() Start.")

        # [시장구분값] (0:장내, 10:코스닥, 3:ELW, 8:ETF, 50:KONEX, 4: 뮤추얼펀드, 5:신주인수권, 6:리츠, 9:하이얼펀드, 30:K-OTC)
        # self.market_types = ["0", "10", "3", "8", "50", "4", "5", "6", "9", "30"]
        # self.market_types = ["5", "8", "10", "0"]
        self.market_types = ["0"]   # "10", "0"
        self.base_info_list = []
#         self.target_code_list = ["032190","263750","091990","056190","032500","034950","225190","049520","225330","040420"]
        self.target_code_list = []
        self.market_type = None
        self.today = (datetime.now()).strftime('%Y%m%d')
        # self.today = "20200616"
        self.stock_number = 0
        self.stock_code = None
        self.stock_name = None
        
        # 수집 시작 번호
        self.current_number = 0
        
        self.tr_screen_number = "1000"
        self.login_event_loop = QEventLoop()
        self.base_info_event_loop = QEventLoop()

        self.get_ocx_instance()
        self.event_slots()
        self.login_signal()
        self.get_account_number()
        self.get_stock_code_list_by_market()

    def get_ocx_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def event_slots(self):
        self.OnEventConnect.connect(self.login_slot)
        self.OnReceiveTrData.connect(self.base_info_slot)

    def login_signal(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_slot(self, error_code):
        print(errors(error_code)[1])
        login_status = self.dynamicCall("GetConnectState()")
        print(">> 로그인 상태(0:연결안됨, 1:연결): %s" % login_status)
        self.login_event_loop.exit()

    def get_account_number(self):
        accounts = self.dynamicCall("GetLoginInfo(QString)", "ACCNO")
        account_list = accounts.split(";")
        for account in account_list:
            print("계좌번호: %s" % account)


    """
        1. 시장유형별 종목 기본 정보를 가져와 DB에 삭제 후 입력한다.
    """
    def get_stock_code_list_by_market(self):
        for market_type in self.market_types:
            self.market_type = market_type
            self.base_info_list = None
            self.base_info_list = []

            # 시장유형에 해당하는 종목코드를 가져와 리스트로 만든 후 반복.
            codes = self.dynamicCall("GetCodeListByMarket(QString)", market_type)
            code_list = codes.split(";")[:-1]
            cnt = 0
            for code in code_list:
                cnt = cnt + 1
                    
#                 if cnt <= 1000:
#                     continue
                

                # 전체 대상 수집 시 주석 처리 후 아래 주석 해제 필요
                # 지정된 종목만 수집하고자 한다.
#                 if self.check_the_stock(code) == True:
#                     print(">> 종목[%s] 수집" % code)
#                     self.base_info_signal(stock_code=code)
#                     self.daily_info_signal(stock_code=code)



                # 서버에서 과도요청으로 짤랐을 경우
                if self.current_number > 0 and cnt < self.current_number:
                    continue

                # 종목의 주식기본정보(opt10001)요청
                self.base_info_signal(stock_code=code)
                
                # 종목의 주식일주월시분(opt10005)요청
                self.stock_number = cnt
#                 self.daily_info_signal(stock_code=code)
#                 print(">> 시장유형[%s] %s/%s번째 종목[%s] 가져오기 완료." % (self.market_type, cnt, len(code_list), code))

#                 100건으로 짤라서 DB 입력
                if (cnt % 100) == 0:
                    self.insert_base_info_list()
                    self.tr_screen_number = str(int(self.tr_screen_number) + 1)
                    print(">> 시장유형[%s] 부분 %s건 저장완료(%s)." % (self.market_type, cnt, self.tr_screen_number))
                    
#             시장유형별로 리스트에 저장된 주식기본정보를 일괄입력방식으로 데이터베이스에 저장
            self.insert_base_info_list()
            self.update_day_num()
            print(">> 시장유형[%s] 잔여 %s건 저장완료." % (self.market_type, cnt))
                        
            self.stock_number = 0
            print("")
            print("--------------------------------------------------------------------------")        
            print("")

    def check_the_stock(self, stock_code=None):
        chk = False
        for target_code in self.target_code_list:
            if target_code == stock_code:
                chk = True
    
        return chk
        
    def base_info_signal(self, stock_code=None):
        # print("\n>> base_info_signal.stock_code[%s]" % stock_code)
        QTest.qWait(1000)
        self.dynamicCall("SetInputValue(QString,QString)", "종목코드", stock_code)
        self.dynamicCall("CommRqData(QString,QString,int,QString)", "주식기본정보", "opt10001", 0, self.tr_screen_number)
        self.base_info_event_loop.exec_()

    def daily_info_signal(self, stock_code=None):
        QTest.qWait(1000)    # 3600
        stock_name = self.dynamicCall("GetMasterCodeName(QString)", stock_code)
#         print("")
#         print(">> 종목코드: %s, 종목명: %s" % (stock_code, stock_name))
#         print("%s,%s" % (stock_code, stock_name))
        self.stock_code = stock_code
        self.stock_name = stock_name

        self.dynamicCall("SetInputValue(QString,QString)", "종목코드", stock_code)
        self.dynamicCall("CommRqData(QString,QString,int,QString)", "주식일주월시분", "opt10005", 0, self.tr_screen_number)
        self.base_info_event_loop.exec_()

    def base_info_slot(self, screen_number, tr_name, tr_code, record_name, prev_next):
        if tr_code == "opt10001":
            stock_code = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "종목코드").strip()
            stock_name = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "종목명").strip()
            settlement_month = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "결산월").strip()
            face_value = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "액면가").strip()
            capital = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "자본금").strip()
            listed_stock = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "상장주식").strip()
            credit_ratio = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "신용비율").strip()
            record_high = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "연중최고").strip()
            record_low = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "연중최저").strip()
            market_capitalization = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "시가총액").strip()
            market_capitalization_weight = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "시가총액비중").strip()
            foreigner_burnout_rate = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "외인소진률").strip()
            substitute_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "대용가").strip()
            price_earnings_ratio = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "PER").strip()
            earnings_per_share = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "EPS").strip()
            return_on_equity = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "ROE").strip()
            price_on_book_value_ratio = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "PBR").strip()
            enterprise_value = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "EV").strip()
            bookvalue_per_share = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "BPS").strip()
            turnover = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "매출액").strip()
            business_profit = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "영업이익").strip()
            net_profit = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "당기순이익").strip()
            high_price_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최고").strip()
            low_price_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최저").strip()
            open_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "시가").strip()
            high_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "고가").strip()
            low_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "저가").strip()
            upper_limit_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "상한가").strip()
            lower_limit_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "하한가").strip()
            standard_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "기준가").strip()
            estimated_settlement_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "예상체결가").strip()
            estimated_settlement_amount = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "예상체결수량").strip()
            high_price_date_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최고가일").strip()
            high_price_ratio_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최고가대비율").strip()
            low_price_date_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최저가일").strip()
            low_price_ratio_250 = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "250최저가대비율").strip()
            close_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "현재가").strip()
            contrast_symbol = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "대비기호").strip()
            net_change = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "전일대비").strip()
            fluctuation_rate = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "등락율").strip()
            volume = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "거래량").strip()
            trade_contrast = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "거래대비").strip()
            face_value_unit = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "액면가단위").strip()
            outstanding_stock = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "유통주식").strip()
            distribution_ratio = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, 0, "유통비율").strip()
            market_type = self.market_type
            dt = self.today
            
            # 가져온 종목 정보는 이중리스트에 저장한 후 데이터베이스에 저장한다.
            # 데이터베이스에는 기본정보 테이블에 삭제 후 저장하고 일봉정보에 당일자로 삭제 후 저장한다.
            base_info = []

            base_info.append("")
            base_info.append(stock_code)
            base_info.append(stock_name)
            base_info.append(self.to_int(settlement_month))
            base_info.append(self.to_float(face_value))
            base_info.append(self.to_int(capital))
            base_info.append(self.to_int(listed_stock))
            base_info.append(self.to_float(credit_ratio))
            base_info.append(self.to_float(record_high))
            base_info.append(self.to_float(record_low))
            base_info.append(self.to_int(market_capitalization))
            base_info.append(self.to_int(market_capitalization_weight))
            base_info.append(self.to_float(foreigner_burnout_rate))
            base_info.append(self.to_int(substitute_price))
            base_info.append(self.to_float(price_earnings_ratio))
            base_info.append(self.to_int(earnings_per_share))
            base_info.append(self.to_float(return_on_equity))
            base_info.append(self.to_float(price_on_book_value_ratio))
            base_info.append(self.to_float(enterprise_value))
            base_info.append(self.to_int(bookvalue_per_share))
            base_info.append(self.to_int(turnover))
            base_info.append(self.to_int(business_profit))
            base_info.append(self.to_int(net_profit))
            base_info.append(self.to_int(high_price_250))
            base_info.append(self.to_int(low_price_250))
            base_info.append(self.to_int(open_price))
            base_info.append(self.to_int(high_price))
            base_info.append(self.to_int(low_price))
            base_info.append(self.to_int(upper_limit_price))
            base_info.append(self.to_int(lower_limit_price))
            base_info.append(int(standard_price))
            base_info.append(self.to_int(estimated_settlement_price))
            base_info.append(self.to_int(estimated_settlement_amount))
            base_info.append(high_price_date_250)
            base_info.append(self.to_float(high_price_ratio_250))
            base_info.append(low_price_date_250)
            base_info.append(self.to_float(low_price_ratio_250))
            base_info.append(self.to_int(close_price))
            base_info.append(self.to_int(contrast_symbol))
            base_info.append(self.to_int(net_change))
            base_info.append(self.to_float(fluctuation_rate))
            base_info.append(self.to_int(volume))
            base_info.append(self.to_float(trade_contrast))
            base_info.append(face_value_unit)
            base_info.append(self.to_int(outstanding_stock))
            base_info.append(self.to_float(distribution_ratio))
            base_info.append(market_type)
            base_info.append(dt)
            base_info.append("")
            
            # print(base_info)
            self.base_info_list.append(base_info.copy())

            print(
                "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.stock_number+1,
                    stock_code,stock_name,settlement_month,face_value,capital,
                    listed_stock,credit_ratio,record_high,record_low,market_capitalization,
                    market_capitalization_weight,foreigner_burnout_rate,substitute_price,price_earnings_ratio,earnings_per_share,
                    return_on_equity,price_on_book_value_ratio,enterprise_value,bookvalue_per_share,turnover,
                    business_profit,net_profit,high_price_250,low_price_250,open_price,
                    high_price,low_price,upper_limit_price,lower_limit_price,standard_price,
                    estimated_settlement_price,estimated_settlement_amount,high_price_date_250,high_price_ratio_250,low_price_date_250,
                    low_price_ratio_250,close_price,contrast_symbol,net_change,fluctuation_rate,
                    volume,trade_contrast,face_value_unit,outstanding_stock,distribution_ratio,
                    market_type,dt
                    ))

        elif tr_code == "opt10005":
            rows = self.dynamicCall("GetRepeatCnt(QString,QString)", tr_code, record_name)
            # print("> %s %s %s rows: %s" % (tr_code, screen_number, prev_next, rows))

            for i in range(rows):
                date = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "날짜").strip()
                open_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "시가").strip()
                high_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "고가").strip()
                low_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "저가").strip()
                close_price = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "종가").strip()
                # 대비
                fluctuation_rate = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "등락률").strip()
                volume = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래량").strip()

                trading_value = self.dynamicCall("GetCommData(QString,QString,int,QString)", tr_code, tr_name, i, "거래대금").strip()
                # 체결강도,외인보유,외인비중,외인순매수,기관순매수,개인순매수,외국계,신용잔고율,프로그램

                if open_price == '':
                    open_price = '0'
                open_price = abs(int(open_price))
                if high_price == '':
                    high_price = '0'
                high_price = abs(int(high_price))
                if low_price == '':
                    low_price = '0'
                low_price = abs(int(low_price))
                if close_price == '':
                    close_price = '0'
                close_price = abs(int(close_price))
                if volume == '':
                    volume = '0'
                volume = abs(int(volume))

                if i <= 0:
                    print("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s" % (self.stock_number, self.stock_code, self.stock_name, date, open_price, high_price, low_price, close_price, volume, fluctuation_rate))

#                     if volume > 2000000:
#                         print("> 날짜: %s, 시가: %s, 고가: %s, 저가: %s, 종가: %s, 거래량: %s, 등락률: %s" % (date, open_price, high_price, low_price, close_price, volume, fluctuation_rate))
                        # print("> 시가: %s" % format(open_price, ","))
                        # print("> 고가: %s" % format(high_price, ","))
                        # print("> 저가: %s" % format(low_price, ","))
                        # print("> 종가: %s" % format(close_price, ","))
                        # print("> 등락률: %s" % fluctuation_rate)
                        # print("> 거래량: %s" % format(volume, ","))
                        # print("> 거래대금: %s" % trading_value)
                        # print("")

        self.base_info_event_loop.exit()

    def insert_base_info_list(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()

        day_num = 0
        for base_info in self.base_info_list:
            delete_base_sql = "delete from stock_base_info where stock_code = %s"
            cur.execute(delete_base_sql, (base_info[1],))

            insert_base_sql = "insert into stock_base_info(stock_code,stock_name,settlement_month,face_value,capital,listed_stock,credit_ratio,record_high,record_low,market_capitalization,market_capitalization_weight,foreigner_burnout_rate,substitute_price,price_earnings_ratio,earnings_per_share,return_on_equity,price_on_book_value_ratio,enterprise_value,bookvalue_per_share,turnover,business_profit,net_profit,high_price_250,low_price_250,open_price,high_price,low_price,upper_limit_price,lower_limit_price,standard_price,estimated_settlement_price,estimated_settlement_amount,high_price_date_250,high_price_ratio_250,low_price_date_250,low_price_ratio_250,close_price,contrast_symbol,net_change,fluctuation_rate,volume,trade_contrast,face_value_unit,outstanding_stock,distribution_ratio,market_type,dt) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cur.execute(insert_base_sql, (
            base_info[1],  base_info[2],  base_info[3],  base_info[4],  base_info[5],  base_info[6],  base_info[7],  base_info[8],  base_info[9],  base_info[10], 
            base_info[11], base_info[12], base_info[13], base_info[14], base_info[15], base_info[16], base_info[17], base_info[18], base_info[19], base_info[20], 
            base_info[21], base_info[22], base_info[23], base_info[24], base_info[25], base_info[26], base_info[27], base_info[28], base_info[29], base_info[30],
            base_info[31], base_info[32], base_info[33], base_info[34], base_info[35], base_info[36], base_info[37], base_info[38], base_info[39], base_info[40],
            base_info[41], base_info[42], base_info[43], base_info[44], base_info[45], base_info[46], base_info[47]
            ))

#             delete_day_sql = "delete from stock_day_info where stock_code = %s and dt = %s"
#             cur.execute(delete_day_sql, (base_info[1], self.today,))
#             
#             insert_day_sql = "insert into stock_day_info (stock_code,stock_name,day_num,dt,volume,trading_value,close_price,open_price,high_price,low_price,market_type) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
#             cur.execute(insert_day_sql, (
#             base_info[1], base_info[2], day_num, self.today, base_info[4], base_info[5], base_info[6], base_info[7], base_info[8],
#             base_info[9], base_info[3]))

        self.base_info_list.clear()

        conn.commit()
        cur.close()
        conn.close()

    def to_int(self, val):
        if val == '': val = 0
        else: val = int(val.lstrip('+').lstrip('-'))
        return val
        
    def to_float(self, val):
        if val == '': val = 0.0
        else: val = float(val.lstrip('+').lstrip('-'))
        return val

    def update_day_num(self):
        conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres' port='5432'"
        conn = psycopg2.connect(conn_string)
        cur = conn.cursor()
        
        # 시장유형의 모든 종목 입력 후 시장유형 별 일봉 순번 갱신
        update_day_sql = """
                            update stock_day_info s
                               set day_num = a.day_num
                              from 
                                 (
                                   select row_number() over(partition by stock_code order by dt desc) as day_num
                                        , stock_code
                                        , dt
                                        , market_type 
                                     from stock_day_info
                                    where market_type = %s      
                                 ) as a
                             where s.stock_code = a.stock_code
                               and s.dt = a.dt
                               and s.market_type = a.market_type
                               and s.market_type = %s        
        """
        cur.execute(update_day_sql, (self.market_type, self.market_type,))

        conn.commit()
        cur.close()
        conn.close()
        

if __name__ == "__main__":
    StockBaseInfo()