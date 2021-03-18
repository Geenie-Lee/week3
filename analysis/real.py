from PyQt5.QAxContainer import QAxWidget
from PyQt5.Qt import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *
from datetime import datetime

import psycopg2
from datetime import datetime


class StockMon(QAxWidget):
    def __init__(self):
        super().__init__()
        print(">> class StockMon start.")
        
        self.login_event_loop = QEventLoop()
        self.real_event_loop = QEventLoop()
        self.order_event_loop = QEventLoop()
        
        self.account_number = None
#         self.my_stock_list = ["096530","252670","122630","261220"]
        self.my_stock_list = ["096530","252670"]
        self.real_screen_number = "3000"
        self.order_screen_number = "4000"
        
        self.get_ocx_instance()
        self.event_slots()
        self.login_signal()
        
        self.real_event_slots()
        self.real_signal()

    def get_ocx_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def event_slots(self):
        self.OnEventConnect.connect(self.login_slot)
        
    def real_event_slots(self):
        self.OnReceiveRealData.connect(self.real_slot)
        self.OnReceiveChejanData.connect(self.order_slot)

    def login_signal(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_slot(self, error_code):
        print(errors(error_code)[1])
        login_status = self.dynamicCall("GetConnectState()")
        print(">> 로그인 상태(0:연결안됨, 1:연결): %s" % login_status)
        if login_status == "1":
            self.account_number = self.dynamicCall("GetLoginInfo(QString)", "ACCNO").split(";")[0]
            print(self.account_number)
        self.login_event_loop.exit()

    def real_signal(self):
        print(">> real_signal.")
        
        codes = ";".join(self.my_stock_list)
        self.dynamicCall("SetRealReg(QString,QString,QString,QString)", self.real_screen_number, codes, "10;11;12", "0")
        self.real_event_loop.exec_()
        
    def real_slot(self, code, real_type, real_data):
        
        print("\n>> code: %s" % code)
        print(">> real_type: %s" % real_type)
        print(">> real_data: %s" % real_data)
        
        close_price = self.dynamicCall("GetCommRealData(QString,QString)", code, "10").strip()
        
        if int(close_price) < 112800:
            print(">> 거래 대상 종목[%s]: %s 보다 낮은 시장가 주문 시작" % (code,close_price))
            self.order_signal(stock_code=code)
        
        self.real_event_loop.exit()    
    
    def order_signal(self, stock_code=None):
        '''
          SendOrder(
          BSTR sRQName, // 사용자 구분명
          BSTR sScreenNo, // 화면번호
          BSTR sAccNo,  // 계좌번호 10자리
          LONG nOrderType,  // 주문유형 1:신규매수, 2:신규매도 3:매수취소, 4:매도취소, 5:매수정정, 6:매도정정
          BSTR sCode, // 종목코드
          LONG nQty,  // 주문수량
          LONG nPrice, // 주문가격
          BSTR sHogaGb,   // 거래구분(혹은 호가구분)은 아래 참고
          BSTR sOrgOrderNo  // 원주문번호입니다. 신규주문에는 공백, 정정(취소)주문할 원주문번호를 입력합니다.
          )        
        '''
        self.dynamicCall("SendOrder(QString,QString,QString,int,QString,int,int,QString,QString)", 
                         "20200616", self.order_screen_number, self.account_number, 1, stock_code,
                         20, 112800, "03", "")
        self.order_event_loop.exec_()
        
    def order_slot(self, gubun, item_cnt, fid_list):
        
        '''
          BSTR sGubun, // 체결구분 접수와 체결시 '0'값, 국내주식 잔고전달은 '1'값, 파생잔고 전달은 '4'
          LONG nItemCnt,
          BSTR sFIdList        
        '''
        print(">> gubun: %s" % gubun)
        print(">> item_cnt: %s" % item_cnt)
        print(">> fid_list: %s" % fid_list)
        print(">> 주문완료.")
        
        self.order_event_loop.exit()