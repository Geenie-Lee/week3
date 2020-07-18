from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
from PyQt5.QtTest import QTest
from config.errorCode import *
from config.kiwoomType import *

class TheReal(QAxWidget):

    def __init__(self):
        super().__init__()
        print(">> class TheReal() Start.")

        self.realType = RealType()
        self.real_screen_number = "1000"
        self.login_event_loop = QEventLoop()

        self.get_ocx_instance()
        self.event_slots()
        self.real_event_slot()
        self.login_signal()
        self.real_signal()

    def get_ocx_instance(self):
        self.setControl("KHOPENAPI.KHOpenAPICtrl.1")

    def event_slots(self):
        self.OnEventConnect.connect(self.login_slot)

    def real_event_slot(self):
        self.OnReceiveRealData.connect(self.realdata_slot)

    def login_signal(self):
        self.dynamicCall("CommConnect()")
        self.login_event_loop.exec_()

    def login_slot(self, error_code):
        print(errors(error_code)[1])
        login_status = self.dynamicCall("GetConnectState()")
        print(">> 로그인 상태(0:연결안됨, 1:연결): %s" % login_status)
        self.login_event_loop.exit()

    def real_signal(self):
        print(">>>>> real_signal %s" % self.realType.REAL_TYPE["장시작시간"]["장운영구분"])
        value = self.dynamicCall("SetRealReg(QString,QString,QString,QString)", self.real_screen_number, ' ', self.realType.REAL_TYPE["장시작시간"]["장운영구분"], "0")
        print(value)

    def realdata_slot(self, stock_code, real_type, real_data):
        print(">> realdata_slot start.")
        print(">> stock_code: [%s]" % stock_code)
        print(">> real_type: [%s]" % real_type)
        print(">> real_data: [%s]" % real_data)

        value = self.dynamicCall("GetCommRealData(QString,int)", stock_code, self.realType.REAL_TYPE[real_type]["장운영구분"])
        print(">> value: %" % value)


if __name__ == "__main__":
    TheReal()
