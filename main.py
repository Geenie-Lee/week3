import sys
import schedule
import time

from PyQt5.QtWidgets import QApplication
from analysis.analysis import Analysis
from analysis.baseinfo import StockBaseInfo
from analysis.elw import EquityLinkedWarrant
from analysis.realinfo import TheReal
from analysis.minute import Minute
# from analysis.month import Month
from analysis.week import Week
from analysis.opt30009 import OPT30009


class Main():

    def __init__(self):

        tm = time.localtime()
        year = tm.tm_year
        month = tm.tm_mon
        day = tm.tm_mday
        hour = tm.tm_hour
        minute = tm.tm_min
        second = tm.tm_sec

        print("-------------------------------------------------------------------------------")
        print(">> Current Time : " + str(year) + "-" + str(month) + "-" + str(day) + " " + str(hour) + ":" + str(minute) + ":" + str(second))
        print("-------------------------------------------------------------------------------")

        self.app = QApplication(sys.argv)

        self.anal = Analysis()
        # self.minute = Minute()
        # self.month = Month()
        # self.week = Week()
        # self.base = StockBaseInfo()
        #         self.real = TheReal(S)
        #         self.elw = EquityLinkedWarrant()
        #         self.opt30009 = OPT30009()
        self.app.exec_()

# schedule.every(5).minutes.do(Main())
#
# while True:
#     schedule.run_pending()
#     time.sleep(1)


if __name__ == "__main__":
    Main()  # 01:analysis, 02:baseinfo
