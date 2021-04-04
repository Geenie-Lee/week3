import sys
from PyQt5.QtWidgets import QApplication
from analysis.analysis import Analysis
from analysis.baseinfo import StockBaseInfo
from analysis.elw import EquityLinkedWarrant
from analysis.realinfo import TheReal
from analysis.minute import Minute
from analysis.month import Month
from analysis.week import Week
from analysis.opt30009 import OPT30009


class Main():

    def __init__(self):
        self.app = QApplication(sys.argv)

        # self.anal = Analysis()
        # self.minute = Minute()
        # self.month = Month()
        # self.week = Week()
        # self.base = StockBaseInfo()
#         self.real = TheReal(S)
        self.elw = EquityLinkedWarrant()
#         self.opt30009 = OPT30009()
        self.app.exec_()


if __name__ == "__main__":
    Main()  # 01:analysis, 02:baseinfo