import sys
from PyQt5.QtWidgets import QApplication
from analysis.analysis import Analysis
from analysis.baseinfo import StockBaseInfo
from analysis.realinfo import TheReal

class Main():

    def __init__(self):
        print(">> class Main() start.")
        self.app = QApplication(sys.argv)
        self.anal = Analysis()
#         self.base = StockBaseInfo()
        # self.real = TheReal()
        self.app.exec_()


if __name__ == "__main__":
    Main()  # 01:analysis, 02:baseinfo