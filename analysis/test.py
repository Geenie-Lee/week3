from PyQt5.QtWidgets import QApplication
from PyQt5.QAxContainer import QAxWidget
import sys

class Test(QAxWidget):

    def __init__(self):
        self.app = QApplication(sys.argv)
        super().__init__()
        print("class StockBaseInfo() Start.")
        self.app.exec_()


if __name__ == "__main__":
    Test()
