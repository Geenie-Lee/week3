from pywinauto import application
import os, time

# 프로세스 종료 명령 taskkill로 실행 중인 크레온 관련 프로세스(coStarter.exe, CpStart.exe, DibServer.exe)를 종료
# /IM 이미지명이 coStart로 시작하는 프로세를 /F 강제로 /T 종료한다
os.system('taskkill /IM coStarter* /F /T')
os.system('taskkill /IM CpStart* /F /T')
os.system('taskkill /IM DibServer* /F /T')

# wmic(windows management instrumentation command-line)
# 윈도우 시스템 정보를 조회하거나 변경할 때 사용하는 명령
# 크레온 강제 종료 신호를 받으면 확인 창을 띄우기 때문에 강제로 한 번 더 프로그램을 종료
os.system('wmic process where "name like \'%coStarter%\'" call terminate')
os.system('wmic process where "name like \'%CpStart%\'" call terminate')
os.system('wmic process where "name like \'%DibServer%\'" call terminate')

time.sleep(5)
app = application.Application()

# 파이윈오토를 이용하여 크레온 프로그램(coStarter.exe)을 크레온 플러스 모드(/prj:cp)로 자동 시작
app.start('C:\CREON\STARTER\coStarter.exe /prj:cp /id:iamaboy /pwd:u23i45!@ /pwdcert:u23i4523!@ /autostart')
time.sleep(60)