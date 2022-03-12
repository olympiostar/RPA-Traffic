# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 09:28:10 2021
@author: Jayden
"""

import cx_Oracle
import pymysql
import os
from datetime import date, timedelta

cm_dev_list=[]
h_data_list=[]
d_data_list=[]
m_data_list=[]

y_date = date.today() - timedelta(1)
y_date = y_date.strftime('%Y%m%d')

class cm_dev:
    seq = 0
    net_type = ""
    grp = 0
    notice = ""
    mng_no = 0
    if_idx = 0
    bw = 0
    
    def __init__(self, seq, net_type, grp, notice, mng_no, if_idx, bw):
        self.seq = seq
        self.net_type = net_type
        self.grp = grp
        self.notice = notice
        self.mng_no = mng_no
        self.if_idx = if_idx
        self.bw = bw       
        
class H_data:
    YYYYMMDD = ""
    HHMMSS = ""
    seq = 0
    net_type = ""
    grp = 0
    notice = ""
    mng_no = 0
    if_idx = 0
    bw = 0       
    MAX_INBPS = 0
    MAX_OUTBPS = 0        
    MAX_INPPS = 0        
    MAX_OUTPPS = 0
    
    def __init__(self, YYYYMMDD, HHMMSS, seq, net_type, grp, notice, mng_no, if_idx, bw,
                 MAX_INBPS, MAX_OUTBPS, MAX_INPPS, MAX_OUTPPS):
        self.YYYYMMDD = YYYYMMDD
        self.HHMMSS = HHMMSS
        self.seq = seq
        self.net_type = net_type
        self.grp = grp
        self.notice = notice
        self.mng_no = mng_no
        self.if_idx = if_idx
        self.bw = bw    
        self.MAX_INBPS = MAX_INBPS        
        self.MAX_OUTBPS = MAX_OUTBPS
        self.MAX_INPPS = MAX_INPPS
        self.MAX_OUTPPS = MAX_OUTPPS
    

class d_data:        
    YYYYMMDD = ""
    seq = 0
    net_type = ""
    grp = 0
    notice = ""
    mng_no = 0
    if_idx = 0
    bw = 0       
    MAX_INBPS = 0
    MAX_OUTBPS = 0        
    MAX_INPPS = 0        
    MAX_OUTPPS = 0
    
    def __init__(self, YYYYMMDD, seq, net_type, grp, notice, mng_no, if_idx, bw,
                 MAX_INBPS, MAX_OUTBPS, MAX_INPPS, MAX_OUTPPS):
        self.YYYYMMDD = YYYYMMDD
        self.seq = seq
        self.net_type = net_type
        self.grp = grp
        self.notice = notice
        self.mng_no = mng_no
        self.if_idx = if_idx
        self.bw = bw    
        self.MAX_INBPS = MAX_INBPS        
        self.MAX_OUTBPS = MAX_OUTBPS
        self.MAX_INPPS = MAX_INPPS
        self.MAX_OUTPPS = MAX_OUTPPS    
    
class m_data:
    YYYYMM = ""
    seq = 0
    net_type = ""
    grp = 0
    notice = ""
    mng_no = 0
    if_idx = 0
    bw = 0       
    MAX_INBPS = 0
    MAX_OUTBPS = 0        
    MAX_INPPS = 0        
    MAX_OUTPPS = 0
    
    def __init__(self, YYYYMM, seq, net_type, grp, notice, mng_no, if_idx, bw,
                 MAX_INBPS, MAX_OUTBPS, MAX_INPPS, MAX_OUTPPS):
        self.YYYYMM = YYYYMM
        self.seq = seq
        self.net_type = net_type
        self.grp = grp
        self.notice = notice
        self.mng_no = mng_no
        self.if_idx = if_idx
        self.bw = bw    
        self.MAX_INBPS = MAX_INBPS        
        self.MAX_OUTBPS = MAX_OUTBPS
        self.MAX_INPPS = MAX_INPPS
        self.MAX_OUTPPS = MAX_OUTPPS    
    
    
    
    
    

def load_cm_dev_list():
    # MySQL Connection
    conn = pymysql.connect((Skip caused by Security))                
    # Connection으로부터 Cursor 생성
    curs = conn.cursor()
    
    #SQL문 실행
    query = (Skip caused by Security)
    curs.execute(query)
    
    result_check = curs.fetchone()                                       
    curs.rownumber=0
       
    #SQL Query 결과가 없다면
    if not result_check:
        os.system('cls')
        print("Query 실행 결과 없음 : (Skip caused by Security)")
    #SQL Query결과가 있다면                    
    else:
        #SQL Query 결과 조회
        for row in curs:   
            dev = cm_dev(row[0], row[1], row[2], row[3], row[4], row[5], row[6])                        
            cm_dev_list.append(dev)

    conn.commit()
    curs.close()
    


def make_hourly_table():
    LOCATION = r"C:\instantclient_11_2"
    os.environ["PATH"] = LOCATION + ";" + os.environ["PATH"]
    ### DB connect ###
    dsn = cx_Oracle.makedsn((Skip caused by Security))
    conn = cx_Oracle.connect((Skip caused by Security),dsn)
    # MySQL Connection
    conn_x = pymysql.connect((Skip caused by Security))                
    
    # Connection으로부터 Cursor 생성
    curs = conn.cursor()    
    curs_x = conn_x.cursor()


    #SQL문 실행
    query =(Skip caused by Security)
    curs_x.execute(query)

    #반복문 cm_dev_list 내 객체로 반복문    
    for dev in cm_dev_list :
        
        #SQL문 실행 : WHERE : 어제 날짜(YYYYMMDD) + mng_id + if_idx    
        query = (Skip caused by Security)
        curs.execute(query)
    
        for row in curs:   
            h = H_data(row[0], row[1], dev.seq, dev.net_type, dev.grp, dev.notice, dev.mng_no, dev.if_idx, dev.bw, row[6], row[9], row[12], row[15])
            h_data_list.append(h)
        
    
    #SQL문 실행 결과 : RPA horuly table에 저장    
    for h in h_data_list :
        query = "INSERT INTO (Skip caused by Security)"
        curs_x.execute(query)
        #h_data_list.clear()
    
    
    conn.commit()
    conn_x.commit()
        
    curs.close()
    curs_x.close()   

def make_daily_table():   
    # MySQL Connection
    conn = pymysql.connect((Skip caused by Security))                
    # Connection으로부터 Cursor 생성
    curs= conn.cursor()
    
    #SQL문 실행
    query = (Skip caused by Security)
    curs.execute(query)
    
    #h_data에서 24개 씩 중 MAX IN OUT BPS/PPS -> d_data로 만들기 ->insert
    h_count = 0
    temp_MAX_INBPS_list = []
    temp_MAX_OUTBPS_list = []
    temp_MAX_INPPS_list = []
    temp_MAX_OUTPPS_list = []
    
    for h in h_data_list :
        h_count=h_count+1
        temp_MAX_INBPS_list.append(h.MAX_INBPS)
        temp_MAX_OUTBPS_list.append(h.MAX_OUTBPS)
        temp_MAX_INPPS_list.append(h.MAX_INPPS)
        temp_MAX_OUTPPS_list.append(h.MAX_OUTPPS)               
        print(h_count)
        if(h_count >= 24) :            
            temp_MAX_INBPS_list.sort(reverse=True)
            temp_MAX_OUTBPS_list.sort(reverse=True)
            temp_MAX_INPPS_list.sort(reverse=True)
            temp_MAX_OUTPPS_list.sort(reverse=True)
            
            D_MAX_INBPS = temp_MAX_INBPS_list[2]
            D_MAX_OUTBPS = temp_MAX_OUTBPS_list[2]
            D_MAX_INPPS = temp_MAX_INPPS_list[2]
            D_MAX_OUTPPS = temp_MAX_OUTPPS_list[2]
            
            d = d_data(h.YYYYMMDD, h.seq, h.net_type, h.grp, h.notice, h.mng_no, h.if_idx, h.bw, \
                       D_MAX_INBPS, D_MAX_OUTBPS, D_MAX_INPPS, D_MAX_OUTPPS)
            d_data_list.append(d)
            
            temp_MAX_INBPS_list.clear()
            temp_MAX_OUTBPS_list.clear()
            temp_MAX_INPPS_list.clear()
            temp_MAX_OUTPPS_list.clear()
            h_count = 0
        
        

    #SQL문 실행 결과 : RPA daily table에 저장    
    for d in d_data_list :
        query = "INSERT INTO (Skip caused by Security)
        curs_x.execute(query)    
    
    h_data_list.clear()    
    d_data_list.clear()    
    conn_x.commit()
    curs_x.close()    



def make_monthly_table():
    n_date = date.today()
    n_date = n_date.strftime('%Y%m%d')
    n_date = n_date[6:8]
    #d_data 30~ 개 씩중 MAX IN OUT BPS/PPS -> m_data로 만들기 -> insert
    if (n_date != "01"):
        print("금월 1일에만 전월 데이터를 DB에 적재합니다")
    else:
        # MySQL Connection
        conn_x = pymysql.connect((Skip caused by Security))                
        # Connection으로부터 Cursor 생성
        curs_x = conn_x.cursor()
        
        #SQL문 실행
        query = (Skip caused by Security)
        curs_x.execute(query)
       
        for dev in cm_dev_list :
            for i in range (1,40) :
               d_date = date.today() - timedelta(i)
               d_date = d_date.strftime('%Y%m%d')    
               l_month_date = int(d_date[6:8])
               
               #SQL문 실행 : WHERE : 어제 날짜(YYYYMMDD) + mng_id + if_idx    
               query = (Skip caused by Security)
               curs_x.execute(query)
            
               for row in curs_x:   
                   d = d_data(row[0], dev.seq, dev.net_type, dev.grp, dev.notice, dev.mng_no, dev.if_idx, dev.bw, row[8], row[9], row[10], row[11])
                   d_data_list.append(d)
                   
               if(l_month_date == "01"):
                   break;                   
        
        temp_MAX_INBPS_list = []
        temp_MAX_OUTBPS_list = []
        temp_MAX_INPPS_list = []
        temp_MAX_OUTPPS_list = []
    
        for d in d_data_list:                
           temp_MAX_INBPS_list.append(d.MAX_INBPS)
           temp_MAX_OUTBPS_list.append(d.MAX_OUTBPS)
           temp_MAX_INPPS_list.append(d.MAX_INPPS)
           temp_MAX_OUTPPS_list.append(d.MAX_OUTPPS)
           
           if(d.YYYYMMDD[6:8] == "01"):
               temp_MAX_INBPS_list.sort(reverse=True)
               temp_MAX_OUTBPS_list.sort(reverse=True)
               temp_MAX_INPPS_list.sort(reverse=True)
               temp_MAX_OUTPPS_list.sort(reverse=True)
               M_MAX_INBPS = temp_MAX_INBPS_list[0]
               M_MAX_OUTBPS = temp_MAX_OUTBPS_list[0]
               M_MAX_INPPS = temp_MAX_INPPS_list[0]
               M_MAX_OUTPPS = temp_MAX_OUTPPS_list[0]
               m = m_data(d.YYYYMMDD[0:6], d.seq, d.net_type, d.grp, d.notice, d.mng_no, d.if_idx, d.bw, \
                           M_MAX_INBPS, M_MAX_OUTBPS, M_MAX_INPPS, M_MAX_OUTPPS)
               m_data_list.append(m)     
               temp_MAX_INBPS_list.clear()
               temp_MAX_OUTBPS_list.clear()
               temp_MAX_INPPS_list.clear()
               temp_MAX_OUTPPS_list.clear()
               
    
        #SQL문 실행 결과 : 저장    
        for m in m_data_list :
            query = "INSERT INTO (Skip caused by Security)"
            curs_x.execute(query)
        d_data_list.clear()            

    
load_cm_dev_list
make_hourly_table
make_daily_table
make_monthly_table
