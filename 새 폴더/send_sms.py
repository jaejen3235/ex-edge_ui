import pandas as pd
import pymysql
import os, sys, time
import datetime
import cx_Oracle as cx
cx_conn=cx.connect("sms_app", '!ehfh2019', "194.16.9.3:1656/AIFRS")
cx_cur = cx_conn.cursor()
cx_cur.execute("select seqno from large3data where sendid = 'emfo'")
for c in cx_cur:
        print(c)

conn=pymysql.connect(host='194.16.0.102',port=23306, user='exiot', password='exiotdb123',db='exiot',charset='utf8', cursorclass=pymysql.cursors.DictCursor)
now = datetime.datetime.now()
end_at = now.strftime('%Y-%m-%d %H:%M:%S')
begin_at = now - datetime.timedelta(minutes=1)

begin_at = begin_at.strftime('%Y-%m-%d %H:%M:%S')

print(begin_at)
print(end_at)
query = "select id, sent_at, hq_title, br_title, rt_title, s_id, s_name, send_num, recv_num, event, send_byte, send_type, sent from sms_message where sent_at >= '" + begin_at + "' and sent_at <= '" + end_at +  "' and sent = 0"
cursor = conn.cursor()
cursor.execute(query)
event_list = cursor.fetchall()
df_list = pd.DataFrame(event_list)

for i in range(len(df_list)):
    event_msg = df_list.loc[i, 'event'].split('\n')
    sql_send = ("INSERT INTO LARGE3DATA (SEQNO, INTIME, MSGTYPE, RECVNUM, SENDNUM, SUBJECT, MSG, REQTIME, SENDID) VALUES"
                " ( SEQ_LARGE3DATA.NEXTVAL, TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS'),"
                " 'L',"
                " '"+df_list.loc[i, 'recv_num']+"',"
                " '"+df_list.loc[i, 'send_num']+"',"
                " '교량난간 전도감지 이벤트',"
                " '"+event_msg[0]+"'|| CHR(13) || CHR(10) ||"
                "'"+event_msg[1]+"'|| CHR(13) || CHR(10) ||"
                "'"+event_msg[2]+"'|| CHR(13) || CHR(10) ||"
                "'"+event_msg[3]+"',"
                " TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS'), 'emfo')")
    print(sql_send)
    cx_cur.execute(sql_send)
    cx_conn.commit()

conn.close()
cx_conn.close()
