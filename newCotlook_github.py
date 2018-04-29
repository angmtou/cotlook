# -*- coding: utf-8 -*-


"""
    writen by:     mua
    ver:     1.0
    date:     2017/09
    func：      catch  cotton cotlook data
    data from ： https://www.cotlook.com/information/cotlook-indices/

"""

# 导入出错记录模块
import traceback


import os
import pandas as pd
os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'
import cx_Oracle
from datetime import datetime
from datetime import date

from dateutil.parser import parse


# change to your oracle env ,if not error to oracle
con = cx_Oracle.connect('xx/xx@xx')
cur = con.cursor()
rs=cur.execute('select fac_id,factory from FACTORY t  where fac_id<65012')
li =rs.fetchall()
cotlookcom="https://www.cotlook.com/information/cotlook-indices/"
data3=pd.read_html(io=cotlookcom)

print '-----------------------------BEGIN------------------------------------------------'


pdcot=data3[0][0:2]
c1 =pdcot.iloc[0,3]
c11=c1[10:]
c11par=parse(c11)

pdcot.fillna(0,inplace=True)
t11=pdcot.iloc[0,3]
t11=pd.Series(t11)

t11=t11.str.slice_replace(12,14,'')

t11=unicode(t11)

t11t=unicode(t11[5:])

t11t2=unicode(t11[15:21])

now=datetime.now()
year=now.year
cotime_new=date(datetime.now().year,c11par.month,c11par.day)


cottime = pdcot.iloc[0, 3]



p1 = pdcot.iloc[0, 1]

if pdcot.iloc[1, 2] == 0:
    p2 = int(0)
else:

    p2 = pdcot.iloc[1, 1]
p3 = pdcot.iloc[0, 2]
p4 = pdcot.iloc[1, 2]

print 'changfa', p4




params = {'COTDATE': cotime_new, 'COTLOOKA': p1, 'COTLOOKFA': p2}
print params



# 插入数据库部分：
try:

    cur.execute(
        'insert into ods_cotlook(cotdate,cotlooka,cotlookfa,insertdate)  values(:COTDATE,:COTLOOKA,:COTLOOKFA,sysdate)',
        params)
    con.commit()
    con.close()
    print "Success!"

except cx_Oracle.IntegrityError,e:
    print '\n-------------orcle -error-----------\n'
    print '\n', Exception,":",e,'\n'

    print '数据已存在，不必重复插入\n'

# except IntegrityError,errmsg:
#     print errmsg

except :
    f=open("/u01/cotlook_error.log","a")
    f.write('\n-----------------------------time is---------------------------------------\n')
    f.write("time is %s\n" %now)
    traceback.print_exc(file=f)
    f.flush()
    f.close()


    print '\n\n\n------------------------------------------ERROR HAPPENED--------------------------------\n\n\n'

    print '\n\n\n------------------------------------------WARNING--------------------------------\n\n\n'



print now
print '--------------------------------CLOSE--------------------------------------------------------'