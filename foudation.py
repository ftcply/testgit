#!/usr/bin/env python
# -*- coding: utf-8 -*- 

import sys
import os
from datetime import *
import datetime
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
import multiprocessing
import time
from time import ctime, sleep
import random
import cx_Oracle
import MySQLdb
import logging as log

#CRITICAL>ERROR>WARNING>INFO>DEBUG>NOTSET
log.basicConfig(level=log.DEBUG,
                format='%(asctime)s pid: %(process)d fn: %(funcName)s %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S',
                filename='x.log',
                filemode='a')

console = log.StreamHandler()
console.setLevel(log.INFO)
formatter = log.Formatter('%(name)-8s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
log.getLogger('').addHandler(console)

class db:
  _db={
       '172':{'host':'172.30.2.172','port': 3308,'user':'ogg','passwd':'ogg','db':'test','charset':'utf8'},
       '175':'ogg/ogg@172.30.2.175:1521/dbopsmt',
       '103':'crm_user/dqa_testic@172.17.3.103:1521/mpay',
       '222':{'host':'172.20.4.222','port': 3306,'user':'ki','passwd':'ki','db':'ki','charset':'utf8'},
       '503':{'host':'10.0.50.3','port': 3308,'user':'root','passwd':'root#1234','db':'xdata_base','charset':'utf8'},
       '14':'XDATA_USER/bigdata_1019@10.0.116.14:1521/mpaybak',
       '106':'ogg/ogg@172.30.2.106:1521/dbopsmt',
       'local':'ogg/ogg@172.20.12.81:1521/xlzx',
       '57':'CRM_USER/dqa_testic@172.17.6.57:1521/mpay',
      }
  _conn = None
  _cur = None
  _dbtype=''
 
  def __init__(self, dbname):
    dbconfig=self._db[dbname]   
    if isinstance(dbconfig,dict):
      self._dbtype='mysql'
    else:
      self._dbtype='oracle'
    if self._dbtype=='mysql':
      self._conn = MySQLdb.connect(host=dbconfig['host'], port=dbconfig['port'], user=dbconfig['user'], passwd=dbconfig['passwd'], db=dbconfig['db'], charset=dbconfig['charset'])
    else:
      self._conn = cx_Oracle.connect(dbconfig)  
    self._cur = self._conn.cursor()

  def query(self,sql):
    result=-1
    try:
      result = self._cur.execute(sql)
    except Exception, e:
      log.error('sql error: %s' % sql)
      log.error('db error: ')
      log.error(e)
    return result

  def close(self):
    self.close()
  
  def getdata_sql(self, sql):
    try:
      self.query(sql)
    except Exception, e:
      log.error('db error')
      log.error(e)
      return []
    rs = self._cur.fetchall()
    return rs

  def getdata(self, str, **kv):
    '''str can be table or sql'''
    where=limit=''
    cols='*'
    if kv.has_key('where'):
      where=kv['where']
    if kv.has_key('cols'):
      cols=','.join(kv['cols'])
    if kv.has_key('limit'):
      n=kv['limit']
      if self._dbtype=='oracle':
        limit='and rownum <= %s' % n
      elif self._dbtype=='mysql':
        limit='limit %s' % n
    sql='select %s from %s where 1=1 %s %s' % (cols, str, where, limit)
    self.query(sql)
    rs = self._cur.fetchall()
    log.info('get %s records, sql:%s' % (len(rs), sql))
    return rs


  def getcols(self,table):
    if table.find('.')<=0:
      log.warning('%s should be like db.table, so return null list' % table)
      return []
    db,tb=table.split('.')
    if self._dbtype=='oracle':
      #_sql="select lower(column_name) from all_tab_columns where table_name=upper('%s') and owner=upper('%s') and lower(data_type) not in ('clob','blob')" % (tb, db)
      _sql="select lower(column_name) from all_tab_columns where table_name=upper('%s') and owner=upper('%s')" % (tb, db)
    else:
      #_sql="select lower(column_name) from information_schema.columns where upper(table_name) = upper('%s') and upper(table_schema)=upper('%s') and lower(data_type) not in ('longblob','longtext')" % (tb, db)  
      _sql="select lower(column_name) from information_schema.columns where upper(table_name) = upper('%s') and upper(table_schema)=upper('%s') " % (tb, db)  
    self.query(_sql)
    _rs=self._cur.fetchall()
    if len(_rs)==0:
      log.warning('can not find cols for table: %s, find sql: %s' % (table, _sql))
      return []
    return [i[0] for i in _rs]
 
  def bulkinsert(self, table, data, c=[]): 
    if len(c)==0:
      d=self.getcols(table)
    else:
      d=c
    cols=','.join(d)
    if self._dbtype=='mysql':
      str_value=','.join("%s" for i in range(len(d)))
      insert_mode='replace'
    else:
      str_value=','.join(":"+str(i+1) for i in range(len(d)))
      insert_mode='insert'
      
    _sql='%s into %s(%s)values(%s)' % (insert_mode, table,cols,str_value)
    log.info('%s records to be insert: %s' % (len(data), _sql))
 
    try:
      n=self._cur.executemany(_sql,data)
    except Exception, e:
      log.error('bulkinsert error')
      log.error(e)
      return 0

    if n==0:
      log.warning('bulkinsert effect 0 records')
      return 0

    log.info("bulkinsert(%s) effect %s records" % (self._dbtype, n))
    self.commit()
    return n 

  def getschema(self, t):
    db, tname=t.upper().split('.')
    if self._dbtype=='oracle':
      sql="select dbms_metadata.get_ddl('TABLE','%s','%s') from dual" % (tname, db)
    elif self._dbtype=='mysql':
      sql='show create table %s' % t
    try:
      rs=self.getdata_sql(sql)
    except Exception, e:
      log.error('getschema error')
      log.error(e)
      return 'can not find schema, maybe no privileges'
    if self._dbtype=='oracle':
      return rs[0][0]
    elif self._dbtype=='mysql':
      return ''.join(rs[0])


  def commit(self):
    self._conn.commit()


if __name__ == '__main__':

  def cols_filter(x,y):
    _t=set(x)-set(y)
    if len(_t)>0:
      log.warning('cols inconsistent, source > distination: %s(%s cols)' % (','.join(_t), len(_t) ) )
    _t=set(y)-set(x)
    if len(_t)>0:
      log.warning('cols inconsistent, source < distination: %s(%s cols)' % (','.join(_t), len(_t) ) )
    log.info('intersection is: %s' % ','.join(set(x)&set(y)))
    return list(set(x)&set(y))
 

  def getsetdata(s,d,ts,td,**kv):
    f=db(s)
    t=db(d)
    cols=[]
    #if kv.has_Key('where'):
    #  where=kv['where']
    #
    if kv.has_key('cols'):
      cols=kv['cols']

    if len(cols)==0:
      x=f.getcols(ts)
      y=t.getcols(td)
      cols=cols_filter(x,y)
      kv['cols']=cols
 
    rs=f.getdata(ts, **kv)
    if not isinstance(rs, list):
      rs=list(rs)
    t.bulkinsert(td, rs, cols)

  #o=db('175')
  #rs=o.getschema('acc_user.t_order_leftpaylist')
  #print rs
  #print '####################'
  #m=db('172')
  #rs=m.getschema('rd_acc_user.t_order_leftpaylist')
  #print rs
  #o=db('103')
  #rs=o.getschema('crm_user.t_bs_user')
  #print rs
  
  getsetdata('175','local','crm_user.t_bs_user','crm_user.t_bs_user',where='and rownum < 10')
