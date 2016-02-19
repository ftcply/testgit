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
       '172':{'host':'172.20.20.172','port': 3308,'user':'ogg','passwd':'ogg','db':'test','charset':'utf8'},
       '175':'ogg/ogg@172.20.20.175:1521/dbopsmt',
       '222':{'host':'172.20.4.222','port': 3306,'user':'ki','passwd':'ki','db':'ki','charset':'utf8'},
       '503':{'host':'10.0.50.3','port': 3308,'user':'root','passwd':'root#1234','db':'xdata_base','charset':'utf8'},
       '14':'XDATA_USER/bigdata_1019@10.0.116.14:1521/mpaybak',
       '106':'ogg/ogg@172.30.2.106:1521/dbopsmt',
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
      print 'sql error:', sql
      print 'db error: ', e
    return result

  def close(self):
    self.close()
  

  def getdata(self, str, where=''):
    '''str can be table or sql'''
    sql=self.getsql(str,where)
    log.info(sql)
    self.query(sql)
    rs = self._cur.fetchall()  
    log.info('get %s records, sql:%s' % (len(rs), sql))
    return rs

  def getsql(self, str, where):
    '''getsql'''
    if len(str.split(' ')) > 1:
      _sql=str
    else:
      _sql='select * from %s where 1=1 %s' % (str,where)
    return _sql

  def getcols(self,table):
    if table.find('.')<=0:
      log.warning('%s should be like db.table, so return null list' % table)
      return []
    db,tb=table.split('.')
    if self._dbtype=='oracle':
      _sql="select lower(column_name) from all_tab_columns where table_name=upper('%s') and owner=upper('%s')" % (tb, db)
    else:
      _sql="select lower(column_name) from information_schema.columns where upper(table_name) = upper('%s') and upper(table_schema)=upper('%s') " % (tb, db)  
    _rs=self.getdata(_sql)
    if len(_rs)==0:
      log.warning('can not find cols for table: %s, find sql: %s' % (table, _sql))
      return []
    return [i[0] for i in _rs]
 
  def bulkinsert(self, table, data): 
    d=self.getcols(table)
    cols=','.join(d)
    if self._dbtype=='mysql':
      str_value=','.join("%s" for i in range(len(d)))
    else:
      str_value=','.join(":"+str(i+1) for i in range(len(d)))
      
    _sql='replace into %s(%s)values(%s);' % (table,cols,str_value)

    try:
      n=self._cur.executemany(_sql,data)
    except Exception, e:
      log.error('bulkinsert error')
      log.error(e)

    if n==0:
      log.warning('bulkinsert effect 0 records')

    log.info("%s bulkinsert sql: %s effect %s records" % (self._dbtype, _sql, n))
    self.commit()
    return n 

  def commit(self):
    self._conn.commit()


if __name__ == '__main__':

  def getsetdata(conn_s, conn_d, tb_s, tb_d, where=''):
    start=time.time()
    '''from oracle to mysql'''
    log.info('begin from %s to %s' % (tb_s,tb_d))
    cs=db(conn_s)
    cd=db(conn_d)
 
    #检查两边字段是否一致
    scol=cs.getcols(tb_s)
    if len(scol)==0:
      return '%s no cols' % tb_s
    dcol=cd.getcols(tb_d)
    if len(dcol)==0:
      return '%s no cols' % tb_d
    #print 'source: %s \ndestination: %s' % (scol, dcol)
    if check(scol, dcol)!=0:
      log.error('cols inconsistent: source: %s, destination: %s' % (scol, dcol))
      return 'table cols inconsistent'
 
    rs=[]

    sum=0
    if '@' in where:
      loadid = where.lstrip('@')
      _maxid = cd.getdata('select ifnull(min(%s),0), ifnull(max(%s),0) from %s' % (loadid, loadid,  tb_d))
      _maxid2 = cs.getdata('select coalesce(min(%s),0), coalesce(max(%s),0) from %s' % (loadid, loadid, tb_s))
      mid= _maxid[0][1]
      bid = _maxid2[0][1]

      if '@@' in where:

        log.info('全量同步%s' % tb_s)
        mid = _maxid2[0][0]-1 #最小的id就是1，后面 beg 会加1, 为了保证从最小开始
        print 'mid',mid
   
      
      log.info('%s max %s: %s, %s max %s: %s' % (tb_s, loadid, mid, tb_d, loadid, bid))

      #每次操作50000条
      step=2
      if bid-mid>0:
        if bid-mid>step: 
          log.info('分%s次操作' % ((bid-mid)/step))
        while 1:
          if mid >= bid:
            log.info('done')
            break
          beg, end= mid+1, mid+step+1
          log.info('本次拉取 [%s, %s) 的数据' % (beg, end))
          _where='and %s >= %s and %s < %s' % (loadid, beg, loadid, end)

          rs=cs.getdata(tb_s, _where)
    
          if len(rs)==0:
            _msg='%s no data' % tb_s
            log.warning(_msg)
            return _msg
    
          tocsv(rs, '123.log')
        
          x=cd.bulkinsert(tb_d, rs)
          sum+=x

          if x==0:
            log.warning('unknown error, no effect records')
            break
          mid+=step
      else:
         log.info('%s 已经和 %s 的最大id一致: %s' % (tb_s, tb_d, bid))

        
    else:
      if where.find('|')>0:
        '''对LPL_TRANS_TIME|dt这样格式的作特殊处理'''
        dtkey,dt=where.split('|')
        where="and %s >= to_date('%s','yyyy-mm-dd') and %s < to_date('%s','yyyy-mm-dd')+1" % (dtkey, dt, dtkey, dt)

      rs=cs.getdata(tb_s, where)
      if len(rs)==0:
        log.warning('not data')
        return 'not data'

      sum=cd.bulkinsert(tb_d, rs)  

    end=time.time()
    log.info('effect records: %s, spend time:%ss |%s' % (sum, int(end - start), tb_d))
    return '%s OK, %s records, %ss' % (tb_d, sum, int(end - start))


  def tocsv(data, file=None):
    if(file is None):
      file='tmp_%s' % datetime.now().strftime('%Y%m%d%H%M%S')
    import csv
    f = open(file, "w")
    writer = csv.writer(f, lineterminator="\n", quoting=1)
    for i in data:
      writer.writerow(i)
    f.close()


  def check(la, lb):
    flag=0
    for i in la:
      if i not in lb:
        flag+=1
    if flag>0:
      return -1
    for i in lb:
      if i not in la:
        flag+=1
    if flag>0:
      return -1
    else:
      return 0

  def testone(conn):
    try:
      c=db(conn)
      return '%s is ok [%s]' % (conn, c._db[conn])
    except Exception,e:
      return '%s is not ok' % (conn)

  def test(str):
    l=str.split(',')
    return map(testone, l)
  

  def opts():
    opts = { 'table':'', 'dt' :'', 'test':''}
    i = 1;
    while (i < len(sys.argv)):
      arg = sys.argv[i]
      if arg == '-t':
        opts['table'] = sys.argv[i+1];
      elif arg == '-d':
        opts['dt'] = sys.argv[i+1];
      elif arg == '--test':
        opts['test'] = sys.argv[i+1];
      else:
        print 'Unknow argument : %s' % (sys.argv[i]);
        i += 1;
        continue;
      i += 2;
    return opts

  result=[]
  start=time.time()

  args = opts()

  if len(args['test'])>0:
    test_rs=test(args['test'])
    for i in test_rs:
      print i
    exit('test end')

  #如果指定了时间，就用这个时间,默认前一天
  if len(args['dt'])>0:
    dt=args['dt']
  else:
    dt=(datetime.date.today()-datetime.timedelta(days=1)).strftime('%Y-%m-%d')

  tables={
#'t_putmachine':{'ts':'xdata_user.t_putmachine','td':'rd_crm_user.t_putmachine','where':''},
#'t_accinfo':{'ts':'xdata_user.t_accinfo','td':'rd_crm_user.t_accinfo','where':''},
#'t_accinfo_account':{'ts':'xdata_user.t_accinfo_account','td':'rd_crm_user.t_accinfo_account','where':''},
#'t_account':{'ts':'xdata_user.t_account','td':'rd_crm_user.t_account','where':''},
#'t_account_cert':{'ts':'xdata_user.t_account_cert','td':'rd_crm_user.t_account_cert','where':''},
#'t_agent_account':{'ts':'xdata_user.t_agent_account','td':'rd_crm_user.t_agent_account','where':''},
#'t_dev_account':{'ts':'xdata_user.t_dev_account','td':'rd_crm_user.t_dev_account','where':''},
#'t_merchant_terminal':{'ts':'xdata_user.t_merchant_terminal','td':'rd_crm_user.t_merchant_terminal','where':''},
#'t_terminal_param':{'ts':'xdata_user.t_terminal_param','td':'rd_crm_user.t_terminal_param','where':''},
#'t_terminal_property':{'ts':'xdata_user.t_terminal_property','td':'rd_crm_user.t_terminal_property','where':''},
#'t_business_batch':{'ts':'xdata_user.t_business_batch','td':'rd_crm_user.t_business_batch','where':''},
#'t_mercertificates':{'ts':'xdata_user.t_mercertificates','td':'rd_crm_user.t_mercertificates','where':''},
#'t_merchantno':{'ts':'xdata_user.t_merchantno','td':'rd_crm_user.t_merchantno','where':''},
#'t_order':{'ts':'xdata_user.t_order','td':'rd_crm_user.t_order','where':''},
#'t_bs_user':{'ts':'xdata_user.t_bs_user','td':'rd_crm_user.t_bs_user','where':''},
#'t_busiman':{'ts':'xdata_user.t_busiman','td':'rd_crm_user.t_busiman','where':''},
#'t_comm_region':{'ts':'xdata_user.t_comm_region','td':'rd_crm_user.t_comm_region','where':''},
#'t_mcc':{'ts':'crm_user.t_mcc','td':'rd_crm_user.t_mcc','where':''},
#'t_mer_lease_contract':{'ts':'xdata_user.t_mer_lease_contract','td':'rd_crm_user.t_mer_lease_contract','where':''},
#'t_terminal_applications':{'ts':'xdata_user.t_terminal_applications','td':'rd_crm_user.t_terminal_applications','where':''},
#'t_trade_account':{'ts':'xdata_user.t_trade_account','td':'rd_crm_user.t_trade_account','where':''},
#'t_trade_feerate':{'ts':'xdata_user.t_trade_feerate','td':'rd_crm_user.t_trade_feerate','where':''},
#'t_system_feerate_route':{'ts':'xdata_user.t_system_feerate_route','td':'rd_crm_user.t_system_feerate_route','where':''},
#'adj_bill':{'ts':'xdata_user.adj_bill','td':'rd_acc_user.adj_bill','where':'operdate|'+dt},
#'postrade':{'ts':'xdata_user.postrade','td':'rd_acc_user.postrade','where':'systrdtime|'+dt},
##'t_adjust_account_detail':{'ts':'xdata_user.t_adjust_account_detail','td':'rd_acc_user.t_adjust_account_detail','where':'ori_cleardate|'+dt},
#'t_adjust_account_detail':{'ts':'xdata_user.t_adjust_account_detail','td':'rd_acc_user.t_adjust_account_detail','where':''}, #截至20160607才8000+条
#'t_order_main':{'ts':'xdata_user.t_order_main','td':'rd_acc_user.t_order_main','where':'trans_time|'+dt},
#'trdfat_profit':{'ts':'xdata_user.trdfat_profit','td':'rd_acc_user.trdfat_profit','where':'@autoid'},
#'trdfat_recon':{'ts':'xdata_user.trdfat_recon','td':'rd_acc_user.trdfat_recon','where':'@autoid'},
#'tradejour':{'ts':'xdata_user.tradejour','td':'rd_acc_user.tradejour','where':'reckdate|'+dt},
#'t_order_extendpaylist':{'ts':'xdata_user.t_order_extendpaylist','td':'rd_acc_user.t_order_extendpaylist','where':'tran_time|'+dt},
#'t_order_leftpaylist':{'ts':'xdata_user.t_order_leftpaylist','td':'rd_acc_user.t_order_leftpaylist','where':'lpl_trans_time|'+dt},
#'t_order_rightpaylist':{'ts':'xdata_user.t_order_rightpaylist','td':'rd_acc_user.t_order_rightpaylist','where':'lpl_trans_time|'+dt},
#'t_r_tradelist':{'ts':'xdata_user.t_r_tradelist','td':'rd_nac_user.t_r_tradelist','where':'rtl_time|'+dt},
#'t_l_tradelist':{'ts':'xdata_user.t_l_tradelist','td':'rd_nac_user.t_l_tradelist','where':'ltl_time|'+dt},
#'t_bank_card':{'ts':'xdata_user.t_bank_card','td':'rd_nac_user.t_bank_card','where':''},
#'pytest':{'ts':'crm_user.pytest','td':'test.pytest','where':'@@userid'},

't_putmachine':{'ts':'crm_user.t_putmachine','td':'rd_crm_user.t_putmachine','where':''},
't_accinfo':{'ts':'crm_user.t_accinfo','td':'rd_crm_user.t_accinfo','where':''},
't_accinfo_account':{'ts':'crm_user.t_accinfo_account','td':'rd_crm_user.t_accinfo_account','where':''},
't_account':{'ts':'crm_user.t_account','td':'rd_crm_user.t_account','where':''},
't_account_cert':{'ts':'crm_user.t_account_cert','td':'rd_crm_user.t_account_cert','where':''},
't_agent_account':{'ts':'crm_user.t_agent_account','td':'rd_crm_user.t_agent_account','where':''},
't_dev_account':{'ts':'crm_user.t_dev_account','td':'rd_crm_user.t_dev_account','where':''},
't_merchant_terminal':{'ts':'crm_user.t_merchant_terminal','td':'rd_crm_user.t_merchant_terminal','where':''},
't_terminal_param':{'ts':'crm_user.t_terminal_param','td':'rd_crm_user.t_terminal_param','where':''},
't_terminal_property':{'ts':'crm_user.t_terminal_property','td':'rd_crm_user.t_terminal_property','where':''},
't_business_batch':{'ts':'crm_user.t_business_batch','td':'rd_crm_user.t_business_batch','where':''},
't_mercertificates':{'ts':'crm_user.t_mercertificates','td':'rd_crm_user.t_mercertificates','where':''},
't_merchantno':{'ts':'crm_user.t_merchantno','td':'rd_crm_user.t_merchantno','where':''},
't_order':{'ts':'crm_user.t_order','td':'rd_crm_user.t_order','where':''},
't_bs_user':{'ts':'crm_user.t_bs_user','td':'rd_crm_user.t_bs_user','where':''},
't_busiman':{'ts':'crm_user.t_busiman','td':'rd_crm_user.t_busiman','where':''},
't_comm_region':{'ts':'crm_user.t_comm_region','td':'rd_crm_user.t_comm_region','where':''},
't_mcc':{'ts':'crm_user.t_mcc','td':'rd_crm_user.t_mcc','where':''},
't_mer_lease_contract':{'ts':'crm_user.t_mer_lease_contract','td':'rd_crm_user.t_mer_lease_contract','where':''},
't_terminal_applications':{'ts':'crm_user.t_terminal_applications','td':'rd_crm_user.t_terminal_applications','where':''},
't_trade_account':{'ts':'crm_user.t_trade_account','td':'rd_crm_user.t_trade_account','where':''},
't_trade_feerate':{'ts':'crm_user.t_trade_feerate','td':'rd_crm_user.t_trade_feerate','where':''},
't_system_feerate_route':{'ts':'crm_user.t_system_feerate_route','td':'rd_crm_user.t_system_feerate_route','where':''},
'adj_bill':{'ts':'acc_user.adj_bill','td':'rd_acc_user.adj_bill','where':'operdate|'+dt},
'postrade':{'ts':'acc_user.postrade','td':'rd_acc_user.postrade','where':'systrdtime|'+dt},
't_adjust_account_detail':{'ts':'acc_user.t_adjust_account_detail','td':'rd_acc_user.t_adjust_account_detail','where':'ori_cleardate|'+dt},
't_order_main':{'ts':'acc_user.t_order_main','td':'rd_acc_user.t_order_main','where':'trans_time|'+dt},
'trdfat_profit':{'ts':'acc_user.trdfat_profit','td':'rd_acc_user.trdfat_profit','where':'@autoid'},
'trdfat_recon':{'ts':'acc_user.trdfat_recon','td':'rd_acc_user.trdfat_recon','where':'@autoid'},
'tradejour':{'ts':'acc_user.tradejour','td':'rd_acc_user.tradejour','where':'reckdate|'+dt},
't_order_extendpaylist':{'ts':'acc_user.t_order_extendpaylist','td':'rd_acc_user.t_order_extendpaylist','where':'tran_time|'+dt},
't_order_leftpaylist':{'ts':'acc_user.t_order_leftpaylist','td':'rd_acc_user.t_order_leftpaylist','where':'lpl_trans_time|'+dt},
't_order_rightpaylist':{'ts':'acc_user.t_order_rightpaylist','td':'rd_acc_user.t_order_rightpaylist','where':'lpl_trans_time|'+dt},
't_r_tradelist':{'ts':'nac_user.t_r_tradelist','td':'rd_nac_user.t_r_tradelist','where':'rtl_time|'+dt},
't_l_tradelist':{'ts':'nac_user.t_l_tradelist','td':'rd_nac_user.t_l_tradelist','where':'ltl_time|'+dt},
't_bank_card':{'ts':'nac_user.t_bank_card','td':'rd_nac_user.t_bank_card','where':''},
'pytest':{'ts':'crm_user.pytest','td':'test.pytest','where':'@userid'}
        }

  #如果指定了表，就只跑这些表
  if len(args['table'])>0:
    _d={}
    _key=args['table'].split(',')
    for i in _key:
      _d[i]=tables[i]
    tables=_d


  #暂时将进程池设为10
  max=10
  pro=len(tables)
  if pro > max:
    pro=max

  pool = multiprocessing.Pool(processes=pro)
  for i in tables:
    #result.append(pool.apply_async(getsetdata, ('175','222',tables[i]['ts'],tables[i]['td'],tables[i]['where'])))
    result.append(pool.apply_async(getsetdata, ('175','222',tables[i]['ts'],tables[i]['td'],'and rownum<10')))  #测试用

  pool.close()
  pool.join()
  for res in result:
    print ':::', res.get()
  
  end=time.time()
  print "All process(es) done."
  print 'total spend time: %ss' % (int(end - start))

