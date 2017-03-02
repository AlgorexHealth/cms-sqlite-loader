import os.path
import sys
import time
import subprocess
import sqlite3
import csv

debug = False

def ensure_db(the_database,ddlfile):
  conn = sqlite3.connect(the_database)
  with open (ddlfile, "r") as myfile:
      ddlstatements=myfile.read()
      conn.executescript(ddlstatements)
  conn.commit()
  conn.close()


def get_file_values(the_file):
  filename = os.path.basename(the_file)
  filesize = os.path.getsize(the_file)
  md5B = subprocess.check_output("md5sum " + the_file,shell=True)
  md5, *rest  = md5B.decode("utf-8").split(" ")
  epoch = int(time.time())
  directory = os.path.abspath(the_file)
  return filename,filesize,md5,epoch,directory


def load_generic(f,skipLines,sqlFuncs,the_database,the_ddl):
  def runner():
    if not os.path.exists(the_database):
      ensure_db(the_database,the_ddl)
    with open(f) as csvfile:
      csvReader = csv.reader(csvfile)
      conn = sqlite3.connect( the_database )
      cur = conn.cursor()
      cur.execute('BEGIN TRANSACTION')
      file_values = get_file_values(f)
      sql_for_batch = "INSERT OR IGNORE INTO batch \
              (filename,filesize,md5,ingest_start_time,directory) \
              VALUES (?,?,?,?,?)"
      cur.execute(sql_for_batch,file_values)
      batchid = cur.lastrowid
      for _ in range(skipLines):
        next(csvReader)
      rownumber = 1
      for row in csvReader:
        if (rownumber % 1000) == 0:
          sys.stdout.write('.')
        for sql,proxyFunc in sqlFuncs:
          for newrow in proxyFunc(row,  batchid ):
            cur.execute(sql, newrow)
        rownumber += 1
        conn.commit()
      conn.close()
  return runner

def load_carrier(the_file,the_ddl=None,the_database =None ):
  print("running load_carrier() with ", the_file)
  if the_database is None:
    file_first,_ = os.path.basename(the_file).split(".")  
    the_database = file_first + ".db"
  sql_for_main_claim = "INSERT OR IGNORE INTO carrier_claim (\
              DESYNPUF_ID  ,\
              CLM_ID  ,     \
              CLM_FROM_DT  ,\
              CLM_THRU_DT  ,\
              BatchId  )   \
           VALUES (?,?,?,?,?)"
  def claim_row(row,batchid):
    colsToKeep = [row[i] for i in [0,1,2,3]]
    yield colsToKeep + [batchid]
  sql_for_diag = "INSERT OR IGNORE INTO carrier_diagnosis (\
                      ICD9_DGNS_CD , \
                      numeric_postfix  , \
                      carrier_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_diags(row,batchid):
    colsToKeep = [row[1]]
    for i in range(5,12+1):
      if row[i]:
        yield [row[i]] + [i] + colsToKeep 
  sql_for_line = """INSERT OR IGNORE INTO line_item (
                    PRF_PHYSN_NPI  ,
                    TAX_NUM  ,
                    HCPCS_CD  ,
                    LINE_NCH_PMT_AMT  ,
                    LINE_BENE_PTB_DDCTBL_AMT  ,
                    LINE_BENE_PRMRY_PYR_PD_AMT  ,
                    LINE_COINSRNC_AMT  ,
                    LINE_ALOWD_CHRG_AMT  ,
                    LINE_PRCSG_IND_CD  ,
                    LINE_ICD9_DGNS_CD  ,
                    numeric_postfix   ,
                    carrier_claim_id)  
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"""
  def explode_row_for_line_items(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,13):
      start_for_npi = 13 -1
      start_for_tax = 26 -1
      start_for_hcpcs_cd = 39  -1
      start_for_payment_amount = 52 -1
      start_for_deduct_amount = 65 -1
      start_for_payor_paid_amount = 78 -1
      start_for_coinsurance_amount = 91 -1
      start_for_allowed_charge = 104 -1
      start_for_prcsd_ind = 117 -1
      start_for_icd9_cod = 130 -1
      numeric_postfix = i + 1
      yield [row[ start_for_npi +i ],
            row[ start_for_tax +i ],
            row[ start_for_hcpcs_cd +i ],
            row[ start_for_payment_amount +i ],
            row[ start_for_deduct_amount +i ],
            row[ start_for_payor_paid_amount +i ],
            row[ start_for_coinsurance_amount +i ],
            row[ start_for_allowed_charge +i ],
            row[ start_for_prcsd_ind +i ],
            row[ start_for_icd9_cod +i ]] +  [numeric_postfix] + colsToKeep  
  sql_function_pairs = [ (sql_for_main_claim, claim_row),
                         (sql_for_line,explode_row_for_line_items),
                         (sql_for_diag,explode_row_for_diags) ]
  realF = load_generic(the_file,1,sql_function_pairs,the_database,the_ddl)
  realF()


def load_outpatient(the_file,the_ddl=None,the_database =None ):
  print("running load_outpatient() with ", the_file)
  if the_database is None:
    file_first,_ = os.path.basename(the_file).split(".")  
    the_database = file_first + ".db"
  sql_for_main_claim = """INSERT OR IGNORE INTO outpatient_claim (
                            DESYNPUF_ID  ,
                            CLM_ID  ,
                            SEGMENT  ,
                            CLM_FROM_DT  ,
                            CLM_THRU_DT  ,
                            PRVDR_NUM  ,
                            CLM_PMT_AMT  ,
                            NCH_PRMRY_PYR_CLM_PD_AMT  ,
                            AT_PHYSN_NPI  ,
                            OP_PHYSN_NPI  ,
                            OT_PHYSN_NPI  ,
                            NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  ,
                            NCH_BENE_PTB_DDCTBL_AMT  ,
                            NCH_BENE_PTB_COINSRNC_AMT  ,
                            ADMTNG_ICD9_DGNS_CD  ,
                            BatchId ) 
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  """
  def claim_row(row,batchid):
    colsToKeep = [row[i] for i in range(0,12)] + [row[i] for i in range(28,31)]
    yield colsToKeep + [batchid]
  sql_for_hcpc_procedure = "INSERT OR IGNORE INTO hcpc_outpatient_procedure (\
                      HCPCS_CD , \
                      numeric_postfix  , \
                      outpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_hcpc_procedures(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,45):
      actual_row = i + 31
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_for_icd9_procedure = "INSERT OR IGNORE INTO icd9_outpatient_procedure (\
                      ICD9_PRCDR_CD , \
                      numeric_postfix  , \
                      outpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_icd9_procedures(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,6):
      actual_row = i + 22
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_for_diag = "INSERT OR IGNORE INTO outpatient_diagnosis (\
                      ICD9_DGNS_CD , \
                      numeric_postfix  , \
                      outpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_diags(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,10):
      actual_row = i + 12
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_function_pairs = [ (sql_for_main_claim, claim_row) ,
                          (sql_for_hcpc_procedure,explode_row_for_hcpc_procedures),
                          (sql_for_icd9_procedure,explode_row_for_icd9_procedures),
                          (sql_for_diag,explode_row_for_diags) ]
  realF = load_generic(the_file,1,sql_function_pairs,the_database,the_ddl)
  realF()

def load_inpatient(the_file,the_ddl=None,the_database =None ):
  print("running load_inpatient() with ", the_file)
  if the_database is None:
    file_first,_ = os.path.basename(the_file).split(".")  
    the_database = file_first + ".db"
  sql_for_main_claim = """INSERT OR IGNORE INTO inpatient_claim (
                            DESYNPUF_ID  ,
                            CLM_ID  ,
                            SEGMENT  ,
                            CLM_FROM_DT  ,
                            CLM_THRU_DT  ,
                            PRVDR_NUM  ,
                            CLM_PMT_AMT  ,
                            NCH_PRMRY_PYR_CLM_PD_AMT  ,
                            AT_PHYSN_NPI  ,
                            OP_PHYSN_NPI  ,
                            OT_PHYSN_NPI  ,
                            CLM_ADMSN_DT  ,
                            ADMTNG_ICD9_DGNS_CD  ,
                            CLM_PASS_THRU_PER_DIEM_AMT  ,
                            NCH_BENE_IP_DDCTBL_AMT  ,
                            NCH_BENE_PTA_COINSRNC_LBLTY_AM  ,
                            NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  ,
                            CLM_UTLZTN_DAY_CNT  ,
                            NCH_BENE_DSCHRG_DT  ,
                            CLM_DRG_CD  ,
                            BatchId ) 
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
  """
  def claim_row(row,batchid):
    colsToKeep = [row[i] for i in range(0,20)]
    yield colsToKeep + [batchid]
  sql_for_hcpc_procedure = "INSERT OR IGNORE INTO hcpc_inpatient_procedure (\
                      HCPCS_CD , \
                      numeric_postfix  , \
                      inpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_hcpc_procedures(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,45):
      actual_row = i + 36
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_for_icd9_procedure = "INSERT OR IGNORE INTO icd9_inpatient_procedure (\
                      ICD9_PRCDR_CD , \
                      numeric_postfix  , \
                      inpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_icd9_procedures(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,6):
      actual_row = i + 30
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_for_diag = "INSERT OR IGNORE INTO inpatient_diagnosis (\
                      ICD9_DGNS_CD , \
                      numeric_postfix  , \
                      inpatient_claim_id \
           ) VALUES (?,?,?)"
  def explode_row_for_diags(row,batchid):
    colsToKeep = [row[1]]
    for i in range(0,10):
      actual_row = i + 21
      if row[actual_row]:
        yield [row[actual_row]] + [i+1] + colsToKeep 
  sql_function_pairs = [ (sql_for_main_claim, claim_row) ,
                          (sql_for_hcpc_procedure,explode_row_for_hcpc_procedures),
                          (sql_for_icd9_procedure,explode_row_for_icd9_procedures),
                          (sql_for_diag,explode_row_for_diags) ]
  realF = load_generic(the_file,1,sql_function_pairs,the_database,the_ddl)
  realF()

def load_main_files():
  # load_carrier("test-data/DE1_0_2008_to_2010_Carrier_Claims_Sample_2A.csv","ddl/create-carrier-etl.ddl","carrier.db")
  # load_carrier("test-data/DE1_0_2008_to_2010_Carrier_Claims_Sample_2B.csv","ddl/create-carrier-etl.ddl","carrier.db")
  # load_inpatient("test-data/DE1_0_2008_to_2010_Inpatient_Claims_Sample_2.csv","ddl/create-inpatient-etl.ddl","inpatient.db")
  load_outpatient("test-data/DE1_0_2008_to_2010_Outpatient_Claims_Sample_2.csv","ddl/create-outpatient-etl.ddl","outpatient.db")



