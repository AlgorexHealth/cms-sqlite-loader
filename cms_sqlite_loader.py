import os.path
import sys
import time
import subprocess
import sqlite3
import csv

sys.path.append("deps/algorex-data-dictionary/")
from algrxdd import dbutils as u
from algrxdd import dbfileloader as dbf
from algrxdd import dbstrategies as strat

debug = False

def load_carrier(the_file,the_database =None ):
  new_line_item_col_names = ("PRF_PHYSN_NPI"  ,
                    "TAX_NUM"  ,
                    "HCPCS_CD"  ,
                    "LINE_NCH_PMT_AMT"  ,
                    "LINE_BENE_PTB_DDCTBL_AMT"  ,
                    "LINE_BENE_PRMRY_PYR_PD_AMT"  ,
                    "LINE_COINSRNC_AMT"  ,
                    "LINE_ALOWD_CHRG_AMT"  ,
                    "LINE_PRCSG_IND_CD"  ,
                    "LINE_ICD9_DGNS_CD"  )
  new_line_item_matchers = tuple("^" + i for i in new_line_item_col_names)
  second_criteria = { "explode-criteria": ("^ICD9_DGNS_CD",) , 
                                  "table-name": "carrier_diagnosis",
                                  "column-names": ("icd9",)  }
  first_criteria = { "explode-criteria": new_line_item_matchers , 
                                  "table-name": "line_item",
                                  "column-names": new_line_item_col_names  }
  daniel = strat.Exploding( [first_criteria,second_criteria] )
  dbf.quick_load(the_file,the_database, "carrier_claim",delim=",",strategy=daniel)


def load_drug_events(the_file,the_database =None ):
  print("running load_drug_events() with ", the_file)
  dbf.quick_load(the_file,the_database, "drug_events")

def load_inpatient(the_file,the_database =None ):
  print("running load_inpatient() with ", the_file)
  first_criteria = { "explode-criteria": ("^HCPCS_CD",) , 
                                  "table-name": "hcpc_inpatient_procedure",
                                  "column-names": ('HCPCS_CD',)  }
  second_criteria = { "explode-criteria": ("^ICD9_PRCDR_CD",) , 
                                  "table-name": "icd9_inpatient_procedure",
                                  "column-names": ("ICD9_PRCDR_CD",)  }
  third_criteria = { "explode-criteria": ("^ICD9_DGNS_CD",) , 
                                  "table-name": "inpatient_diagnosis",
                                  "column-names": ("ICD9_DGNS_CD",)  }
  tuple_p = strat.Exploding( [first_criteria,second_criteria,third_criteria] )
  dbf.quick_load(the_file,the_database, "inpatient_claim",delim=",",strategy=tuple_p)



def load_outpatient(the_file,the_database =None ):
  print("running load_outpatient() with ", the_file)
  first_criteria = { "explode-criteria": ("^HCPCS_CD",) , 
                                  "table-name": "hcpc_outpatient_procedure",
                                  "column-names": ('HCPCS_CD',)  }
  second_criteria = { "explode-criteria": ("^ICD9_PRCDR_CD",) , 
                                  "table-name": "icd9_outpatient_procedure",
                                  "column-names": ("ICD9_PRCDR_CD",)  }
  third_criteria = { "explode-criteria": ("^ICD9_DGNS_CD",) , 
                                  "table-name": "outpatient_diagnosis",
                                  "column-names": ("ICD9_DGNS_CD",)  }
  tuple_p = strat.Exploding( [first_criteria,second_criteria,third_criteria] )
  dbf.quick_load(the_file,the_database, "outpatient_claim",delim=",",strategy=tuple_p)

def load_beneficiary(the_file,the_database =None ):
  print("running load_beneficiary() with ", the_file)
  dbf.quick_load(the_file,the_database, "beneficiary")

def load_main_files(sample):
  filen =   "sample_{0}/DE1_0_2008_to_2010_Carrier_Claims_Sample_{0}A.csv".format(sample)
  load_carrier(filen,"carrier.db")
  filen =   "sample_{0}/DE1_0_2008_to_2010_Carrier_Claims_Sample_{0}B.csv".format(sample)
  load_carrier(filen,"carrier.db")
  filen =   "sample_{0}/DE1_0_2008_to_2010_Inpatient_Claims_Sample_{0}.csv".format(sample)
  load_inpatient(filen,"inpatient.db")
  filen =   "sample_{0}/DE1_0_2008_to_2010_Outpatient_Claims_Sample_{0}.csv".format(sample)
  load_outpatient(filen,"outpatient.db")
  filen =   "sample_{0}/DE1_0_2008_Beneficiary_Summary_File_Sample_{0}.csv".format(sample)
  load_beneficiary(filen,"beneficiary.db")
  filen =   "sample_{0}/DE1_0_2009_Beneficiary_Summary_File_Sample_{0}.csv".format(sample)
  load_beneficiary(filen,"beneficiary.db")
  filen =   "sample_{0}/DE1_0_2010_Beneficiary_Summary_File_Sample_{0}.csv".format(sample)
  load_beneficiary(filen,"beneficiary.db")
  filen =   "sample_{0}/DE1_0_2008_to_2010_Prescription_Drug_Events_Sample_{0}.csv".format(sample)
  load_drug_events(filen,"prescription.db")


if __name__ == "__main__": 
  load_main_files(sys.argv[1])
