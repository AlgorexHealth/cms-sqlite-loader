drop table if exists inpatient_claim;
drop table if exists inpatient_diagnosis;
drop table if exists icd9_inpatient_procedure;
drop table if exists hcpc_inpatient_procedure;
drop table if exists batch;

CREATE TABLE inpatient_claim
(
  DESYNPUF_ID  TEXT,
  CLM_ID  TEXT,
  SEGMENT  TEXT,
  CLM_FROM_DT  TEXT,
  CLM_THRU_DT  TEXT,
  PRVDR_NUM  TEXT,
  CLM_PMT_AMT  TEXT,
  NCH_PRMRY_PYR_CLM_PD_AMT  TEXT,
  AT_PHYSN_NPI  TEXT,
  OP_PHYSN_NPI  TEXT,
  OT_PHYSN_NPI  TEXT,
  CLM_ADMSN_DT  TEXT,
  ADMTNG_ICD9_DGNS_CD  TEXT,
  CLM_PASS_THRU_PER_DIEM_AMT  TEXT,
  NCH_BENE_IP_DDCTBL_AMT  TEXT,
  NCH_BENE_PTA_COINSRNC_LBLTY_AM  TEXT,
  NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  TEXT,
  CLM_UTLZTN_DAY_CNT  TEXT,
  NCH_BENE_DSCHRG_DT  TEXT,
  CLM_DRG_CD  TEXT,
  BatchId  INTEGER
);

CREATE TABLE hcpc_inpatient_procedure
(
  HCPCS_CD  TEXT,
  numeric_postfix   INTEGER,
  inpatient_claim_id  TEXT
);
CREATE TABLE icd9_inpatient_procedure
(
  ICD9_PRCDR_CD  TEXT,
  numeric_postfix   INTEGER,
  inpatient_claim_id  TEXT
);

CREATE TABLE inpatient_diagnosis
(
  ICD9_DGNS_CD  TEXT,
  numeric_postfix   INTEGER,
  inpatient_claim_id  TEXT
);

CREATE TABLE batch
(
  filename  TEXT,
  filesize  INTEGER,
  md5  TEXT,
  ingest_start_time  INTEGER,
  directory  TEXT
);
