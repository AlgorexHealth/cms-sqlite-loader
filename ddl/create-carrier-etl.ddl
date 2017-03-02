drop table if exists carrier_claim;
drop table if exists line_item;
drop table if exists carrier_diagnosis;
drop table if exists batch;

CREATE TABLE carrier_claim
(
  CLM_ID  TEXT PRIMARY KEY,
  DESYNPUF_ID  TEXT,
  CLM_FROM_DT  TEXT,
  CLM_THRU_DT  TEXT,
  BatchId  INTEGER
);

CREATE TABLE line_item
(
  PRF_PHYSN_NPI  TEXT,
  TAX_NUM  TEXT,
  HCPCS_CD  TEXT,
  LINE_NCH_PMT_AMT  REAL,
  LINE_BENE_PTB_DDCTBL_AMT  REAL,
  LINE_BENE_PRMRY_PYR_PD_AMT  REAL,
  LINE_COINSRNC_AMT  REAL,
  LINE_ALOWD_CHRG_AMT  REAL,
  LINE_PRCSG_IND_CD  TEXT,
  LINE_ICD9_DGNS_CD  TEXT,
  numeric_postfix   INTEGER,
  carrier_claim_id  TEXT
);

CREATE TABLE carrier_diagnosis
(
  ICD9_DGNS_CD  TEXT,
  numeric_postfix   INTEGER,
  carrier_claim_id  TEXT
);

CREATE TABLE batch
(
  filename  TEXT,
  filesize  INTEGER,
  md5  TEXT,
  ingest_start_time  INTEGER,
  directory  TEXT
);
