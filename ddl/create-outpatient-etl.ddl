drop table if exists outpatient_claim;
drop table if exists outpatient_diagnosis;
drop table if exists icd9_outpatient_procedure;
drop table if exists hcpc_outpatient_procedure;
drop table if exists batch;

CREATE TABLE outpatient_claim
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
    NCH_BENE_BLOOD_DDCTBL_LBLTY_AM  TEXT,
    NCH_BENE_PTB_DDCTBL_AMT  TEXT,
    NCH_BENE_PTB_COINSRNC_AMT  TEXT,
    ADMTNG_ICD9_DGNS_CD  TEXT,
    BatchId  INTEGER
);

CREATE TABLE hcpc_outpatient_procedure
(
  HCPCS_CD  TEXT,
  numeric_postfix   INTEGER,
  outpatient_claim_id  TEXT
);
CREATE TABLE icd9_outpatient_procedure
(
  ICD9_PRCDR_CD  TEXT,
  numeric_postfix   INTEGER,
  outpatient_claim_id  TEXT
);

CREATE TABLE outpatient_diagnosis
(
  ICD9_DGNS_CD  TEXT,
  numeric_postfix   INTEGER,
  outpatient_claim_id  TEXT
);

CREATE TABLE batch
(
  filename  TEXT,
  filesize  INTEGER,
  md5  TEXT,
  ingest_start_time  INTEGER,
  directory  TEXT
);
