drop table if exists batch;
drop table if exists drug_events;

CREATE TABLE drug_events
(
    DESYNPUF_ID TEXT,
    PDE_ID TEXT,
    SRVC_DT TEXT,
    PROD_SRVC_ID TEXT,
    QTY_DSPNSD_NUM TEXT,
    DAYS_SUPLY_NUM TEXT,
    PTNT_PAY_AMT TEXT,
    TOT_RX_CST_AMT TEXT,
    BatchId  INTEGER
);

CREATE TABLE batch
(
  filename  TEXT,
  filesize  INTEGER,
  md5  TEXT,
  ingest_start_time  INTEGER,
  directory  TEXT
);
