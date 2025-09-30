library(DBI)
library(RSQLite)

con <- dbConnect(SQLite(), "~/HaltCompany_Predict/data/SQLiteDB.sqlite")

dbExecute(con, "
  CREATE TABLE clean_stock (
    corp_code TEXT PRIMARY KEY,
    corp_name TEXT NOT NULL,
    corp_eng_name TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    modify_date DATE
  )
")

dbWriteTable(con, "clean_stock", clean_stock, append = TRUE)

dbExecute(con, "
  CREATE TABLE STOCK_INFO (
    corp_code     TEXT NOT NULL,
    corp_name     TEXT,
    corp_name_eng TEXT,
    stock_name    TEXT,
    stock_code    TEXT,
    ceo_nm        TEXT,
    corp_cls      TEXT,
    jurir_no      TEXT NOT NULL,
    bizr_no       TEXT NOT NULL,
    induty_code   TEXT,
    est_dt        TEXT,
    acc_mt        TEXT,
    PRIMARY KEY (bizr_no),
    FOREIGN KEY (corp_code) REFERENCES clean_stock(corp_code)
  )
")

dbWriteTable(con, "STOCK_INFO", stock_info, append = TRUE)
