###라이브러리 로드

library(tidyverse)
library(rvest)
library(xml2)
library(readr)
library(httr)
library(jsonlite)
library(openxlsx)
library(DBI)
library(RSQLite)

###DB용 데이터 정제

###함수 생성

##기업 고유번호 추출 함수

corp_func <- function() {
  
  if(length(Sys.glob("~/HaltCompany_Predict/data/CORPCODE.*")) > 0) {
    
    cat("File is already exist. Load a file")
    
    file_name <- list.files("~/HaltCompany_Predict/data", pattern = "CORPCODE.*")
    
    if(str_detect(file_name, pattern = ".xml") > 0) {
      
      read_xml(paste0("~/HaltCompany_Predict/data/", file_name)) %>% 
        as_list() %>% 
        {.$result} %>% 
        bind_rows()
    } else {
      read_xml(unzip(paste0("~/HaltCompany_Predict/data/", file_name))) %>% 
        as_list() %>% 
        {.$result} %>% 
        bind_rows()
    }
    
  } else {
    
    cat("File is not exist. Download and Load a file")
    
    url = "https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key="
    key = Sys.getenv("DART_FSS")
    
    GET(paste0(url, key), write_disk("~/HaltCompany_Predict/data/CORPCODE.zip", overwrite = TRUE))
    
    read_xml(unzip("~/HaltCompany_Predict/data/CORPCODE.zip")) %>% 
      as_list() %>% 
      {.$result} %>% 
      bind_rows()
    
  }
}

##클래스 맞추기

class_transf <- function(data_df) {
  
  data_df <- lapply(data_df, unlist)
  
  data_df$corp_code <- as.character(data_df$corp_code)
  data_df$corp_name <- as.character(data_df$corp_name)
  data_df$corp_eng_name <- as.character(data_df$corp_eng_name)
  data_df$stock_code <- as.character(data_df$stock_code)
  data_df$modify_date <- as.Date(unlist(data_df$modify_date), "%Y%m%d")
  
  data_df <- as.data.frame(data_df)
  
  return(data_df)
}

##회사 상태 불러오기 (주식시장 확인용)

company_status <- function(code) {
  url = "https://opendart.fss.or.kr/api/company.json?crtfc_key="
  crtfc_key = Sys.getenv("DART_FSS")
  corp_code = code
  
  res = read_json(paste0(url, crtfc_key, "&corp_code=", corp_code))
  
  res = bind_rows(res)
  
  return(res)
}

##회사 재무지표 불러오기

company_fnltt <- function(code, year) {
  url = "https://opendart.fss.or.kr/api/fnlttSinglAcnt.json?crtfc_key="
  crtfc_key = Sys.getenv("DART_FSS")
  corp_code = code
  years = year
  report_code = "11011"
  
  res = read_json(paste0(url, crtfc_key, "&corp_code=", code, "&bsns_year=", years, "&reprt_code=", report_code))
  
  res = bind_rows(res$list)
  
  return(res)
}

##KRX 회사 정보 불러오기

krx_api <- function(market_name) {
  main_url = "https://data-dbg.krx.co.kr/svc/apis/sto/"
  sub_url <- switch(market_name,
                    kospi  = "stk_isu_base_info.json",
                    kosdaq = "ksq_isu_base_info.json",
                    konex  = "knx_isu_base_info.json",
                    stop("잘못된 시장 이름입니다. (kospi, kosdaq, konex 중 하나 입력)")
  )
  api = Sys.getenv("KRX")
  date = format(Sys.Date() - 1, "%Y%m%d")
  full_url <- paste0(main_url, sub_url, "?AUTH_KEY=", api, "&basDd=", date)
  
  fromJSON(full_url)$OutBlock_1
}


###동작 라인

corp_df <- corp_func()

stock_df <- corp_df[-which(corp_df$stock_code == " "), ]

clean_stock <- class_transf(stock_df)

stock_info <- map(1:length(clean_stock$corp_code), function(i) {
  company_status(clean_stock$corp_code[i])
})

stock_info <- stock_info %>% bind_rows()

stock_info$est_dt <- as.Date(stock_info$est_dt, "%Y%m%d")

stock_info$ceo_nm <- gsub(" ", "", stock_info$ceo_nm)

stock_info <- stock_info %>% select(
  corp_code, corp_name, corp_name_eng, 
  stock_name, stock_code, ceo_nm, 
  corp_cls, jurir_no, bizr_no, 
  induty_code, est_dt, acc_mt
)

con <- dbConnect(SQLite(), "~/HaltCompany_Predict/data/SQLiteDB.sqlite")

k_std <- read.xlsx("~/HaltCompany_Predict/data/k_std_sort.xlsx", 
                   sheet = 2,
                   startRow = 4,
                   colNames = FALSE,
                   skipEmptyRows = FALSE,
                   skipEmptyCols = FALSE,
                   fillMergedCells = TRUE
)

colnames(k_std) <- c("major_category_code", "major_category", "middle_category_code", "middle_category", "minor_category_code", "minor_category", "fine_category_code", "fine_category", "micro_category_code", "micro_category")

krx_status <- rbind(
  krx_api("kospi"),
  krx_api("kosdaq"),
  krx_api("konex")
)

###DB 생성 라인

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
    stock_code    TEXT NOT NULL,
    ceo_nm        TEXT,
    corp_cls      TEXT,
    jurir_no      TEXT NOT NULL,
    bizr_no       TEXT NOT NULL,
    induty_code   TEXT NOT NULL,
    est_dt        TEXT,
    acc_mt        TEXT,
    PRIMARY KEY (bizr_no),
    FOREIGN KEY (corp_code) REFERENCES clean_stock(corp_code)
  )
")

dbWriteTable(con, "STOCK_INFO", stock_info, append = TRUE)

dbExecute(con, "
          CREATE TABLE K_STD_SORT (
          major_category_code TEXT, 
          major_category TEXT, 
          middle_category_code TEXT, 
          middle_category TEXT, 
          minor_category_code TEXT, 
          minor_category TEXT,
          fine_category_code TEXT,
          fine_category TEXT,
          micro_category_code TEXT NOT NULL,
          micro_category TEXT NOT NULL,
          PRIMARY KEY (micro_category_code),
          FOREIGN KEY (minor_category_code) REFERENCES STOCK_INFO(induty_code)
          )
        ")

dbWriteTable(con, "K_STD_SORT", k_std, append = TRUE)

dbExecute(con, "
          CREATE TABLE KRX_STATUS (
          ISU_CD TEXT NOT NULL,
          ISU_SRT_CD TEXT NOT NULL,
          ISU_NM TEXT,
          ISU_ABBRV TEXT,
          ISU_ENG_NM TEXT,
          LIST_DD TEXT,
          MKT_TP_NM TEXT,
          SECUGRP_NM TEXT,
          SECT_TP_NM TEXT,
          KIND_STKCERT_TP_NM TEXT,
          PARVAL TEXT,
          LIST_SHRS TEXT,
          PRIMARY KEY (ISU_CD),
          FOREIGN KEY (ISU_SRT_CD) REFERENCES STOCK_INFO(stock_code)
          )
        ")

dbWriteTable(con, "KRX_STATUS", krx_status, append = TRUE)

### 메인 동작라인

con <- dbConnect(SQLite(), "~/HaltCompany_Predict/data/SQLiteDB.sqlite")

corp_code <- dbGetQuery(con, "SELECT corp_code FROM STOCK_INFO WHERE corp_cls != 'E'")$corp_code

company_fnltt(corp_code[1], 2024)

company_fn <- map(1:length(corp_code), function(i) {
  company_fnltt(corp_code[i], 2024)
})

sort <- dbGetQuery(con, "SELECT * FROM K_STD_SORT")
induty_code <- dbGetQuery(con, "SELECT induty_code FROM STOCK_INFO")

#x1 <- map(1:nrow(induty_code), function(i) {
#  ifelse(nchar(induty_code$induty_code[i]) == 5, subset(k_std, micro_category_code == induty_code$induty_code[i]#)$micro_category, 
#         ifelse(nchar(induty_code$induty_code[i]) == 4, subset(k_std, fine_category_code == induty_code$induty_code[i])$fine_category, 
#                ifelse(nchar(induty_code$induty_code[i]) == 3, subset(k_std, minor_category_code == induty_code$induty_code[i])$minor_category, 
#                       ifelse(nchar(induty_code$induty_code[i]) == 2, subset(k_std, middle_category_code == induty_code$induty_code[i])$middle_category, 
#                              0))))
#})

x2 <- dbGetQuery(con, "SELECT stock_name, stock_code
FROM STOCK_INFO
WHERE stock_name NOT LIKE '%스팩%'
  AND stock_name NOT GLOB '*[0-9]*호*'
  AND stock_name NOT LIKE '%리츠'
  AND stock_name NOT LIKE '%투자회사%'
  AND stock_name NOT LIKE '%인수목적%'
  AND stock_name NOT LIKE '%펀드%';")

x3 <- dbGetQuery(con, "
           SELECT *
           FROM STOCK_INFO A
           JOIN KRX_STATUS B
           ON A.stock_code = B.ISU_SRT_CD
           WHERE B.SECUGRP_NM = '주권' 
           AND B.KIND_STKCERT_TP_NM = '보통주' 
           AND B.SECT_TP_NM NOT LIKE '%소속부없음%'
           ")

x4 <- dbGetQuery(con, "
           SELECT A.corp_code
           FROM STOCK_INFO A
           JOIN KRX_STATUS B
           ON A.stock_code = B.ISU_SRT_CD
           WHERE B.SECUGRP_NM = '주권' 
           AND B.KIND_STKCERT_TP_NM = '보통주' 
           AND B.SECT_TP_NM NOT LIKE '%소속부없음%'
           ")

x5 <- fromJSON(paste0("https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?crtfc_key=", Sys.getenv("DART_FSS"), "&corp_code=", paste(head(x4$corp_code, n = 100), collapse = ","), "&bsns_year=2018&reprt_code=11011"))$list

#map 함수로 2015년 부터 2025년 까지의 재무정보 불러오기 + db에 저장 필요