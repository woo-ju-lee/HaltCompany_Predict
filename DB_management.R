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
library(data.table)

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

##다중회사 주요계정

dart_finance <- function(bsns_year, reprt_code, data_name) {
  main_url = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json?crtfc_key="
  api = Sys.getenv("DART_FSS")
  
  corp_cd = data_name$corp_code
  
  chunck = c(seq(0, length(corp_cd), by = 100), length(corp_cd))
  
  res = map(2:length(chunck), function(i) {
    full_url = paste0(main_url, api, "&corp_code=", paste(corp_cd[(chunck[i-1] + 1):chunck[i]], collapse = ","), "&bsns_year=", bsns_year, "&reprt_code=", reprt_code)
    
    fromJSON(full_url)$list
  })
  
  fin_res = bind_rows(res)
}


#dart 전체 재무 정보
dart_finance_state <- function(corp_code, bsns_yaer, reprt_code, fs_div) {
  
  main_url = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json?crtfc_key="
  
  api = Sys.getenv("DART_FSS")
  
  comp_name = corp_code$corp_code
  
  res = map(1:length(comp_name), function(i) {
    
    full_url = paste0(main_url, api, "&corp_code=", comp_name[i], "&bsns_year=", bsns_yaer, "&reprt_code=", reprt_code, "&fs_div=", fs_div)
    
    fromJSON(full_url)$list
  })
  
  fin_res = bind_rows(res)
}

#dart 기준 영업정지 기업 목록

halt_company_list <- function(bgn_de, end_de) {
  
  url <- "https://opendart.fss.or.kr/disclosureinfo/mainMatter/list.do"
  
  base_payload <- list(
    pageIndex = 2,
    pageSize = 10,
    pageUnit = 10,
    recordCountPerPage = 100,
    sortStdr = "crp",
    sortOrdr = "asc",
    sumSortStdr = "",
    sumSortOrdr = "asc",
    textCrpCik = "",
    bgnDe = bgn_de,
    endDe = end_de,
    textCrpNm = "",
    startDate = bgn_de,
    endDate = end_de,
    corpTypeAll = 1,
    reportCode = "11303"
  )
  
  corp_types <- setNames(
    as.list(c("P", "A", "N", "E")),
    rep("corpType", 4)
  )
  
  payload <- c(base_payload, corp_types)
  
  res <- POST(
    url,
    body = payload,
    encode = "form",
    add_headers(
      "User-Agent" = "Mozilla/5.0",
      "Referer" = "https://opendart.fss.or.kr/disclosureinfo/mainMatter/list.do"
    )
  )
  
  html_txt <- content(res, "text", encoding = "UTF-8")
  doc <- read_html(html_txt)
  doc_table <- html_table(doc, header = TRUE)[[1]][-1, ]
  
  return(doc_table)
}

#XBRL 요소 이름들

status_list <- function() {
  
  url = "https://opendart.fss.or.kr/api/xbrlTaxonomy.json?crtfc_key="
  api = Sys.getenv("DART_FSS")
  sj_div = c("BS1", "BS3", "IS1" ,"IS3", "CIS1", "CIS3", "DCIS1", "DCIS3", "DCIS5", "DCIS7", "CF1", "CF3", "SCE1")
  
  div = map(1:length(sj_div), function(i) {
    main_url = paste0(url, api, "&sj_div=", sj_div[i])
  }) %>% unlist()
  
  res = map(1:length(div), function(i) {
    fromJSON(div[i])$list
  })
  
  fin_res = bind_rows(res)
}

status_list_single <- function() {
  
  url = "https://opendart.fss.or.kr/api/xbrlTaxonomy.json?crtfc_key="
  api = Sys.getenv("DART_FSS")
  sj_div = c("BS2", "BS4", "IS2" ,"IS4", "CIS2", "CIS4", "DCIS2", "DCIS4", "DCIS6", "DCIS8", "CF2", "CF4", "SCE2")
  
  div = map(1:length(sj_div), function(i) {
    main_url = paste0(url, api, "&sj_div=", sj_div[i])
  }) %>% unlist()
  
  res = map(1:length(div), function(i) {
    fromJSON(div[i])$list
  })
  
  fin_res = bind_rows(res)
}

#다중회사 주요재무지표

status_list_multi <- function(corp_code, year) {
  url = "https://opendart.fss.or.kr/api/fnlttCmpnyIndx.json?crtfc_key="
  api = Sys.getenv("DART_FSS")
  code = corp_code$corp_code
  idx = c("M210000", "M220000", "M230000", "M240000")
  
  chunck = c(seq(0, length(code), by = 100), length(code))
  
  res = map(1:length(idx), function(j) {
    sub_res = map(2:length(chunck), function(i) {
      main_url = paste0(url, api, "&corp_code=", paste(code[(chunck[i-1] + 1):chunck[i]], collapse = ","), "&bsns_year=", year, "&reprt_code=11011", "&idx_cl_code=", idx[j])
      
      fromJSON(main_url)$list
    })
    
    bind_rows(sub_res)
  })
  
  fin_res = bind_rows(res)
  return(fin_res)
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

##2015~2025 재무 정보 다운로드 코드 45만건 * 21피처
##DB에 추가 필요, 정규화도 고민 필요

x5 <- map(2015:2026, function(i) {
  dart_finance(i, 11011, x4)
}) %>% bind_rows()

write_csv(x5, "finance.csv")

x5 <- fread("finance.csv")

x6 <- x5 %>% group_split(corp_code)

#system.time(split(x5, x5$corp_code))

##정규화 할 때  연도 기준? 회사 기준?
##연결재무제표도 재무제표와 어떤식으로 분리할지 기준이 필요함

company_status_total <- map(2020:2025, function(i) {
  dart_finance_state(x4, i, 11011, "CFS")
})

company_status_total2 <- map(2015:2019, function(i) {
  dart_finance_state(x4, i, 11011, "CFS")
})

x10 <- data.frame(corp_code = x4$corp_code[which(x4$corp_code %in% unique(x9$corp_code) == FALSE)])

company_status_single_total <- map(2015:2025, function(i) {
  dart_finance_state(x10, i, 11011, "OFS")
})

#saveRDS(company_status_total, "company_status_total.rds")
#saveRDS(company_status_total2, "company_status_total2.rds")
#saveRDS(company_status_single_total, "company_status_single_total.rds")

company <- readRDS("company_status_total.rds")
company2 <- readRDS("company_status_total2.rds")
company3 <- readRDS("company_status_single_total.rds")

x9 <- bind_rows(c(company, company2, company3)) %>% arrange(bsns_year)

x9 <- x9[,-c(18:21)]

#saveRDS(x9, "cp_total.rds")

company <- readRDS("cp_total.rds")

company <- company %>% mutate(across(c(1, 2, 3, 11, 13, 15, 16), as.numeric))

company_short <- company %>% 
  distinct(corp_code, bsns_year) %>% 
  group_by(corp_code) %>% 
  summarise(
    n_year = n(),
    all_year_exist = setequal(bsns_year, 2015:2024),
    .groups = "drop"
  ) %>% 
  filter(n_year == length(2015:2024), all_year_exist) %>% 
  pull(corp_code)

company_short <- company %>% 
  filter(corp_code %in% company_short)

x1 <- company_short %>% 
  group_by(corp_code) %>% 
  filter(!any(account_id == "-표준계정코드 미사용-")) %>% 
  ungroup()

company_short %>% select(account_nm, thstrm_amount) %>% filter(str_detect(account_nm, "자본"))
company_short %>% select(account_nm, thstrm_amount) %>% filter(account_nm == "자본총계")


###표준계정코드 미사용이 있는 기업들 제외하니 1개 기업만 남음 해당 문제 해결해야할 듯. 먼저 각 재무지표 수익성, 위험성 등 어떤 재무정보가 들어가는 지 확인한 후 해당 정보부터 추출하여 사용하는 것이 좋아보임. 현재는 1100개 기업정도.

##XBRL 요소중에 ifrs-full_을 갖고 있는 애들이 있음. 없애야 함.


company_BS <- company_short %>% filter(sj_nm == "재무상태표")

company_BS <- company_BS %>% 
  mutate(account_nm = gsub("[^가-힣]", "", account_nm))

x2 <- company_BS %>% select(account_nm, thstrm_amount, account_id) %>% filter(str_detect(account_nm, "자본")) %>% select(account_id, account_nm) %>% unique()

x3 <- dart_finance(2015, 11011, x4)
x5 <- status_list_multi(x4, 2023)
x6 <- company_short %>% filter(corp_code == "00101220" & bsns_year == 2023) 
x7 <- x5 %>% filter(idx_cl_code == "M220000" & corp_code == "00101220")
