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

###함수 생성

##회사 상태 불러오기 (주식시장 확인용)

company_status <- function(code) {
  url = "https://opendart.fss.or.kr/api/company.json?crtfc_key="
  crtfc_key = Sys.getenv("DART_FSS")
  corp_code = code
  
  res = read_json(paste0(url, crtfc_key, "&corp_code=", corp_code))
  
  res = bind_rows(res)
  
  return(res)
}

###동작 라인

con <- dbConnect(SQLite(), "~/HaltCompany_Predict/data/SQLiteDB.sqlite")

