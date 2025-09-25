###라이브러리 로드
library(tidyverse)
library(rvest)
library(xml2)
library(readr)
library(httr)
library(jsonlite)
library(RSQLite)
library(DBI)

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

###동작 라인
corp_df <- corp_func()

stock_df <- corp_df[-which(corp_df$stock_code == " "), ]

clean_stock <- class_transf(stock_df)

corp_info <- function(url = "https://opendart.fss.or.kr/api/list.json?crtfc_key=", key = Sys.getenv("DART_FSS"), pblntf_ty = "A", corp_cls) {
  read_json(paste0(url, key, "&", pblntf_ty, "&", corp_cls))
}

corp_info(corp_cls = "Y")

con <- dbConnect(SQLite(), "~/HaltCompany_Predict/data/SQLiteDB.sqlite")
dbWriteTable(con, "clean_stock", clean_stock)
