###라이브러리 로드
library(tidyverse)
library(rvest)
library(xml2)
library(readr)
library(httr)
library(jsonlite)
library(openxlsx)

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

company_status <- function(code) {
  url = "https://opendart.fss.or.kr/api/company.json?crtfc_key="
  crtfc_key = Sys.getenv("DART_FSS")
  corp_code = code
  
  res = read_json(paste0(url, crtfc_key, "&corp_code=", corp_code))
  
  res = bind_rows(res)
  
  return(res)
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
          startRow = 3,
          colNames = FALSE,
          skipEmptyRows = FALSE,
          skipEmptyCols = FALSE,
          fillMergedCells = TRUE
)

k_std_sort <- k_std[complete.cases(k_std[, 6]), 1:6]

colnames(k_std_sort) <- c("major_category_code", "major_category", "medium_category_code", "medium_category", "minor_category_code", "minor_category")
