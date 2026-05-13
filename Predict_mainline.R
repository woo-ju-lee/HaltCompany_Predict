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
library(xgboost)

###함수 생성



###동작 라인
set.seed(123)

firm_ids <- unique(primary_ratio_only$corp_code)

train_firms <- sample(
  firm_ids,
  size = floor(length(firm_ids) * 0.8)
)

test_firms <- setdiff(firm_ids, train_firms)

train <- primary_ratio_only %>%
  filter(
    corp_code %in% train_firms,
    bsns_year >= 2015,
    bsns_year <= 2021
  )

valid <- primary_ratio_only %>%
  filter(
    corp_code %in% train_firms,
    bsns_year == 2022
  )

test <- primary_ratio_only %>%
  filter(
    corp_code %in% test_firms,
    bsns_year >= 2023,
    bsns_year <= 2024
  )

