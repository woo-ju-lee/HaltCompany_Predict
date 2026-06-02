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
library(glue)
library(caret)
library(Ckmeans.1d.dp)

###함수 생성



###동작 라인
primary_ratio_only$bsns_year <- as.numeric(primary_ratio_only$bsns_year)

primary_ratio_only <- primary_ratio_only %>%
  mutate(across(where(is.numeric), ~ replace(.x, is.infinite(.x), NA_real_)))


set.seed(1004)

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

halt_company_name <- map(1:2, function(i) {
  halt_company_list_krx(i)
}) %>% unlist()

names <- dbGetQuery(con, glue_sql("
           SELECT stock_name, corp_code, stock_code
           FROM STOCK_INFO
           WHERE stock_name IN ({halt_company_name*})
           ", .con = con)
           )

halt_company_name %in% names$stock_name

train_halt <- ifelse(train$corp_code %in% names$corp_code == TRUE, 1, 0)
valid_halt <- ifelse(valid$corp_code %in% names$corp_code == TRUE, 1, 0)
test_halt <- ifelse(test$corp_code %in% names$corp_code == TRUE, 1, 0) %>% as.factor()

dtrain <- xgb.DMatrix(as.matrix(train %>% select(-corp_code)), label = train_halt, missing = NA)
dvalid <- xgb.DMatrix(as.matrix(valid %>% select(-corp_code)), label = valid_halt, missing = NA)
dtest <- xgb.DMatrix(as.matrix(test %>% select(-corp_code)), missing = NA)

params <- list(
  objective = "binary:logistic",
  eval_metric = "aucpr",
  max_depth = 3,
  eta = 0.03,
  subsample = 0.8,
  colsample_bytree = 0.8,
  min_child_weight = 10,
  gamma = 1,
  lambda = 5,
  alpha = 0,
  max_delta_step = 1,
  nthread = 4
)

model <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 5000,
  evals = list(train = dtrain, valid = dvalid),
  early_stopping_rounds = 200,
  verbose = 1
)

eval_log <- attr(model, "evaluation_log")

ggplot(eval_log, aes(x = iter)) +
  geom_line(aes(y = train_aucpr, color = "Train"), size = 1) +
  geom_line(aes(y = valid_aucpr, color = "Validation"), size = 1) +
  labs(
    title = "XGBoost Learning Curve (AUCPR)",
    x = "Iterations (nrounds)",
    y = "AUCPR Score",
    color = "Dataset"
  ) +
  theme_minimal()

result <- predict(model, dtest)

results <- ifelse(result >= 0.02, 1, 0) %>% as.factor()

confusionMatrix(results, test_halt,
                positive = "1",
                mode = "prec_recall")

models_importance <- xgb.importance(model = models)
xgb.ggplot.importance(models_importance)
