#필요한 라이브러리 다운로드 및 로드

required_packages <- c("tidyverse", "magrittr", "httr", "rvest", "xgboost", 
                       "DMwR", "ggplot2", "dygraphs", "zoo", "purrr", 
                       "jsonlite", "randomForest", "forecast", "pROC", "caret", "DiagrammeR", 
                       "Ckmeans.1d.dp")

installed_packages <- rownames(installed.packages())
packages_to_install <- setdiff(required_packages, installed_packages)

if(length(packages_to_install) > 0) {
  install.packages(packages_to_install)
}

remotes::install_github("cran/DMwR")

lapply(required_packages, library, character.only = TRUE)
library(DMwR)

url1 <- "https://finance.naver.com/sise/trading_halt.naver" 
halt <- read_html(url1, encoding = "euc-kr")

halt_code <- halt %>% 
  html_node("#contentarea") %>% 
  html_nodes("div.box_type_l") %>% 
  html_nodes("table") %>% 
  html_nodes("tr") %>% 
  html_nodes("td:nth-child(2) a") %>%
  html_attr("href") %>%
  str_extract("(?<=code=)\\d+")

halt_date <- halt %>% 
  html_node("#contentarea") %>% 
  html_nodes("div.box_type_l") %>% 
  html_nodes("table") %>% 
  html_nodes("tr") %>% 
  html_nodes("td.center") %>% 
  html_text(trim = T)

halt_html <- map(1:length(halt_code), function(i) {
  url2 <- paste0("https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A", halt_code[i], "&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701")
  read_html(url2, encoding = "utf-8")
})

#재무 데이터의 날짜 정보 가져오기

finance_date <- map(1:length(halt_html), function(i) {
  halt_html[[i]] %>% 
    html_node("#compBody") %>% 
    html_nodes("div.section.ul_de div:nth-child(3) div.um_table thead tr th") %>% 
    html_text() %>%
    .[-1]
})

#거래 정지 날짜에 맞는 재무 데이터 인덱스 가져오기

halt_date_fin <- map(1:length(finance_date), function(i) {
  which(finance_date[[i]] <= halt_date[i])
})

#세부 재무 정보 가져오기

halt_df <- map(1:length(halt_html), function(i) {
  halt_html[[i]] %>% 
    html_nodes("#p_grid1_1 ,#p_grid1_2, #p_grid1_3, #p_grid1_7, #p_grid1_8, #p_grid1_13, #p_grid1_14, #p_grid1_15, #p_grid1_16, #p_grid1_17, #p_grid1_20, #p_grid1_22") %>% 
    html_nodes("td") %>% 
    html_text() %>% 
    as.numeric() %>%
    matrix(ncol = 5, byrow = F) %>% 
    as.data.frame()
})

#재무 항목 이름 가져오기

halt_fi_name <- map(1:length(halt_html), function(i) {
  halt_html[[i]] %>% 
    html_nodes("#p_grid1_1, #p_grid1_2, #p_grid1_3, #p_grid1_7, #p_grid1_8, #p_grid1_13, #p_grid1_14, #p_grid1_15, #p_grid1_16, #p_grid1_17, #p_grid1_20, #p_grid1_22") %>% 
    html_nodes("th") %>% 
    html_nodes("div") %>% 
    html_nodes("div") %>% 
    html_nodes("a") %>% 
    html_nodes("span") %>% 
    html_text(trim = T)
})

#최대주주 변경일 가져오기

json_txt <- map(1:length(halt_code), function(i) {
  url3 <- paste0("https://comp.fnguide.com/SVO2/json/data/01_09_01/A", halt_code[i], ".json?_=1716794852929")
  response <- GET(url3)
  content(response, "text", encoding = "UTF-8")
})

results <- map(1:length(json_txt), function(i) {
  tryCatch(
    fromJSON(json_txt[[i]]) %>% as.data.frame(),
    error = function(e) NULL
  )
})

results_fi <- map(1:length(results), function(i) {
  results[[i]]$comp.CHG_DT[1]
})

finance_date <- sapply(finance_date, "[", 1)

#재무 항목이 12개 미만인 데이터 필터링

name_na <- which(sapply(halt_fi_name, length) < 12)
results_fi <- results_fi[-name_na]
halt_df <- halt_df[-which(sapply(halt_fi_name, length) < 12)]
halt_fi_name <- halt_fi_name[sapply(halt_fi_name, length) >= 12]
halt_date_fin <- halt_date_fin[-name_na]

#유효하지 않은 날짜 데이터 필터링

date_na <- which(sapply(halt_date_fin, length) == 0)
halt_df <- halt_df[-date_na]
halt_fi_name <- halt_fi_name[-date_na]
halt_date_fin <- halt_date_fin[-date_na]
results_fi <- results_fi[-date_na]

#거래 정지일 이전 데이터만 선택

halt_df <- map(1:length(halt_df), function(i) {
  halt_df[[i]][0:length(halt_date_fin[[i]])]
})

#재무 항목 이름 설정

halt_df <- map2(halt_df, halt_fi_name, ~ {
  rownames(.x) <- .y
  .x
})

#결측치 보정 (선형 보간법 및 최전후방 결측치는 앞 뒤 값으로 대체)

halt_df <- map(1:length(halt_df), function(i) {
  df_filled <- apply(halt_df[[i]], 2, function(row) {
    filled_row <- row %>% 
      na.approx(na.rm = FALSE) %>% 
      na.locf(na.rm = FALSE) %>% 
      na.locf(fromLast = TRUE, na.rm = FALSE)
  })
  df_filled <- as.data.frame(df_filled)
  rownames(df_filled) <- rownames(halt_df[[i]])
  return(df_filled)
})

#차분 계산

halt_df_fin <- lapply(halt_df, function(x) {
  if (ncol(x) == 2) {
    apply(x, 1, function(i) diff(i)) %>% as.data.frame()
  } else {
    t(apply(x, 1, function(i) diff(i))) %>% as.data.frame()
  }
})

#유효하지 않은 차분 데이터 필터링

diff_na <- which(sapply(halt_df_fin, ncol) == 0)
halt_df_fin <- halt_df_fin[-diff_na]
halt_fi_name <- halt_fi_name[-diff_na]
results_fi <- results_fi[-diff_na]

#전체 데이터에 최대주주 변경일 추가

halt_df_fin <- map(1:length(halt_df_fin), function(i) {
  rbind(halt_df_fin[[i]], results_fi[[i]])
})

halt_fi_name <- map(1:length(halt_fi_name), function(i) {
  c(halt_fi_name[[i]], "최대주주변동")
})

#전체 데이터 이름 매핑

halt_df_fin <- halt_df_fin[-61]
halt_fi_name <- halt_fi_name[-61]
results_fi <- results_fi[-61]


halt_df_fin <- map2(halt_df_fin, halt_fi_name, ~ {
  rownames(.x) <- .y
  .x
})

#데이터 프레임 형식으로 변환

halt_df_fin <- map_dfr(1:length(halt_df_fin), function(i) {
  t(halt_df_fin[[i]]) %>% as.data.frame()
})

#행 번호 설정

rownames(halt_df_fin) <- 1:nrow(halt_df_fin)

#모델 형식에 맞게 변환

halt_df_fin <- halt_df_fin %>% mutate(months = month(`최대주주변동`), years = year(`최대주주변동`))

halt_df_fin <- apply(halt_df_fin[,-13], 2, function(x) as.numeric(x))

page_url <- paste0('https://finance.naver.com/sise/sise_market_sum.naver?sosok=', 0:1)
page_html <- map(1:2, function(i) { 
  read_html(page_url[i], encoding = "euc-kr")
})

#해당 시장의 마지막 페이지 번호를 가져오는 구문

last_page  <- map(1:2, function(i) {
  html_node(page_html[[i]], "#contentarea") %>%
    html_nodes(css = ".box_type_l") %>%
    html_nodes(css = ".Nnavi") %>%
    html_nodes(css = "tr") %>%
    html_nodes(css = ".pgRR") %>%
    html_nodes("a") %>%
    html_attr("href")
})

last_page <- as.numeric(str_extract(last_page, "\\d+$"))

stock_code <- map(1:2, function(i) {
  map(1:last_page[i], function(o) {
    main_url <- paste0(page_url[i], '&page=', o)
    html <- read_html(main_url, encoding = "euc-kr")
    nodes <- html_node(html, ".type_2") %>%
      html_nodes("tr")
    
    codes <- nodes %>% html_nodes("td:nth-child(2) a") %>% html_attr("href") %>% str_extract("(?<=code=)\\d+")
    return(codes)
  }) %>% unlist()
}) %>% unlist()

set.seed(1004)
random_file <- sample(stock_code, 200)

random_file <- random_file[!random_file %in% halt_code]

#FnGuide 일반 회사 페이지 스크래핑

normal_html <- map(1:length(random_file), function(i) {
  url2 <- paste0("https://comp.fnguide.com/SVO2/ASP/SVD_FinanceRatio.asp?pGB=1&gicode=A", random_file[i], "&cID=&MenuYn=Y&ReportGB=&NewMenuID=104&stkGb=701")
  read_html(url2, encoding = "utf-8")
})

#일반 회사 재무 데이터 가져오기

normal_df <- map(1:length(normal_html), function(i) {
  normal_html[[i]] %>% 
    html_nodes("#p_grid1_1 ,#p_grid1_2, #p_grid1_3, #p_grid1_7, #p_grid1_8, #p_grid1_13, #p_grid1_14, #p_grid1_15, #p_grid1_16, #p_grid1_17, #p_grid1_20, #p_grid1_22") %>% 
    html_nodes("td") %>% 
    html_text() %>% 
    as.numeric() %>%
    matrix(ncol = 5, byrow = F) %>% 
    as.data.frame()
})

#일반 회사 재무 항목 이름 가져오기

normal_fi_name <- map(1:length(normal_html), function(i) {
  normal_html[[i]] %>% 
    html_nodes("#p_grid1_1 ,#p_grid1_2, #p_grid1_3, #p_grid1_7, #p_grid1_8, #p_grid1_13, #p_grid1_14, #p_grid1_15, #p_grid1_16, #p_grid1_17, #p_grid1_20, #p_grid1_22") %>% 
    html_nodes("th") %>% 
    html_nodes("div") %>% 
    html_nodes("div") %>% 
    html_nodes("a") %>% 
    html_nodes("span") %>% 
    html_text(trim = T)
})

#최대주주 변경일 가져오기

json_normal_txt <- map(1:length(random_file), function(i) {
  url3 <- paste0("https://comp.fnguide.com/SVO2/json/data/01_09_01/A", random_file[i], ".json?_=1716794852929")
  response <- GET(url3)
  content(response, "text", encoding = "UTF-8")
})

normal_results <- map(1:length(json_normal_txt), function(i) {
  tryCatch(
    fromJSON(json_normal_txt[[i]]) %>% as.data.frame(),
    error = function(e) NULL
  )
})

normal_results_fi <- map(1:length(normal_results), function(i) {
  normal_results[[i]]$comp.CHG_DT[1]
})

#재무 항목이 12개 미만인 데이터 필터링

na_normal_name <- which(sapply(normal_fi_name, length) < 12)
normal_results_fi <- normal_results_fi[-na_normal_name]
normal_df <- normal_df[-which(sapply(normal_fi_name, length) < 12)]
normal_fi_name <- normal_fi_name[sapply(normal_fi_name, length) >= 12]

#재무 항목 이름 설정

normal_df <- map2(normal_df, normal_fi_name, ~ {
  rownames(.x) <- .y
  .x
})

#결측치 보정 (선형 보간법 및 최전후방 결측치는 앞 뒤 값으로 대체)

normal_df <- lapply(normal_df, function(df) {
  df1 <- apply(df, 1, function(x) x %>% na.approx(na.rm = F) %>% na.locf(na.rm = F) %>% na.locf(fromLast = T, na.rm = F)) %>% t() %>% as.data.frame()
  return(df1)
})

#차분 계산

normal_df_fin <- lapply(normal_df, function(x) {
  apply(x, 1, function(i) diff(i)) %>% as.data.frame()
})

#기존 데이터에 최대주주 변경일 추가

normal_df_fin <- map(1:length(normal_df_fin), function(i) {
  merge(normal_df_fin[[i]], normal_results_fi[[i]])
})

normal_fi_name <- map(1:length(normal_fi_name), function(i) {
  c(normal_fi_name[[i]], "최대주주변동")
})

#유효하지 않은 데이터 필터링

normal_fi_name <- normal_fi_name[-which(sapply(normal_df_fin, nrow) < 4)]
normal_df_fin <- normal_df_fin[-which(sapply(normal_df_fin, nrow) < 4)]

#이름 매핑

normal_df_fin <- map2(normal_df_fin, normal_fi_name, ~ {
  colnames(.x) <- .y
  .x
})

#일반 회사 데이터 통합

normal_df_fin <- bind_rows(normal_df_fin)

#모델 형식에 맞게 변환

normal_df_fin <- normal_df_fin %>% mutate(months = month(`최대주주변동`), years = year(`최대주주변동`))

normal_df_fin <- normal_df_fin[,-13]

#0은 거래 정지, 1은 일반

halt_df_fin <- data.frame(halt_df_fin, survival = 0)
normal_df_fin <- data.frame(normal_df_fin, survival = 1)
total_df <- bind_rows(halt_df_fin, normal_df_fin)

train_indi <- createDataPartition(total_df$survival, p = 0.7, list = FALSE)

train_df <- total_df[train_indi,]

validation_df <- total_df[-train_indi,]

train_label <- train_df$survival
train_mod <- xgb.DMatrix(as.matrix(train_df %>% select(-survival)), label = train_label)

validation_label <- validation_df$survival
validation_mod <- xgb.DMatrix(as.matrix(validation_df %>% select(-survival)))

#랜덤 서치를 위한 하이퍼파라미터 조합 생성

random_params <- data.frame(
  nrounds = sample(50:500, 10),
  max_depth = sample(1:10, 10, replace = TRUE),
  eta = runif(10, min = 0.01, max = 0.3),
  gamma = runif(10, min = 0, max = 5),
  colsample_bytree = runif(10, min = 0.5, max = 1.0),
  min_child_weight = sample(1:10, 10, replace = TRUE),
  subsample = runif(10, min = 0.5, max = 1.0)
)

#랜덤 서치 수행

random_search <- train(
  x = as.matrix(train_df %>% select(-survival)),
  y = as.factor(train_label),
  method = "xgbTree",
  trControl = trainControl(method = "repeatedcv", number = 5, repeats = 5, verboseIter = TRUE),
  tuneGrid = random_params,
  verbose = TRUE
)

#최적 하이퍼파라미터 선택

best_param <- random_search$bestTune

param <- list(
  nrounds = best_param$nrounds,
  max_depth = best_param$max_depth,
  eta = best_param$eta,
  nthread = 4,
  objective = "binary:logistic",
  eval_metric = "auc",
  gamma = best_param$gamma,
  colsample_bytree = best_param$colsample_bytree,
  min_child_weight = best_param$min_child_weight,
  subsample = best_param$subsample
)

#최적의 nround 값 탐색

cv_model <- xgb.cv(params = param, 
                   data = train_mod, 
                   nfold = 5, 
                   nrounds = 1000, 
                   early_stopping_rounds = 10, 
                   verbose = 1, 
                   metrics = "auc")

best_nrounds <- cv_model$best_iteration

model_fin <- xgboost(params = param, data = train_mod, nrounds = best_nrounds, verbose = 1, booster = "dart")

pred_prob <- predict(model_fin, validation_mod, type = "prob")

pred_label <- ifelse(pred_prob > 0.5, 1, 0)

# Feature importance 계산

importance_matrix <- xgb.importance(feature_names = colnames(train_df %>% select(-survival)), model = model_fin)

# Feature importance 시각화

xgb.plot.importance(importance_matrix, main = "Feature Importance (Trading Halt Model)")

xgb.plot.tree(model = model_fin, trees = 1)

xgb.plot.multi.trees(model = model_fin)

xgb.ggplot.shap.summary(data = as.matrix(total_df[,-15]), model = model_fin) +
  labs(y = "SHAP Value", color = "feature value") +
  theme(legend.title = element_text(size = 10))

#예측 레이블과 실제 레이블의 혼동 행렬 계산

conf_matrix <- confusionMatrix(as.factor(pred_label), as.factor(validation_label))
print(conf_matrix)

#ROC 계산 및 AUC 값 추출

roc_obj <- roc(validation_label, pred_prob)

auc_val <- auc(roc_obj)

#ROC 시각화

plot.roc(roc_obj,
         main = "ROC Curve",
         print.auc = TRUE,
         auc.polygon = TRUE,
         grid = TRUE,
         col = "red",
         lwd = 2,
         print.thres = TRUE,
         print.thres.cex = 0.8,
         max.auc.polygon = T)

train_label <- as.data.frame(train_df$survival)
write.csv(train_df %>% select(-survival), "train_df.csv", row.names = FALSE)
write.csv(train_label, "train_label.csv", row.names = FALSE)
test_label <- validation_df$survival
write.csv(test_label, "test_label.csv", row.names = FALSE)
write.csv(validation_df %>% select(-survival), "valid_df.csv", row.names = FALSE)
