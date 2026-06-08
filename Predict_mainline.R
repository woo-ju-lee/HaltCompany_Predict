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
library(plotly)
library(htmlwidgets)
library(scales)

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

feature_cols <- setdiff(colnames(train), "corp_code")

stopifnot(all(vapply(train[feature_cols], is.numeric, logical(1))))
stopifnot(all(vapply(valid[feature_cols], is.numeric, logical(1))))
stopifnot(all(vapply(test[feature_cols],  is.numeric, logical(1))))

x_train <- as.matrix(train[, feature_cols])
x_valid <- as.matrix(valid[, feature_cols])
x_test  <- as.matrix(test[, feature_cols])

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

eval_log <- attr(model, "evaluation_log") %>% as_tibble()

best_iter <- if (!is.null(model$best_iteration)) {
  model$best_iteration
} else {
  nrow(eval_log)
}

p_learning <- plot_ly(eval_log, x = ~iter) %>%
  add_lines(
    y = ~train_aucpr,
    name = "Train AUCPR",
    text = ~paste0(
      "iter: ", iter,
      "<br>train_aucpr: ", round(train_aucpr, 5)
    ),
    hoverinfo = "text"
  ) %>%
  add_lines(
    y = ~valid_aucpr,
    name = "Validation AUCPR",
    text = ~paste0(
      "iter: ", iter,
      "<br>valid_aucpr: ", round(valid_aucpr, 5)
    ),
    hoverinfo = "text"
  ) %>%
  layout(
    title = "XGBoost Learning Curve - AUCPR",
    xaxis = list(title = "Iteration"),
    yaxis = list(title = "AUCPR"),
    shapes = list(
      list(
        type = "line",
        x0 = best_iter,
        x1 = best_iter,
        y0 = 0,
        y1 = 1,
        xref = "x",
        yref = "paper",
        line = list(dash = "dash")
      )
    )
  )

p_learning

result <- predict(model, dtest)

results <- ifelse(result >= 0.02, 1, 0) %>% as.factor()

confusionMatrix(results, test_halt,
                positive = "1",
                mode = "prec_recall")

importance <- xgb.importance(model = model) %>% as_tibble()

p_importance <- importance %>%
  slice_max(Gain, n = 25) %>%
  arrange(Gain) %>%
  mutate(Feature = factor(Feature, levels = Feature)) %>%
  plot_ly(
    x = ~Gain,
    y = ~Feature,
    type = "bar",
    orientation = "h",
    text = ~paste0(
      "Feature: ", Feature,
      "<br>Gain: ", round(Gain, 5),
      "<br>Cover: ", round(Cover, 5),
      "<br>Frequency: ", round(Frequency, 5)
    ),
    hoverinfo = "text"
  ) %>%
  layout(
    title = "Top 25 Feature Importance - Gain",
    xaxis = list(title = "Gain"),
    yaxis = list(title = "")
  )

p_importance

metric_at_threshold <- function(truth, prob, threshold) {
  truth <- if (is.factor(truth)) as.integer(as.character(truth)) else as.integer(truth)
  pred  <- as.integer(prob >= threshold)
  
  tp <- sum(pred == 1 & truth == 1, na.rm = TRUE)
  fp <- sum(pred == 1 & truth == 0, na.rm = TRUE)
  tn <- sum(pred == 0 & truth == 0, na.rm = TRUE)
  fn <- sum(pred == 0 & truth == 1, na.rm = TRUE)
  
  precision <- if ((tp + fp) == 0) NA_real_ else tp / (tp + fp)
  recall    <- if ((tp + fn) == 0) NA_real_ else tp / (tp + fn)
  specificity <- if ((tn + fp) == 0) NA_real_ else tn / (tn + fp)
  accuracy  <- (tp + tn) / (tp + tn + fp + fn)
  
  f1 <- if (
    is.na(precision) || is.na(recall) || (precision + recall) == 0
  ) {
    NA_real_
  } else {
    2 * precision * recall / (precision + recall)
  }
  
  tibble(
    threshold = threshold,
    TP = tp,
    FP = fp,
    TN = tn,
    FN = fn,
    precision = precision,
    recall = recall,
    specificity = specificity,
    accuracy = accuracy,
    f1 = f1,
    predicted_positive_rate = mean(pred == 1, na.rm = TRUE)
  )
}

valid_prob <- predict(model, dvalid)
test_prob  <- predict(model, dtest)

threshold_grid <- seq(0, 1, by = 0.001)

valid_metric <- map_dfr(
  threshold_grid,
  ~ metric_at_threshold(valid_halt, valid_prob, .x)
)

best_threshold <- valid_metric %>%
  filter(!is.na(f1)) %>%
  arrange(desc(f1), desc(precision), desc(recall)) %>%
  slice(1) %>%
  pull(threshold)

# validation에 positive가 전혀 없으면 F1 threshold 산출 불가
if (length(best_threshold) == 0 || is.na(best_threshold)) {
  best_threshold <- 0.02
}

best_threshold

p_threshold <- valid_metric %>%
  select(threshold, precision, recall, f1, predicted_positive_rate) %>%
  pivot_longer(
    cols = -threshold,
    names_to = "metric",
    values_to = "value"
  ) %>%
  plot_ly(
    x = ~threshold,
    y = ~value,
    color = ~metric,
    type = "scatter",
    mode = "lines",
    text = ~paste0(
      "threshold: ", round(threshold, 4),
      "<br>metric: ", metric,
      "<br>value: ", round(value, 5)
    ),
    hoverinfo = "text"
  ) %>%
  layout(
    title = paste0("Validation Threshold Trade-off - selected threshold = ", round(best_threshold, 4)),
    xaxis = list(title = "Threshold"),
    yaxis = list(title = "Metric", range = c(0, 1)),
    shapes = list(
      list(
        type = "line",
        x0 = best_threshold,
        x1 = best_threshold,
        y0 = 0,
        y1 = 1,
        xref = "x",
        yref = "paper",
        line = list(dash = "dash")
      )
    )
  )

p_threshold

test_pred_tbl <- test %>%
  select(corp_code, bsns_year) %>%
  mutate(
    row_id = row_number(),
    actual = factor(test_halt, levels = c(0, 1)),
    pred_prob = test_prob,
    pred = factor(as.integer(pred_prob >= best_threshold), levels = c(0, 1)),
    correct = actual == pred
  )

confusionMatrix(
  data = test_pred_tbl$pred,
  reference = test_pred_tbl$actual,
  positive = "1",
  mode = "prec_recall"
)

cm <- table(
  Predicted = test_pred_tbl$pred,
  Actual = test_pred_tbl$actual
)

cm_text <- matrix(
  "",
  nrow = nrow(cm),
  ncol = ncol(cm),
  dimnames = dimnames(cm)
)

for (i in seq_len(nrow(cm))) {
  for (j in seq_len(ncol(cm))) {
    cm_text[i, j] <- paste0(
      "Predicted: ", rownames(cm)[i],
      "<br>Actual: ", colnames(cm)[j],
      "<br>N: ", cm[i, j]
    )
  }
}

annotations <- list()
for (i in seq_len(nrow(cm))) {
  for (j in seq_len(ncol(cm))) {
    annotations <- append(
      annotations,
      list(
        list(
          x = colnames(cm)[j],
          y = rownames(cm)[i],
          text = as.character(cm[i, j]),
          showarrow = FALSE
        )
      )
    )
  }
}

p_confusion <- plot_ly(
  x = colnames(cm),
  y = rownames(cm),
  z = cm,
  type = "heatmap",
  text = cm_text,
  hoverinfo = "text"
) %>%
  layout(
    title = "Test Confusion Matrix",
    xaxis = list(title = "Actual"),
    yaxis = list(title = "Predicted"),
    annotations = annotations
  )

p_confusion

p_pred_dist <- plot_ly(
  test_pred_tbl,
  x = ~pred_prob,
  color = ~actual,
  type = "histogram",
  nbinsx = 50,
  opacity = 0.65,
  text = ~paste0(
    "actual: ", actual,
    "<br>pred_prob: ", round(pred_prob, 5)
  ),
  hoverinfo = "text"
) %>%
  layout(
    title = "Test Predicted Probability Distribution by Actual Label",
    xaxis = list(title = "Predicted probability"),
    yaxis = list(title = "Count"),
    barmode = "overlay",
    shapes = list(
      list(
        type = "line",
        x0 = best_threshold,
        x1 = best_threshold,
        y0 = 0,
        y1 = 1,
        xref = "x",
        yref = "paper",
        line = list(dash = "dash")
      )
    )
  )

p_pred_dist

shap_mat <- predict(model, dtest, predcontrib = TRUE)

if (length(dim(shap_mat)) != 2) {
  stop("SHAP output이 2차원 matrix가 아닙니다. objective 또는 xgboost 버전을 확인하세요.")
}

if (is.null(colnames(shap_mat))) {
  colnames(shap_mat) <- c(feature_cols, "BIAS")
}

# 마지막 컬럼은 BIAS/intercept 성격
shap_feature_cols <- colnames(shap_mat)[seq_len(ncol(shap_mat) - 1)]

shap_long <- as_tibble(
  shap_mat[, shap_feature_cols, drop = FALSE],
  .name_repair = "minimal"
) %>%
  mutate(row_id = row_number()) %>%
  pivot_longer(
    cols = -row_id,
    names_to = "feature",
    values_to = "shap_value"
  )

value_long <- as_tibble(
  x_test,
  .name_repair = "minimal"
) %>%
  mutate(row_id = row_number()) %>%
  pivot_longer(
    cols = -row_id,
    names_to = "feature",
    values_to = "feature_value"
  )

shap_long <- shap_long %>%
  left_join(value_long, by = c("row_id", "feature")) %>%
  left_join(
    test_pred_tbl %>%
      select(row_id, corp_code, bsns_year, actual, pred_prob),
    by = "row_id"
  )

top_shap <- shap_long %>%
  group_by(feature) %>%
  summarise(
    mean_abs_shap = mean(abs(shap_value), na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(mean_abs_shap))

p_shap_bar <- top_shap %>%
  slice_max(mean_abs_shap, n = 25) %>%
  arrange(mean_abs_shap) %>%
  mutate(feature = factor(feature, levels = feature)) %>%
  plot_ly(
    x = ~mean_abs_shap,
    y = ~feature,
    type = "bar",
    orientation = "h",
    text = ~paste0(
      "feature: ", feature,
      "<br>mean |SHAP|: ", round(mean_abs_shap, 5)
    ),
    hoverinfo = "text"
  ) %>%
  layout(
    title = "Top 25 Mean Absolute SHAP Values",
    xaxis = list(title = "Mean absolute SHAP value"),
    yaxis = list(title = "")
  )

p_shap_bar

top_features_15 <- top_shap %>%
  slice_max(mean_abs_shap, n = 15) %>%
  arrange(mean_abs_shap) %>%
  pull(feature)

shap_plot_tbl <- shap_long %>%
  filter(feature %in% top_features_15) %>%
  mutate(feature = factor(feature, levels = top_features_15))

# 점이 너무 많으면 브라우저가 느려질 수 있으므로 샘플링
if (nrow(shap_plot_tbl) > 5000) {
  set.seed(1004)
  shap_plot_tbl <- shap_plot_tbl %>% slice_sample(n = 5000)
}

as_num_safe <- function(x) {
  if (is.factor(x)) {
    x <- as.character(x)
  }
  
  if (is.character(x)) {
    readr::parse_number(x, locale = readr::locale(grouping_mark = ","))
  } else {
    as.numeric(x)
  }
}

top_features_15 <- top_shap %>%
  slice_max(mean_abs_shap, n = 15) %>%
  arrange(mean_abs_shap) %>%
  pull(feature)

# feature를 factor로 직접 넘기지 말고,
# 숫자 y축 + tick label 방식으로 처리
feature_axis <- tibble(
  feature = top_features_15,
  feature_id = seq_along(top_features_15)
)

shap_plot_tbl <- shap_long %>%
  filter(feature %in% top_features_15) %>%
  mutate(
    feature = as.character(feature),
    shap_value = as.numeric(shap_value),
    feature_value_num = as_num_safe(feature_value),
    corp_code = as.character(corp_code),
    actual = as.character(actual),
    pred_prob = as.numeric(pred_prob)
  ) %>%
  left_join(feature_axis, by = "feature")

# 점이 너무 많으면 브라우저가 느려질 수 있으므로 샘플링
if (nrow(shap_plot_tbl) > 5000) {
  set.seed(1004)
  shap_plot_tbl <- shap_plot_tbl %>% slice_sample(n = 5000)
}

# y축 겹침 완화용 jitter
set.seed(1004)
shap_plot_tbl <- shap_plot_tbl %>%
  mutate(
    feature_y = feature_id + runif(n(), min = -0.25, max = 0.25)
  )

p_shap_summary <- plot_ly(
  data = shap_plot_tbl,
  x = ~shap_value,
  y = ~feature_y,
  type = "scatter",
  mode = "markers",
  marker = list(
    color = ~feature_value_num,
    colorscale = "Viridis",
    showscale = TRUE,
    colorbar = list(title = "Feature value"),
    opacity = 0.55,
    size = 6
  ),
  text = ~paste0(
    "corp_code: ", corp_code,
    "<br>year: ", bsns_year,
    "<br>feature: ", feature,
    "<br>feature value: ", signif(feature_value_num, 5),
    "<br>SHAP: ", signif(shap_value, 5),
    "<br>pred_prob: ", scales::percent(pred_prob, accuracy = 0.01),
    "<br>actual: ", actual
  ),
  hoverinfo = "text"
) %>%
  layout(
    title = "SHAP Summary Plot - Test Set",
    xaxis = list(
      title = "SHAP contribution to log-odds margin",
      zeroline = TRUE
    ),
    yaxis = list(
      title = "",
      tickmode = "array",
      tickvals = feature_axis$feature_id,
      ticktext = feature_axis$feature
    )
  )

p_shap_summary

target_row <- test_pred_tbl %>%
  arrange(desc(pred_prob)) %>%
  slice(1) %>%
  pull(row_id)

one_case <- shap_long %>%
  filter(row_id == target_row) %>%
  mutate(abs_shap = abs(shap_value)) %>%
  slice_max(abs_shap, n = 20) %>%
  arrange(shap_value) %>%
  mutate(feature = factor(feature, levels = feature))

case_meta <- test_pred_tbl %>%
  filter(row_id == target_row)

p_one_case <- plot_ly(
  one_case,
  x = ~shap_value,
  y = ~feature,
  type = "bar",
  orientation = "h",
  text = ~paste0(
    "feature: ", feature,
    "<br>feature value: ", signif(feature_value, 5),
    "<br>SHAP: ", signif(shap_value, 5)
  ),
  hoverinfo = "text"
) %>%
  layout(
    title = paste0(
      "Individual SHAP Decomposition - corp_code: ",
      case_meta$corp_code,
      ", year: ",
      case_meta$bsns_year,
      ", pred_prob: ",
      round(case_meta$pred_prob, 5)
    ),
    xaxis = list(title = "SHAP contribution to log-odds margin"),
    yaxis = list(title = "")
  )

p_one_case

test_stock_info <- dbGetQuery(
  con,
  glue_sql(
    "
    SELECT stock_name, corp_code, stock_code
    FROM STOCK_INFO
    WHERE corp_code IN ({unique(test$corp_code)*})
    ",
    .con = con
  )
) %>%
  mutate(corp_code = as.character(corp_code))

test_pred_tbl_named <- test_pred_tbl %>%
  mutate(corp_code = as.character(corp_code)) %>%
  left_join(test_stock_info, by = "corp_code")

risk_rank_named <- test_pred_tbl_named %>%
  arrange(desc(pred_prob)) %>%
  transmute(
    stock_name,
    stock_code,
    corp_code,
    bsns_year,
    pred_prob = round(pred_prob, 6),
    actual,
    pred,
    correct
  ) %>%
  head(50)

p_risk_table_named <- plot_ly(
  type = "table",
  header = list(values = colnames(risk_rank_named)),
  cells = list(values = unname(lapply(risk_rank_named, as.character)))
) %>%
  layout(
    title = "Top 50 High-risk Companies"
  )

p_risk_table_named

dashboard_main <- subplot(
  p_learning,
  p_importance,
  p_threshold,
  p_confusion,
  p_pred_dist,
  p_shap_bar,
  nrows = 3,
  margin = 0.06,
  titleX = TRUE,
  titleY = TRUE
) %>%
  layout(
    title = "XGBoost Diagnostic Dashboard - Trading Halt Prediction"
  )

dashboard_main
