library(themis)

train$target <- as.factor(train_halt)

library(dplyr)
library(caret)

train_name <- colnames(train)

knn_impute_caret_by_group <- function(
    train,
    newdata,
    group_cols,
    impute_cols,
    knn_cols = NULL,
    k = 5,
    outcome_cols = c("target")
) {
  impute_cols <- as.character(impute_cols)
  
  if (is.null(knn_cols)) {
    knn_cols <- impute_cols
  }
  
  knn_cols <- as.character(knn_cols)
  
  # 그룹 변수, 종속변수 제거
  impute_cols <- setdiff(impute_cols, c(group_cols, outcome_cols))
  knn_cols <- setdiff(knn_cols, c(group_cols, outcome_cols))
  
  use_cols <- unique(c(impute_cols, knn_cols))
  use_cols <- intersect(use_cols, names(train))
  use_cols <- intersect(use_cols, names(newdata))
  
  # caret knnImpute는 numeric만 가능
  numeric_cols <- use_cols[sapply(train[use_cols], is.numeric)]
  use_cols <- numeric_cols
  impute_cols <- intersect(impute_cols, use_cols)
  
  if (length(impute_cols) == 0) {
    stop("impute_cols에 사용할 수 있는 numeric 변수가 없습니다.")
  }
  
  fit_pp <- function(ref_data, cols) {
    if (nrow(ref_data) < 2) return(NULL)
    
    x <- ref_data[, cols, drop = FALSE]
    x <- as.data.frame(x)
    
    # 비결측값이 2개 미만인 열은 KNN 불가능
    valid_cols <- names(x)[sapply(x, function(v) sum(!is.na(v)) >= 2)]
    x <- x[, valid_cols, drop = FALSE]
    
    if (nrow(x) < 2 || ncol(x) < 2) {
      return(NULL)
    }
    
    # zero variance 열 제거
    zero_var_cols <- names(x)[sapply(x, function(v) {
      v2 <- v[!is.na(v)]
      length(unique(v2)) <= 1
    })]
    
    x <- x[, setdiff(names(x), zero_var_cols), drop = FALSE]
    
    if (nrow(x) < 2 || ncol(x) < 2) {
      return(NULL)
    }
    
    # 핵심: 각 열별 비결측 개수보다 k가 크면 안 됨
    min_non_missing <- min(colSums(!is.na(x)))
    k_eff <- min(k, min_non_missing)
    
    if (k_eff < 1) {
      return(NULL)
    }
    
    pp <- tryCatch(
      preProcess(
        x,
        method = c("center", "scale", "knnImpute"),
        k = k_eff
      ),
      error = function(e) NULL
    )
    
    if (is.null(pp)) {
      return(NULL)
    }
    
    list(
      pp = pp,
      cols = names(x),
      k_eff = k_eff
    )
  }
  
  # caret knnImpute 결과를 원래 스케일로 복원
  inverse_scale <- function(x_imp, pp) {
    mean_vec <- pp$mean
    sd_vec <- pp$std
    
    restore_cols <- intersect(names(x_imp), names(mean_vec))
    restore_cols <- intersect(restore_cols, names(sd_vec))
    
    for (col in restore_cols) {
      x_imp[[col]] <- x_imp[[col]] * sd_vec[[col]] + mean_vec[[col]]
    }
    
    x_imp
  }
  
  apply_pp <- function(g, fit, impute_cols) {
    if (is.null(fit)) {
      return(g)
    }
    
    x_new <- g[, fit$cols, drop = FALSE]
    x_new <- as.data.frame(x_new)
    
    x_imp <- tryCatch(
      predict(fit$pp, x_new),
      error = function(e) NULL
    )
    
    if (is.null(x_imp)) {
      return(g)
    }
    
    x_imp <- inverse_scale(x_imp, fit$pp)
    
    copy_cols <- intersect(impute_cols, names(x_imp))
    
    for (col in copy_cols) {
      na_idx <- is.na(g[[col]])
      
      if (any(na_idx)) {
        g[[col]][na_idx] <- x_imp[[col]][na_idx]
      }
    }
    
    g
  }
  
  fill_remaining <- function(g, ref_data, impute_cols) {
    if (nrow(ref_data) == 0) return(g)
    
    for (col in impute_cols) {
      if (!col %in% names(g)) next
      
      na_idx <- is.na(g[[col]])
      if (!any(na_idx)) next
      
      ref_values <- ref_data[[col]]
      ref_values <- ref_values[!is.na(ref_values)]
      
      if (length(ref_values) == 0) next
      
      # 0/1 dummy면 최빈값, 아니면 median
      if (all(ref_values %in% c(0, 1))) {
        fill_value <- as.numeric(names(sort(table(ref_values), decreasing = TRUE))[1])
      } else {
        fill_value <- median(ref_values, na.rm = TRUE)
      }
      
      g[[col]][na_idx] <- fill_value
    }
    
    g
  }
  
  global_fit <- fit_pp(train, use_cols)
  
  newdata2 <- newdata %>%
    mutate(.row_id_knn = row_number())
  
  split_newdata <- newdata2 %>%
    group_by(across(all_of(group_cols))) %>%
    group_split(.keep = TRUE)
  
  result <- lapply(split_newdata, function(g) {
    key <- g %>%
      distinct(across(all_of(group_cols)))
    
    train_g <- train %>%
      semi_join(key, by = group_cols)
    
    group_fit <- fit_pp(train_g, use_cols)
    
    # 그룹별 KNN 불가능하면 전체 train 기준 KNN 사용
    if (is.null(group_fit)) {
      group_fit <- global_fit
    }
    
    g <- apply_pp(g, group_fit, impute_cols)
    
    # KNN으로 안 채워진 값은 그룹 median/mode로 보완
    g <- fill_remaining(g, train_g, impute_cols)
    
    # 그래도 안 채워진 값은 전체 train median/mode로 보완
    g <- fill_remaining(g, train, impute_cols)
    
    g
  })
  
  bind_rows(result) %>%
    arrange(.row_id_knn) %>%
    select(-.row_id_knn)
}

train_imp <- knn_impute_caret_by_group(
  train = train,
  newdata = train,
  group_cols = "corp_code",
  impute_cols = train_name,
  knn_cols = train_name,
  k = 3,
  outcome_cols = "target"
)

bsmote_train <- themis::bsmote(train_imp[, -1], var = "target", k = 3)

train_halt <- bsmote_train$target %>% as.numeric() - 1

bsmote_train <- as.matrix(bsmote_train %>% select(-target))
train_halt <- as.matrix(train_halt)

valid_halt <- ifelse(valid$corp_code %in% names$corp_code == TRUE, 1, 0)
test_halt <- ifelse(test$corp_code %in% names$corp_code == TRUE, 1, 0) %>% as.factor()

pos <- sum(train_halt == 1)
neg <- sum(train_halt == 0)

stopifnot(pos > 0, neg > 0)

scale_pos_weight_value <- neg / pos

dtrain <- xgb.DMatrix(as.matrix(bsmote_train %>% select(-target)), label = train_halt, missing = NA)
dvalid <- xgb.DMatrix(as.matrix(valid %>% select(-corp_code)), label = valid_halt, missing = NA)
dtest <- xgb.DMatrix(as.matrix(test %>% select(-corp_code)), missing = NA)

params <- xgb.params(
  objective = "binary:logistic",
  eval_metric = "aucpr",
  max_depth = 7,
  learning_rate = 0.4,
  subsample = 0.8,
  scale_pos_weight = scale_pos_weight_value,
  colsample_bytree = 0.8,
  min_child_weight = 3,
  max_delta_step = 1,
  nthread = 4
)

models <- xgb.train(
  params = params,
  data = dtrain,
  nrounds = 1000,
  evals = list(train = dtrain, valid = dvalid),
  early_stopping_rounds = 50,
  verbose = 1
)

results <- predict(models, dtest)

results <- ifelse(results > 0.5, 1, 0) %>% as.factor()

confusionMatrix(results, test_halt,
                positive = "1",
                mode = "prec_recall")

models_importance <- xgb.importance(model = models)
xgb.ggplot.importance(models_importance)

knn_impute_caret_by_group <- function(
    train,
    newdata,
    group_cols,
    impute_cols,
    knn_cols = NULL,
    k = 5,
    outcome_cols = c("target")
) {
  impute_cols <- as.character(impute_cols)
  
  if (is.null(knn_cols)) {
    knn_cols <- impute_cols
  }
  
  knn_cols <- as.character(knn_cols)
  
  # 그룹 변수, 종속변수 제거
  impute_cols <- setdiff(impute_cols, c(group_cols, outcome_cols))
  knn_cols <- setdiff(knn_cols, c(group_cols, outcome_cols))
  
  use_cols <- unique(c(impute_cols, knn_cols))
  use_cols <- intersect(use_cols, names(train))
  use_cols <- intersect(use_cols, names(newdata))
  
  # caret knnImpute는 numeric만 가능
  numeric_cols <- use_cols[sapply(train[use_cols], is.numeric)]
  use_cols <- numeric_cols
  impute_cols <- intersect(impute_cols, use_cols)
  
  if (length(impute_cols) == 0) {
    stop("impute_cols에 사용할 수 있는 numeric 변수가 없습니다.")
  }
  
  fit_pp <- function(ref_data, cols) {
    if (nrow(ref_data) < 2) return(NULL)
    
    x <- ref_data[, cols, drop = FALSE]
    x <- as.data.frame(x)
    
    # 비결측값이 2개 미만인 열은 KNN 불가능
    valid_cols <- names(x)[sapply(x, function(v) sum(!is.na(v)) >= 2)]
    x <- x[, valid_cols, drop = FALSE]
    
    if (nrow(x) < 2 || ncol(x) < 2) {
      return(NULL)
    }
    
    # zero variance 열 제거
    zero_var_cols <- names(x)[sapply(x, function(v) {
      v2 <- v[!is.na(v)]
      length(unique(v2)) <= 1
    })]
    
    x <- x[, setdiff(names(x), zero_var_cols), drop = FALSE]
    
    if (nrow(x) < 2 || ncol(x) < 2) {
      return(NULL)
    }
    
    # 핵심: 각 열별 비결측 개수보다 k가 크면 안 됨
    min_non_missing <- min(colSums(!is.na(x)))
    k_eff <- min(k, min_non_missing)
    
    if (k_eff < 1) {
      return(NULL)
    }
    
    pp <- tryCatch(
      preProcess(
        x,
        method = c("center", "scale", "knnImpute"),
        k = k_eff
      ),
      error = function(e) NULL
    )
    
    if (is.null(pp)) {
      return(NULL)
    }
    
    list(
      pp = pp,
      cols = names(x),
      k_eff = k_eff
    )
  }
  
  # caret knnImpute 결과를 원래 스케일로 복원
  inverse_scale <- function(x_imp, pp) {
    mean_vec <- pp$mean
    sd_vec <- pp$std
    
    restore_cols <- intersect(names(x_imp), names(mean_vec))
    restore_cols <- intersect(restore_cols, names(sd_vec))
    
    for (col in restore_cols) {
      x_imp[[col]] <- x_imp[[col]] * sd_vec[[col]] + mean_vec[[col]]
    }
    
    x_imp
  }
  
  apply_pp <- function(g, fit, impute_cols) {
    if (is.null(fit)) {
      return(g)
    }
    
    x_new <- g[, fit$cols, drop = FALSE]
    x_new <- as.data.frame(x_new)
    
    x_imp <- tryCatch(
      predict(fit$pp, x_new),
      error = function(e) NULL
    )
    
    if (is.null(x_imp)) {
      return(g)
    }
    
    x_imp <- inverse_scale(x_imp, fit$pp)
    
    copy_cols <- intersect(impute_cols, names(x_imp))
    
    for (col in copy_cols) {
      na_idx <- is.na(g[[col]])
      
      if (any(na_idx)) {
        g[[col]][na_idx] <- x_imp[[col]][na_idx]
      }
    }
    
    g
  }
  
  fill_remaining <- function(g, ref_data, impute_cols) {
    if (nrow(ref_data) == 0) return(g)
    
    for (col in impute_cols) {
      if (!col %in% names(g)) next
      
      na_idx <- is.na(g[[col]])
      if (!any(na_idx)) next
      
      ref_values <- ref_data[[col]]
      ref_values <- ref_values[!is.na(ref_values)]
      
      if (length(ref_values) == 0) next
      
      # 0/1 dummy면 최빈값, 아니면 median
      if (all(ref_values %in% c(0, 1))) {
        fill_value <- as.numeric(names(sort(table(ref_values), decreasing = TRUE))[1])
      } else {
        fill_value <- median(ref_values, na.rm = TRUE)
      }
      
      g[[col]][na_idx] <- fill_value
    }
    
    g
  }
  
  global_fit <- fit_pp(train, use_cols)
  
  newdata2 <- newdata %>%
    mutate(.row_id_knn = row_number())
  
  split_newdata <- newdata2 %>%
    group_by(across(all_of(group_cols))) %>%
    group_split(.keep = TRUE)
  
  result <- lapply(split_newdata, function(g) {
    key <- g %>%
      distinct(across(all_of(group_cols)))
    
    train_g <- train %>%
      semi_join(key, by = group_cols)
    
    group_fit <- fit_pp(train_g, use_cols)
    
    # 그룹별 KNN 불가능하면 전체 train 기준 KNN 사용
    if (is.null(group_fit)) {
      group_fit <- global_fit
    }
    
    g <- apply_pp(g, group_fit, impute_cols)
    
    # KNN으로 안 채워진 값은 그룹 median/mode로 보완
    g <- fill_remaining(g, train_g, impute_cols)
    
    # 그래도 안 채워진 값은 전체 train median/mode로 보완
    g <- fill_remaining(g, train, impute_cols)
    
    g
  })
  
  bind_rows(result) %>%
    arrange(.row_id_knn) %>%
    select(-.row_id_knn)
}

train_name <- colnames(train)[-c(1:2, 19)]

train_imp <- knn_impute_caret_by_group(
  train = train,
  newdata = train,
  group_cols = "corp_code",
  impute_cols = train_name,
  knn_cols = train_name,
  k = 3,
  outcome_cols = "target"
)
