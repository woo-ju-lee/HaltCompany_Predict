make_rank_data <- function(df, halt_codes) {
  df %>%
    mutate(y = as.integer(corp_code %in% halt_codes)) %>%
    arrange(bsns_year, corp_code)
}

train_rank <- make_rank_data(train, names$corp_code)
valid_rank <- make_rank_data(valid, names$corp_code)
test_rank  <- make_rank_data(test,  names$corp_code)

x_cols <- setdiff(
  names(train_rank),
  c("corp_code", "bsns_year", "y")
)

make_rank_dmatrix <- function(rank_df, x_cols) {
  rank_df <- rank_df %>%
    arrange(bsns_year, corp_code)
  
  dmat <- xgb.DMatrix(
    data = as.matrix(rank_df %>% select(all_of(x_cols))),
    label = rank_df$y,
    missing = NA
  )
  
  group_info <- rank_df %>%
    count(bsns_year, name = "n") %>%
    pull(n)
  
  setinfo(dmat, "group", group_info)
  
  dmat
}

dtrain_rank <- make_rank_dmatrix(train_rank, x_cols)
dvalid_rank <- make_rank_dmatrix(valid_rank, x_cols)
dtest_rank  <- make_rank_dmatrix(test_rank,  x_cols)

rank_params <- list(
  objective = "rank:ndcg",
  eval_metric = "ndcg@50",
  
  max_depth = 2,
  eta = 0.03,
  min_child_weight = 20,
  gamma = 3,
  lambda = 10,
  alpha = 0.1,
  
  subsample = 0.7,
  colsample_bytree = 0.7,
  
  seed = 1004,
  nthread = 4
)

rank_model <- xgb.train(
  params = rank_params,
  data = dtrain_rank,
  nrounds = 3000,
  evals = list(train = dtrain_rank, valid = dvalid_rank),
  early_stopping_rounds = 100,
  verbose = 1
)

rank_log <- attr(rank_model, "evaluation_log")

ggplot(rank_log, aes(x = iter)) +
  geom_line(aes(y = `train_ndcg@50`, color = "Train"), linewidth = 1) +
  geom_line(aes(y = `valid_ndcg@50`, color = "Validation"), linewidth = 1) +
  labs(
    title = "Ranking Model Learning Curve",
    x = "Iterations",
    y = "NDCG@50",
    color = "Dataset"
  ) +
  theme_minimal()

test_score <- predict(rank_model, dtest_rank)

test_scored <- test_rank %>%
  mutate(score = test_score) %>%
  arrange(bsns_year, desc(score))

top50_by_year <- test_scored %>%
  group_by(bsns_year) %>%
  arrange(desc(score), .by_group = TRUE) %>%
  mutate(rank = row_number()) %>%
  filter(rank <= 50) %>%
  ungroup() %>%
  select(bsns_year, rank, corp_code, y, score, everything())

top50_by_year

average_precision_at_k <- function(y_sorted, k) {
  k_eff <- min(k, length(y_sorted))
  y_top <- y_sorted[seq_len(k_eff)]
  
  total_pos <- sum(y_sorted == 1)
  
  if (total_pos == 0) return(NA_real_)
  
  rel_idx <- which(y_top == 1)
  
  if (length(rel_idx) == 0) return(0)
  
  precision_at_rel <- cumsum(y_top == 1)[rel_idx] / rel_idx
  
  sum(precision_at_rel) / min(total_pos, k_eff)
}

ndcg_at_k <- function(y_sorted, k) {
  k_eff <- min(k, length(y_sorted))
  
  y_top <- y_sorted[seq_len(k_eff)]
  
  dcg <- sum(y_top / log2(seq_len(k_eff) + 1))
  
  ideal <- sort(y_sorted, decreasing = TRUE)[seq_len(k_eff)]
  idcg <- sum(ideal / log2(seq_len(k_eff) + 1))
  
  if (idcg == 0) return(NA_real_)
  
  dcg / idcg
}

rank_metrics_at_k <- function(scored_df, k_values = c(10, 20, 50, 100)) {
  map_dfr(k_values, function(k) {
    scored_df %>%
      group_by(bsns_year) %>%
      group_modify(~{
        d <- .x %>%
          arrange(desc(score))
        
        y <- d$y
        
        k_eff <- min(k, length(y))
        total_pos <- sum(y == 1)
        
        tp <- sum(y[seq_len(k_eff)] == 1)
        
        precision <- tp / k_eff
        
        recall <- if (total_pos > 0) {
          tp / total_pos
        } else {
          NA_real_
        }
        
        base_rate <- total_pos / length(y)
        
        lift <- if (base_rate > 0) {
          precision / base_rate
        } else {
          NA_real_
        }
        
        tibble(
          n = length(y),
          positives = total_pos,
          k_eff = k_eff,
          tp_at_k = tp,
          precision_at_k = precision,
          recall_at_k = recall,
          base_rate = base_rate,
          lift_at_k = lift,
          ap_at_k = average_precision_at_k(y, k_eff),
          ndcg_at_k = ndcg_at_k(y, k_eff)
        )
      }) %>%
      ungroup() %>%
      mutate(k = k, .before = 1)
  })
}

rank_eval <- rank_metrics_at_k(
  test_scored,
  k_values = c(10, 20, 50, 100)
)

rank_eval

rank_eval_summary <- rank_eval %>%
  group_by(k) %>%
  summarise(
    years = n(),
    total_n = sum(n),
    total_positives = sum(positives),
    total_tp_at_k = sum(tp_at_k),
    mean_precision_at_k = mean(precision_at_k, na.rm = TRUE),
    mean_recall_at_k = mean(recall_at_k, na.rm = TRUE),
    mean_lift_at_k = mean(lift_at_k, na.rm = TRUE),
    mean_ap_at_k = mean(ap_at_k, na.rm = TRUE),
    mean_ndcg_at_k = mean(ndcg_at_k, na.rm = TRUE),
    .groups = "drop"
  )

rank_eval_summary

binary_prob <- predict(model, dtest)

binary_scored <- test_rank %>%
  mutate(score = binary_prob) %>%
  arrange(bsns_year, desc(score))

binary_eval <- rank_metrics_at_k(
  binary_scored,
  k_values = c(10, 20, 50, 100)
)

binary_eval_summary <- binary_eval %>%
  group_by(k) %>%
  summarise(
    total_n = sum(n),
    total_positives = sum(positives),
    total_tp_at_k = sum(tp_at_k),
    mean_precision_at_k = mean(precision_at_k, na.rm = TRUE),
    mean_recall_at_k = mean(recall_at_k, na.rm = TRUE),
    mean_lift_at_k = mean(lift_at_k, na.rm = TRUE),
    mean_ap_at_k = mean(ap_at_k, na.rm = TRUE),
    mean_ndcg_at_k = mean(ndcg_at_k, na.rm = TRUE),
    .groups = "drop"
  )

comparison <- bind_rows(
  binary_eval_summary %>% mutate(model_type = "binary_logistic"),
  rank_eval_summary %>% mutate(model_type = "rank_ndcg")
) %>%
  arrange(k, model_type)

comparison
