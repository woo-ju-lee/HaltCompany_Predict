valid_prob <- predict(model, dvalid)

threshold_tbl <- tibble(
  threshold = seq(0.01, 0.99, by = 0.01)
) %>%
  mutate(
    pred = map(threshold, ~ ifelse(valid_prob >= .x, 1, 0)),
    tp = map_dbl(pred, ~ sum(.x == 1 & valid_halt == 1)),
    fp = map_dbl(pred, ~ sum(.x == 1 & valid_halt == 0)),
    fn = map_dbl(pred, ~ sum(.x == 0 & valid_halt == 1)),
    tn = map_dbl(pred, ~ sum(.x == 0 & valid_halt == 0)),
    precision = ifelse(tp + fp == 0, NA_real_, tp / (tp + fp)),
    recall = ifelse(tp + fn == 0, NA_real_, tp / (tp + fn)),
    f1 = ifelse(
      is.na(precision) | precision + recall == 0,
      NA_real_,
      2 * precision * recall / (precision + recall)
    ),
    f2 = ifelse(
      is.na(precision) | precision + recall == 0,
      NA_real_,
      5 * precision * recall / (4 * precision + recall)
    )
  )

# 예: recall 0.8 이상 유지하면서 precision 최대
best_threshold <- threshold_tbl %>%
  filter(recall >= 0.8) %>%
  arrange(desc(precision), desc(f1)) %>%
  slice(1) %>%
  pull(threshold)

best_threshold
