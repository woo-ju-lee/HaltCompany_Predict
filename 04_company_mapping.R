library(tidyverse)
library(DBI)
library(stringi)


# ============================================================
# 0. 경로
# ============================================================

PROJECT_DIR <- path.expand(
  "~/HaltCompany_Predict"
)

PARSED_SUMMARY_PATH <- file.path(
  PROJECT_DIR,
  "dart_parsed_summary.csv"
)

MAPPING_PATH <- file.path(
  PROJECT_DIR,
  "04_receipt_company_mapping.csv"
)

PARSED_WITH_ID_PATH <- file.path(
  PROJECT_DIR,
  "04_parsed_summary_with_company_id.csv"
)

NAME_CANDIDATE_PATH <- file.path(
  PROJECT_DIR,
  "04_name_match_candidates.csv"
)

CODE_CANDIDATE_PATH <- file.path(
  PROJECT_DIR,
  "04_stock_code_match_candidates.csv"
)

MANUAL_REVIEW_PATH <- file.path(
  PROJECT_DIR,
  "04_company_mapping_manual_review.csv"
)

ALIAS_COLLISION_PATH <- file.path(
  PROJECT_DIR,
  "04_stock_info_alias_collisions.csv"
)


# ============================================================
# 1. 코드 정리 함수
# ============================================================

clean_id_code <- function(
    x,
    width
) {
  
  x <- as.character(x)
  
  value <- stringr::str_extract(
    x,
    "\\d+"
  )
  
  result <- stringr::str_pad(
    value,
    width = width,
    side = "left",
    pad = "0"
  )
  
  result[
    is.na(value) |
      !nzchar(value)
  ] <- NA_character_
  
  result
}


# ============================================================
# 2. 기업명 정규화 함수
#
# 다음 차이만 제거
# - (주), ㈜, 주식회사
# - 공백 및 구두점
# - 종목명 뒤의 "주권", "보통주" 등
#
# 유사도 기반 fuzzy matching은 하지 않음
# ============================================================

normalize_company_name <- function(
    x
) {
  
  x <- as.character(x)
  
  x[
    is.na(x)
  ] <- ""
  
  # 전각문자, 특수 법인기호 등을 표준화
  x <- stringi::stri_trans_nfkc(
    x
  )
  
  x <- stringr::str_squish(
    x
  )
  
  # 종목명 끝의 주권 관련 표현 제거
  x <- stringr::str_replace(
    x,
    paste0(
      "(?:\\s*",
      "(?:보통주|우선주|주권|종목)",
      ")+\\s*$"
    ),
    ""
  )
  
  # 법인 표기 제거
  x <- stringr::str_replace_all(
    x,
    paste0(
      "\\(\\s*주\\s*\\)|",
      "주식회사|",
      "유한회사"
    ),
    ""
  )
  
  # 공백 및 문장부호 제거
  x <- stringr::str_replace_all(
    x,
    "[[:space:][:punct:]ㆍ·・]",
    ""
  )
  
  stringr::str_to_upper(
    x
  )
}


# ============================================================
# 3. 파싱 결과 불러오기
# ============================================================

parsed_summary <- readr::read_csv(
  PARSED_SUMMARY_PATH,
  show_col_types = FALSE,
  col_types = cols(
    rcept_no = col_character(),
    .default = col_guess()
  )
) |>
  mutate(
    rcept_no = as.character(
      rcept_no
    ),
    
    # 접수번호 앞 8자리가 접수일자
    rcept_date = as.Date(
      stringr::str_sub(
        rcept_no,
        1L,
        8L
      ),
      format = "%Y%m%d"
    )
  )


# 메타데이터 열이 없을 때를 대비
required_metadata_columns <- c(
  "market_badge",
  "company_name_index",
  "report_name",
  "event_type",
  "parse_status"
)

for (
  column_name in setdiff(
    required_metadata_columns,
    names(parsed_summary)
  )
) {
  
  parsed_summary[[
    column_name
  ]] <- NA_character_
}


# ============================================================
# 4. STOCK_INFO 불러오기
# ============================================================

stock_info <- DBI::dbGetQuery(
  con,
  "
  SELECT
    corp_code,
    corp_name,
    corp_name_eng,
    stock_name,
    stock_code,
    ceo_nm,
    corp_cls,
    jurir_no,
    bizr_no,
    induty_code,
    est_dt,
    acc_mt
  FROM STOCK_INFO
  "
) |>
  tibble::as_tibble() |>
  mutate(
    corp_code = clean_id_code(
      corp_code,
      width = 8L
    ),
    
    stock_code = clean_id_code(
      stock_code,
      width = 6L
    ),
    
    corp_name = as.character(
      corp_name
    ),
    
    stock_name = as.character(
      stock_name
    ),
    
    corp_name_norm = normalize_company_name(
      corp_name
    ),
    
    stock_name_norm = normalize_company_name(
      stock_name
    )
  )


# corp_code가 없는 행은 식별 기준표로 사용할 수 없음
stock_info_valid <- stock_info |>
  filter(
    !is.na(corp_code)
  )


# ============================================================
# 5. STOCK_INFO 자체 중복 점검
# ============================================================

corp_code_duplicates <- stock_info_valid |>
  count(
    corp_code,
    name = "n_rows"
  ) |>
  filter(
    n_rows > 1L
  )

stock_code_duplicates <- stock_info_valid |>
  filter(
    !is.na(stock_code)
  ) |>
  distinct(
    corp_code,
    stock_code
  ) |>
  count(
    stock_code,
    name = "n_corp_codes"
  ) |>
  filter(
    n_corp_codes > 1L
  )

cat(
  "중복 corp_code 수:",
  nrow(corp_code_duplicates),
  "\n"
)

cat(
  "복수 corp_code에 연결된 stock_code 수:",
  nrow(stock_code_duplicates),
  "\n"
)


# corp_code당 대표 행
# 아래 alias 테이블은 중복 제거 전에 만들기 때문에
# 동일 corp_code의 복수 이름이 있으면 별칭으로 유지됨
stock_master <- stock_info_valid |>
  arrange(
    corp_code
  ) |>
  distinct(
    corp_code,
    .keep_all = TRUE
  )


# ============================================================
# 6. STOCK_INFO 기업명 별칭표 생성
#
# corp_name과 stock_name을 모두 사용
# ============================================================

stock_alias <- bind_rows(
  stock_info_valid |>
    transmute(
      corp_code,
      stock_code,
      alias_source = "stock_name",
      alias_raw = stock_name,
      alias_norm = stock_name_norm
    ),
  
  stock_info_valid |>
    transmute(
      corp_code,
      stock_code,
      alias_source = "corp_name",
      alias_raw = corp_name,
      alias_norm = corp_name_norm
    )
) |>
  filter(
    !is.na(corp_code),
    !is.na(alias_norm),
    nzchar(alias_norm)
  ) |>
  distinct(
    corp_code,
    stock_code,
    alias_source,
    alias_raw,
    alias_norm
  )


# 같은 정규화 이름이 여러 corp_code에 연결되는 경우
# 자동 매칭하면 안 됨
alias_collisions <- stock_alias |>
  distinct(
    alias_norm,
    corp_code
  ) |>
  group_by(
    alias_norm
  ) |>
  summarise(
    n_corp_codes = n_distinct(
      corp_code
    ),
    
    candidate_corp_codes = paste(
      sort(
        unique(corp_code)
      ),
      collapse = "|"
    ),
    
    .groups = "drop"
  ) |>
  filter(
    n_corp_codes > 1L
  ) |>
  arrange(
    desc(n_corp_codes),
    alias_norm
  )

readr::write_csv(
  alias_collisions,
  ALIAS_COLLISION_PATH,
  na = ""
)


# ============================================================
# 7. 공시에서 기업명 후보 만들기
#
# 기존 파싱 결과에 존재하는 열만 사용
# 파서를 다시 실행하거나 수정하지 않음
# ============================================================

disclosure_name_columns <- intersect(
  c(
    "company_name_index",
    "stock_name",
    "company_name"
  ),
  names(parsed_summary)
)

if (length(disclosure_name_columns) == 0L) {
  stop(
    paste(
      "parsed_summary에서 기업명 열을 찾지 못했습니다.",
      "company_name_index, stock_name, company_name 중",
      "하나 이상이 필요합니다."
    )
  )
}


disclosure_names <- parsed_summary |>
  select(
    rcept_no,
    all_of(
      disclosure_name_columns
    )
  ) |>
  pivot_longer(
    cols = -rcept_no,
    names_to = "name_source",
    values_to = "name_raw"
  ) |>
  mutate(
    name_raw = replace_na(
      as.character(name_raw),
      ""
    ),
    
    # 하나의 추출 필드에 복수 값이 들어 있는 경우 분리
    name_raw = stringr::str_split(
      name_raw,
      "\\s*\\|\\|\\s*"
    )
  ) |>
  tidyr::unnest(
    name_raw
  ) |>
  mutate(
    name_raw = stringr::str_squish(
      name_raw
    ),
    
    name_norm = normalize_company_name(
      name_raw
    )
  ) |>
  filter(
    nzchar(name_norm)
  ) |>
  distinct(
    rcept_no,
    name_source,
    name_raw,
    name_norm
  )


# ============================================================
# 8. 기업명 정확 매칭 후보
# ============================================================

name_candidate_evidence <- suppressWarnings(
  disclosure_names |>
    inner_join(
      stock_alias,
      by = c(
        "name_norm" = "alias_norm"
      )
    )
) |>
  distinct(
    rcept_no,
    name_source,
    name_raw,
    name_norm,
    corp_code,
    stock_code,
    alias_source,
    alias_raw
  )

readr::write_csv(
  name_candidate_evidence,
  NAME_CANDIDATE_PATH,
  na = ""
)


# ============================================================
# 9. 공시에서 추출된 종목코드가 있으면 별도 매칭
#
# 현재 추출 결과에서 stock_code가 모두 NA여도
# 코드는 그대로 작동함
# ============================================================

if ("stock_code" %in% names(parsed_summary)) {
  
  parsed_stock_codes <- parsed_summary |>
    transmute(
      rcept_no,
      
      parsed_stock_code_raw = replace_na(
        as.character(stock_code),
        ""
      )
    ) |>
    mutate(
      parsed_stock_code_raw = stringr::str_split(
        parsed_stock_code_raw,
        "\\s*\\|\\|\\s*"
      )
    ) |>
    tidyr::unnest(
      parsed_stock_code_raw
    ) |>
    mutate(
      parsed_stock_code = clean_id_code(
        parsed_stock_code_raw,
        width = 6L
      )
    ) |>
    filter(
      !is.na(parsed_stock_code)
    ) |>
    distinct(
      rcept_no,
      parsed_stock_code
    )
  
} else {
  
  parsed_stock_codes <- tibble(
    rcept_no = character(),
    parsed_stock_code = character()
  )
}


stock_code_dimension <- stock_info_valid |>
  filter(
    !is.na(stock_code)
  ) |>
  distinct(
    corp_code,
    stock_code
  )


code_candidate_evidence <- parsed_stock_codes |>
  inner_join(
    stock_code_dimension,
    by = c(
      "parsed_stock_code" = "stock_code"
    )
  ) |>
  distinct(
    rcept_no,
    parsed_stock_code,
    corp_code
  )

readr::write_csv(
  code_candidate_evidence,
  CODE_CANDIDATE_PATH,
  na = ""
)


# ============================================================
# 10. 후보 집계 함수
# ============================================================

single_unique_value <- function(
    x
) {
  
  values <- sort(
    unique(
      x[
        !is.na(x) &
          nzchar(x)
      ]
    )
  )
  
  if (length(values) == 1L) {
    return(
      values[[1]]
    )
  }
  
  NA_character_
}


collapse_unique_values <- function(
    x
) {
  
  values <- sort(
    unique(
      x[
        !is.na(x) &
          nzchar(x)
      ]
    )
  )
  
  if (length(values) == 0L) {
    return(
      NA_character_
    )
  }
  
  paste(
    values,
    collapse = "|"
  )
}


# ============================================================
# 11. 기업명 매칭 후보를 접수번호별로 집계
# ============================================================

name_match_rollup <- name_candidate_evidence |>
  group_by(
    rcept_no
  ) |>
  summarise(
    n_name_corp = n_distinct(
      corp_code,
      na.rm = TRUE
    ),
    
    name_corp_code = single_unique_value(
      corp_code
    ),
    
    name_candidate_corp_codes =
      collapse_unique_values(
        corp_code
      ),
    
    name_match_evidence =
      collapse_unique_values(
        paste0(
          name_source,
          ":",
          name_raw,
          " => ",
          alias_source,
          ":",
          alias_raw
        )
      ),
    
    .groups = "drop"
  )


# ============================================================
# 12. 종목코드 매칭 후보를 접수번호별로 집계
# ============================================================

code_match_rollup <- code_candidate_evidence |>
  group_by(
    rcept_no
  ) |>
  summarise(
    n_code_corp = n_distinct(
      corp_code,
      na.rm = TRUE
    ),
    
    code_corp_code = single_unique_value(
      corp_code
    ),
    
    code_candidate_corp_codes =
      collapse_unique_values(
        corp_code
      ),
    
    .groups = "drop"
  )


# ============================================================
# 13. 접수번호별 기업 매핑
#
# 종목코드가 유일하면 종목코드 우선
# 종목코드가 없으면 기업명 정확 매칭
# 다중매칭은 자동 결정하지 않음
# ============================================================

receipt_mapping <- parsed_summary |>
  select(
    rcept_no,
    rcept_date,
    market_badge,
    company_name_index,
    report_name,
    event_type,
    parse_status
  ) |>
  left_join(
    code_match_rollup,
    by = "rcept_no"
  ) |>
  left_join(
    name_match_rollup,
    by = "rcept_no"
  ) |>
  mutate(
    n_code_corp = coalesce(
      n_code_corp,
      0L
    ),
    
    n_name_corp = coalesce(
      n_name_corp,
      0L
    ),
    
    code_name_conflict = (
      n_code_corp == 1L &
        n_name_corp == 1L &
        !is.na(code_corp_code) &
        !is.na(name_corp_code) &
        code_corp_code != name_corp_code
    ),
    
    mapped_corp_code = case_when(
      code_name_conflict ~
        NA_character_,
      
      n_code_corp == 1L ~
        code_corp_code,
      
      n_code_corp == 0L &
        n_name_corp == 1L ~
        name_corp_code,
      
      TRUE ~
        NA_character_
    ),
    
    match_status = case_when(
      code_name_conflict ~
        "CONFLICT_CODE_VS_NAME",
      
      n_code_corp == 1L ~
        "MATCHED_STOCK_CODE",
      
      n_code_corp > 1L ~
        "AMBIGUOUS_STOCK_CODE",
      
      n_name_corp == 1L ~
        "MATCHED_EXACT_NAME",
      
      n_name_corp > 1L ~
        "AMBIGUOUS_EXACT_NAME",
      
      TRUE ~
        "UNMATCHED"
    )
  )


# ============================================================
# 14. 수동검토 파일 생성
#
# 이미 파일이 존재하면 덮어쓰지 않음
# override_corp_code만 직접 입력
# ============================================================

manual_review_template <- receipt_mapping |>
  filter(
    match_status %in% c(
      "UNMATCHED",
      "AMBIGUOUS_EXACT_NAME",
      "AMBIGUOUS_STOCK_CODE",
      "CONFLICT_CODE_VS_NAME"
    )
  ) |>
  transmute(
    rcept_no,
    rcept_date = as.character(
      rcept_date
    ),
    market_badge,
    company_name_index,
    report_name,
    event_type,
    match_status,
    name_candidate_corp_codes,
    code_candidate_corp_codes,
    
    override_corp_code = NA_character_,
    review_note = NA_character_
  )


if (!file.exists(MANUAL_REVIEW_PATH)) {
  
  readr::write_csv(
    manual_review_template,
    MANUAL_REVIEW_PATH,
    na = ""
  )
  
  message(
    "수동검토 파일 생성: ",
    MANUAL_REVIEW_PATH
  )
}


# ============================================================
# 15. 수동 매핑 적용
#
# 파일의 override_corp_code를 입력한 후
# 이 코드부터 다시 실행하면 됨
# ============================================================

manual_override <- readr::read_csv(
  MANUAL_REVIEW_PATH,
  show_col_types = FALSE,
  col_types = cols(
    .default = col_character()
  )
) |>
  mutate(
    override_corp_code = clean_id_code(
      override_corp_code,
      width = 8L
    )
  ) |>
  filter(
    !is.na(override_corp_code)
  ) |>
  distinct(
    rcept_no,
    .keep_all = TRUE
  )


# STOCK_INFO에 없는 corp_code가 입력됐는지 검증
invalid_manual_override <- manual_override |>
  anti_join(
    stock_master,
    by = c(
      "override_corp_code" = "corp_code"
    )
  )

if (nrow(invalid_manual_override) > 0L) {
  
  print(
    invalid_manual_override,
    n = Inf,
    width = Inf
  )
  
  stop(
    "수동 입력한 corp_code 중 STOCK_INFO에 없는 값이 있습니다."
  )
}


receipt_mapping_final <- receipt_mapping |>
  left_join(
    manual_override |>
      select(
        rcept_no,
        override_corp_code,
        review_note
      ),
    by = "rcept_no"
  ) |>
  mutate(
    final_corp_code = coalesce(
      override_corp_code,
      mapped_corp_code
    ),
    
    final_match_status = case_when(
      !is.na(override_corp_code) ~
        "MANUAL_OVERRIDE",
      
      TRUE ~
        match_status
    )
  )


# ============================================================
# 16. STOCK_INFO 대표정보 결합
# ============================================================

stock_dimension <- stock_master |>
  transmute(
    final_corp_code = corp_code,
    
    master_corp_name = corp_name,
    master_stock_name = stock_name,
    master_stock_code = stock_code,
    master_corp_cls = corp_cls,
    master_corp_name_eng = corp_name_eng,
    master_jurir_no = jurir_no,
    master_bizr_no = bizr_no,
    master_induty_code = induty_code,
    master_acc_mt = acc_mt
  )


receipt_mapping_final <- receipt_mapping_final |>
  left_join(
    stock_dimension,
    by = "final_corp_code"
  )


# ============================================================
# 17. 원래 parsed_summary에 corp_code 결합
# ============================================================

parsed_summary_with_company_id <- parsed_summary |>
  left_join(
    receipt_mapping_final |>
      select(
        rcept_no,
        final_corp_code,
        final_match_status,
        master_corp_name,
        master_stock_name,
        master_stock_code,
        master_corp_cls,
        master_induty_code,
        master_acc_mt
      ),
    by = "rcept_no"
  )


# ============================================================
# 18. 결과 저장
# ============================================================

readr::write_csv(
  receipt_mapping_final,
  MAPPING_PATH,
  na = ""
)

readr::write_csv(
  parsed_summary_with_company_id,
  PARSED_WITH_ID_PATH,
  na = ""
)


# ============================================================
# 19. 매칭 결과 확인
# ============================================================

cat(
  "\n===== 매칭 상태 =====\n"
)

print(
  receipt_mapping_final |>
    count(
      final_match_status,
      sort = TRUE
    ),
  n = Inf
)


cat(
  "\n===== 전체 매칭률 =====\n"
)

print(
  receipt_mapping_final |>
    summarise(
      total_receipts = n(),
      
      matched_receipts = sum(
        !is.na(final_corp_code)
      ),
      
      unmatched_receipts = sum(
        is.na(final_corp_code)
      ),
      
      match_rate = mean(
        !is.na(final_corp_code)
      )
    )
)


cat(
  "\n===== 미매칭 기업명 상위 =====\n"
)

print(
  receipt_mapping_final |>
    filter(
      is.na(final_corp_code)
    ) |>
    count(
      company_name_index,
      sort = TRUE
    ) |>
    slice_head(
      n = 50L
    ),
  n = 50,
  width = Inf
)

# ============================================================
# 20. 재무적·규제절차 후보 기업 선정
# ============================================================

candidate_corp_codes <- parsed_summary_with_company_id |>
  filter(
    !is.na(final_corp_code),
    
    keyword_financial_reason %in% TRUE |
      keyword_regulatory_process %in% TRUE
  ) |>
  distinct(
    final_corp_code
  )


# ============================================================
# 21. 후보 기업의 관련 공시 전체 타임라인
# ============================================================

core_event_types <- c(
  "HALT_START",
  "HALT_PERIOD_CHANGE",
  "HALT_RELEASE",
  "HALT_AND_RELEASE",
  "DELISTING_RELATED",
  "INTERNAL_SETTLEMENT_WARNING",
  "MARKET_NOTICE"
)


candidate_timeline_by_corp <- parsed_summary_with_company_id |>
  filter(
    !is.na(final_corp_code),
    event_type %in% core_event_types
  ) |>
  semi_join(
    candidate_corp_codes,
    by = "final_corp_code"
  ) |>
  arrange(
    final_corp_code,
    rcept_date,
    rcept_no
  ) |>
  group_by(
    final_corp_code
  ) |>
  mutate(
    company_disclosure_order = row_number(),
    
    previous_disclosure_date = lag(
      rcept_date
    ),
    
    days_from_previous_disclosure = as.integer(
      rcept_date -
        previous_disclosure_date
    )
  ) |>
  ungroup()


readr::write_csv(
  candidate_timeline_by_corp,
  file.path(
    PROJECT_DIR,
    "04_candidate_timeline_by_corp_code.csv"
  ),
  na = ""
)


cat(
  "후보 기업 수:",
  n_distinct(
    candidate_timeline_by_corp$final_corp_code
  ),
  "\n"
)

cat(
  "후보 기업 관련공시 수:",
  nrow(
    candidate_timeline_by_corp
  ),
  "\n"
)
