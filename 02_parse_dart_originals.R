# ============================================================
# 02_parse_dart_originals.R
# 압축해제된 OpenDART 원문(XML 확장자이지만 HTML형 문서)을 일괄 파싱
# - 접수번호별 RDS 저장으로 중단 후 재개 가능
# - 모든 표 행 보존
# - 거래정지/기간변경/해제/상장폐지 공통 필드 자동 추출
# ============================================================

library(tidyverse)
library(xml2)
library(rvest)

# ------------------------------------------------------------
# 0. 사용자 설정
# ------------------------------------------------------------

PROJECT_DIR <- path.expand("~/HaltCompany_Predict")
ORIGINAL_ROOT <- file.path(PROJECT_DIR, "dart_original")

CLASSIFIED_INDEX_PATH <- file.path(
  PROJECT_DIR,
  "dart_trading_halt_index_classified.csv"
)

# 01 스크립트를 거치지 않았다면 원래 인덱스를 사용
FALLBACK_INDEX_PATH <- file.path(
  PROJECT_DIR,
  "dart_trading_halt_search.csv"
)

PARSED_ROOT <- file.path(
  PROJECT_DIR,
  "dart_parsed_receipts"
)

PARSE_LOG_RDS <- file.path(
  PROJECT_DIR,
  "dart_parse_log.rds"
)

PARSE_LOG_CSV <- file.path(
  PROJECT_DIR,
  "dart_parse_log.csv"
)

PARSED_SUMMARY_PATH <- file.path(
  PROJECT_DIR,
  "dart_parsed_summary.csv"
)

PARSED_FIELDS_PATH <- file.path(
  PROJECT_DIR,
  "dart_parsed_fields.csv"
)

PARSED_SOURCE_META_PATH <- file.path(
  PROJECT_DIR,
  "dart_parsed_source_meta.csv"
)

OVERWRITE_PARSE <- FALSE
PROGRESS_EVERY <- 50L

if (!dir.exists(ORIGINAL_ROOT)) {
  stop("원문 폴더를 찾을 수 없습니다: ", ORIGINAL_ROOT, call. = FALSE)
}

dir.create(PARSED_ROOT, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------
# 1. 인덱스 읽기/분류 보조 함수
# ------------------------------------------------------------

normalize_report_name <- function(x) {
  x |>
    tidyr::replace_na("") |>
    stringr::str_squish() |>
    stringr::str_remove("^(?:\\[[^\\]]+\\]\\s*)+")
}

classify_report_type <- function(report_name) {
  report_key <- report_name |>
    normalize_report_name() |>
    stringr::str_replace_all("\\s+", "")

  dplyr::case_when(
    stringr::str_detect(
      report_key,
      "^주권매매거래정지기간변경"
    ) ~ "HALT_PERIOD_CHANGE",

    stringr::str_detect(
      report_key,
      "^주권매매거래정지해제"
    ) ~ "HALT_RELEASE",

    stringr::str_detect(
      report_key,
      "^주권매매거래정지"
    ) ~ "HALT_START",

    stringr::str_detect(
      report_key,
      "^매매거래정지및정지해제"
    ) ~ "HALT_AND_RELEASE",

    stringr::str_detect(
      report_key,
      "내부결산시점.*(?:상장폐지|상장적격성실질심사)"
    ) ~ "INTERNAL_SETTLEMENT_WARNING",

    stringr::str_detect(
      report_key,
      "^기타시장안내"
    ) ~ "MARKET_NOTICE",

    stringr::str_detect(
      report_key,
      "상장폐지"
    ) ~ "DELISTING_RELATED",

    TRUE ~ "OTHER_SEARCH_HIT"
  )
}

index_path <- if (file.exists(CLASSIFIED_INDEX_PATH)) {
  CLASSIFIED_INDEX_PATH
} else {
  FALLBACK_INDEX_PATH
}

if (!file.exists(index_path)) {
  stop("검색 인덱스를 찾을 수 없습니다.", call. = FALSE)
}

halt_index <- readr::read_csv(
  index_path,
  col_types = readr::cols(
    rcept_no = readr::col_character(),
    report_name = readr::col_character(),
    detail_url = readr::col_character(),
    row_text = readr::col_character(),
    .default = readr::col_guess()
  ),
  show_col_types = FALSE
) |>
  dplyr::distinct(rcept_no, .keep_all = TRUE)

if (!"report_name_clean" %in% names(halt_index)) {
  halt_index <- halt_index |>
    dplyr::mutate(
      report_name_clean = normalize_report_name(report_name)
    )
}

if (!"event_type" %in% names(halt_index)) {
  halt_index <- halt_index |>
    dplyr::mutate(
      event_type = classify_report_type(report_name)
    )
}

if (!"company_name_index" %in% names(halt_index)) {
  halt_index <- halt_index |>
    dplyr::mutate(
      row_header = stringr::str_split_fixed(
        tidyr::replace_na(row_text, ""),
        "\\s*\\|\\s*",
        n = 2L
      )[, 1],
      company_market_raw = purrr::map2_chr(
        row_header,
        report_name,
        function(header, current_report_name) {
          if (is.na(current_report_name) || !nzchar(current_report_name)) {
            return(stringr::str_squish(header))
          }

          stringr::str_squish(
            stringr::str_remove(
              header,
              stringr::fixed(current_report_name)
            )
          )
        }
      ),
      market_badge = stringr::str_extract(
        company_market_raw,
        "^[유코기넥채]"
      ),
      company_name_index = company_market_raw |>
        stringr::str_remove("^[유코기넥채]") |>
        stringr::str_squish()
    )
}

halt_index <- halt_index |>
  dplyr::mutate(
    rcept_date = as.Date(
      stringr::str_sub(rcept_no, 1L, 8L),
      format = "%Y%m%d"
    ),
    title_reason = stringr::str_match(
      report_name_clean,
      "\\((.*)\\)\\s*$"
    )[, 2]
  )

# ------------------------------------------------------------
# 2. 공통 텍스트/파일 함수
# ------------------------------------------------------------

clean_text <- function(x) {
  x <- as.character(x)
  x <- stringr::str_replace_all(x, "\u00A0", " ")
  x <- stringr::str_squish(x)
  x[!is.na(x) & nzchar(x)]
}

normalize_for_match <- function(x) {
  x |>
    tidyr::replace_na("") |>
    stringr::str_squish() |>
    stringr::str_replace_all("\\s+", "") |>
    stringr::str_remove("^[0-9]+(?:-[0-9]+)?[.．)]*") |>
    stringr::str_remove("^※")
}

receipt_source_files <- function(receipt_dir) {
  if (!dir.exists(receipt_dir)) {
    return(character())
  }

  list.files(
    receipt_dir,
    pattern = "\\.(xml|html?|xhtml)$",
    recursive = TRUE,
    full.names = TRUE,
    ignore.case = TRUE
  )
}

read_dart_document <- function(file_path) {
  xml2::read_html(
    file_path,
    encoding = "UTF-8",
    options = c(
      "RECOVER",
      "NOERROR",
      "NOWARNING",
      "NOBLANKS",
      "HUGE"
    )
  )
}

empty_rows <- function() {
  tibble::tibble(
    source_file = character(),
    row_no = integer(),
    n_cells = integer(),
    row_text = character(),
    cells = list()
  )
}

extract_rows_from_document <- function(doc, source_file) {
  row_nodes <- rvest::html_elements(doc, "tr")

  if (length(row_nodes) == 0L) {
    return(empty_rows())
  }

  purrr::map_dfr(
    seq_along(row_nodes),
    function(row_no) {
      # 직접 자식 셀만 사용하여 중첩 표의 과도한 중복을 줄임
      cell_nodes <- xml2::xml_find_all(
        row_nodes[[row_no]],
        "./th | ./td"
      )

      if (length(cell_nodes) == 0L) {
        return(NULL)
      }

      cells <- rvest::html_text2(cell_nodes) |>
        clean_text()

      # colspan/rowspan 변환 과정에서 같은 값이 반복되는 경우 제거
      cells <- cells[!duplicated(cells)]

      if (length(cells) == 0L) {
        return(NULL)
      }

      tibble::tibble(
        source_file = source_file,
        row_no = as.integer(row_no),
        n_cells = length(cells),
        row_text = paste(cells, collapse = " | "),
        cells = list(cells)
      )
    }
  ) |>
    dplyr::distinct(source_file, row_text, .keep_all = TRUE)
}

score_document_text <- function(
    full_text,
    file_path,
    rcept_no,
    n_rows
) {
  match_text <- normalize_for_match(full_text)

  score_patterns <- c(
    "매매거래정지",
    "주권매매거래정지",
    "상장폐지",
    "정리매매",
    "상장적격성실질심사",
    "개선기간",
    "감사의견",
    "자본잠식",
    "회생절차",
    "부도"
  )

  keyword_score <- sum(
    vapply(
      score_patterns,
      function(pattern) {
        stringr::str_detect(
          match_text,
          stringr::fixed(pattern)
        )
      },
      logical(1)
    )
  )

  exact_name_bonus <- as.integer(
    identical(
      tools::file_path_sans_ext(basename(file_path)),
      rcept_no
    )
  ) * 100L

  exact_name_bonus + keyword_score * 10L + min(n_rows, 50L)
}

parse_source_file <- function(file_path, receipt_dir, rcept_no) {
  source_file <- substring(
    file_path,
    nchar(receipt_dir) + 2L
  )

  parsed <- tryCatch(
    {
      doc <- read_dart_document(file_path)
      rows <- extract_rows_from_document(doc, source_file)
      full_text <- rvest::html_text2(doc) |>
        stringr::str_squish()

      list(
        meta = tibble::tibble(
          source_file = source_file,
          read_status = "ok",
          read_message = NA_character_,
          n_rows = nrow(rows),
          text_length = nchar(full_text),
          source_score = score_document_text(
            full_text = full_text,
            file_path = file_path,
            rcept_no = rcept_no,
            n_rows = nrow(rows)
          )
        ),
        rows = rows,
        full_text = full_text
      )
    },
    error = function(e) {
      list(
        meta = tibble::tibble(
          source_file = source_file,
          read_status = "read_error",
          read_message = conditionMessage(e),
          n_rows = 0L,
          text_length = 0L,
          source_score = -1L
        ),
        rows = empty_rows(),
        full_text = ""
      )
    }
  )

  parsed
}

# ------------------------------------------------------------
# 3. 공통 필드 사전
#    패턴은 normalize_for_match()가 적용된 셀을 기준으로 작성
# ------------------------------------------------------------

FIELD_DICTIONARY <- tibble::tribble(
  ~field_name, ~label_pattern,
  "company_name", "(?:회사명|법인명)",
  "stock_name", "(?:종목명|대상종목|대상주권|대상증권)",
  "stock_code", "(?:종목코드|단축코드)",
  "halt_type", "매매거래정지유형",
  "halt_start", "(?:매매거래정지일시|정지일시)",
  "halt_end", "(?:매매거래정지해제일시|정지해제일시|해제일시)",
  "halt_reason", "(?:매매거래정지및정지해제사유|매매거래정지사유|정지사유)",
  "halt_period", "(?:매매거래정지기간|정지기간)",
  "period_before", "(?:변경전.*(?:정지기간|기간)|^변경전$)",
  "period_after", "(?:변경후.*(?:정지기간|기간)|^변경후$)",
  "change_reason", "변경사유",
  "release_reason", "(?:매매거래정지해제사유|정지해제사유|해제사유)",
  "delisting_reason", "상장폐지사유",
  "delisting_date", "상장폐지일",
  "cleanup_period", "정리매매기간",
  "cleanup_start", "(?:정리매매개시일|정리매매시작일)",
  "cleanup_end", "정리매매종료일",
  "improvement_period", "개선기간",
  "review_result", "(?:심의결과|심사결과|결정내용)",
  "audit_opinion", "감사의견",
  "capital_impairment", "(?:자본잠식률|자본전액잠식|자본잠식)",
  "legal_basis", "(?:근거규정|근거)",
  "related_disclosure", "관련공시"
)

extract_fields_from_rows <- function(rows) {
  if (nrow(rows) == 0L) {
    return(
      tibble::tibble(
        field_name = character(),
        field_value = character(),
        source_file = character(),
        match_row_no = integer(),
        match_row_text = character()
      )
    )
  }

  all_label_pattern <- paste0(
    "(?:",
    paste(FIELD_DICTIONARY$label_pattern, collapse = ")|(?:"),
    ")"
  )

  result <- vector("list", nrow(FIELD_DICTIONARY))

  for (field_index in seq_len(nrow(FIELD_DICTIONARY))) {
    field_name <- FIELD_DICTIONARY$field_name[[field_index]]
    label_pattern <- FIELD_DICTIONARY$label_pattern[[field_index]]

    field_matches <- list()
    match_index <- 1L

    for (row_index in seq_len(nrow(rows))) {
      cells <- rows$cells[[row_index]]
      normalized_cells <- normalize_for_match(cells)

      label_positions <- which(
        stringr::str_detect(
          normalized_cells,
          stringr::regex(label_pattern, ignore_case = TRUE)
        )
      )

      if (length(label_positions) == 0L) {
        next
      }

      label_position <- label_positions[[1]]

      candidate_values <- if (label_position < length(cells)) {
        cells[seq.int(label_position + 1L, length(cells))]
      } else {
        character()
      }

      # 레이블이 값 후보에 반복된 경우 제거
      if (length(candidate_values) > 0L) {
        candidate_norm <- normalize_for_match(candidate_values)
        candidate_values <- candidate_values[
          !stringr::str_detect(
            candidate_norm,
            stringr::regex(label_pattern, ignore_case = TRUE)
          )
        ]
      }

      # 같은 셀 안의 '레이블: 값' 형식 보완
      if (length(candidate_values) == 0L) {
        label_cell <- cells[[label_position]]
        colon_parts <- stringr::str_split(
          label_cell,
          "[:：]",
          n = 2L,
          simplify = FALSE
        )[[1]]

        if (length(colon_parts) == 2L && nzchar(stringr::str_squish(colon_parts[[2]]))) {
          candidate_values <- colon_parts[[2]]
        }
      }

      # 레이블 다음 행에 값만 배치된 구조 보완
      if (
        length(candidate_values) == 0L &&
          row_index < nrow(rows)
      ) {
        next_cells <- rows$cells[[row_index + 1L]]
        next_norm <- normalize_for_match(next_cells)

        next_row_contains_label <- any(
          stringr::str_detect(
            next_norm,
            stringr::regex(all_label_pattern, ignore_case = TRUE)
          )
        )

        if (!next_row_contains_label) {
          candidate_values <- next_cells
        }
      }

      candidate_values <- clean_text(candidate_values)
      candidate_values <- candidate_values[!duplicated(candidate_values)]

      if (length(candidate_values) == 0L) {
        next
      }

      field_matches[[match_index]] <- tibble::tibble(
        field_name = field_name,
        field_value = paste(candidate_values, collapse = " "),
        source_file = rows$source_file[[row_index]],
        match_row_no = rows$row_no[[row_index]],
        match_row_text = rows$row_text[[row_index]]
      )

      match_index <- match_index + 1L
    }

    result[[field_index]] <- dplyr::bind_rows(field_matches)
  }

  dplyr::bind_rows(result) |>
    dplyr::distinct(
      field_name,
      field_value,
      source_file,
      match_row_no,
      .keep_all = TRUE
    )
}

# ------------------------------------------------------------
# 4. 접수번호 1건 파싱
# ------------------------------------------------------------

empty_source_meta <- function() {
  tibble::tibble(
    rcept_no = character(),
    source_file = character(),
    read_status = character(),
    read_message = character(),
    n_rows = integer(),
    text_length = integer(),
    source_score = integer(),
    is_primary = logical()
  )
}

empty_fields <- function() {
  tibble::tibble(
    rcept_no = character(),
    report_name = character(),
    event_type = character(),
    field_name = character(),
    field_value = character(),
    source_file = character(),
    match_row_no = integer(),
    match_row_text = character()
  )
}

parsed_receipt_path <- function(rcept_no) {
  year_dir <- file.path(
    PARSED_ROOT,
    stringr::str_sub(rcept_no, 1L, 4L)
  )

  dir.create(year_dir, recursive = TRUE, showWarnings = FALSE)

  file.path(year_dir, paste0(rcept_no, ".rds"))
}

save_rds_atomic <- function(object, path) {
  temp_path <- paste0(path, ".tmp")
  saveRDS(object, temp_path)

  if (file.exists(path)) {
    unlink(path, force = TRUE)
  }

  moved <- file.rename(temp_path, path)

  if (!moved) {
    stop("파싱 결과를 저장하지 못했습니다: ", path)
  }

  invisible(path)
}

parse_dart_receipt <- function(index_row) {
  rcept_no <- index_row$rcept_no[[1]]
  report_name <- index_row$report_name[[1]]
  report_name_clean <- index_row$report_name_clean[[1]]
  event_type <- index_row$event_type[[1]]
  title_reason <- index_row$title_reason[[1]]

  receipt_dir <- file.path(ORIGINAL_ROOT, rcept_no)
  source_files <- receipt_source_files(receipt_dir)

  base_summary <- tibble::tibble(
    rcept_no = rcept_no,
    rcept_date = index_row$rcept_date[[1]],
    market_badge = index_row$market_badge[[1]],
    company_name_index = index_row$company_name_index[[1]],
    report_name = report_name,
    report_name_clean = report_name_clean,
    title_reason = title_reason,
    event_type = event_type
  )

  if (!dir.exists(receipt_dir)) {
    return(
      list(
        summary = base_summary |>
          dplyr::mutate(
            parse_status = "not_downloaded",
            primary_source_file = NA_character_,
            n_source_files = 0L,
            n_primary_rows = 0L,
            n_extracted_fields = 0L,
            keyword_financial_reason = NA,
            keyword_regulatory_process = NA,
            keyword_technical_reason = NA,
            keyword_cleanup_trading = NA,
            keyword_normal_resume = NA,
            technical_only_candidate = NA
          ),
        fields = empty_fields(),
        rows = empty_rows() |>
          dplyr::mutate(rcept_no = character()),
        source_meta = empty_source_meta(),
        primary_text = ""
      )
    )
  }

  if (length(source_files) == 0L) {
    return(
      list(
        summary = base_summary |>
          dplyr::mutate(
            parse_status = "no_supported_source_file",
            primary_source_file = NA_character_,
            n_source_files = 0L,
            n_primary_rows = 0L,
            n_extracted_fields = 0L,
            keyword_financial_reason = NA,
            keyword_regulatory_process = NA,
            keyword_technical_reason = NA,
            keyword_cleanup_trading = NA,
            keyword_normal_resume = NA,
            technical_only_candidate = NA
          ),
        fields = empty_fields(),
        rows = empty_rows() |>
          dplyr::mutate(rcept_no = character()),
        source_meta = empty_source_meta(),
        primary_text = ""
      )
    )
  }

  parsed_sources <- purrr::map(
    source_files,
    parse_source_file,
    receipt_dir = receipt_dir,
    rcept_no = rcept_no
  )

  source_meta <- purrr::map_dfr(
    parsed_sources,
    "meta"
  ) |>
    dplyr::mutate(
      rcept_no = rcept_no,
      .source_index = dplyr::row_number()
    )

  readable_meta <- source_meta |>
    dplyr::filter(read_status == "ok") |>
    dplyr::arrange(
      dplyr::desc(source_score),
      dplyr::desc(n_rows),
      source_file
    )

  if (nrow(readable_meta) == 0L) {
    return(
      list(
        summary = base_summary |>
          dplyr::mutate(
            parse_status = "all_source_read_error",
            primary_source_file = NA_character_,
            n_source_files = length(source_files),
            n_primary_rows = 0L,
            n_extracted_fields = 0L,
            keyword_financial_reason = NA,
            keyword_regulatory_process = NA,
            keyword_technical_reason = NA,
            keyword_cleanup_trading = NA,
            keyword_normal_resume = NA,
            technical_only_candidate = NA
          ),
        fields = empty_fields(),
        rows = empty_rows() |>
          dplyr::mutate(rcept_no = character()),
        source_meta = source_meta |>
          dplyr::mutate(is_primary = FALSE) |>
          dplyr::select(-.source_index),
        primary_text = ""
      )
    )
  }

  primary_source_index <- readable_meta$.source_index[[1]]
  primary_source_file <- readable_meta$source_file[[1]]

  source_meta <- source_meta |>
    dplyr::mutate(
      is_primary = .source_index == primary_source_index
    )

  all_rows <- purrr::map_dfr(
    parsed_sources,
    "rows"
  ) |>
    dplyr::mutate(rcept_no = rcept_no, .before = 1L)

  primary_rows <- parsed_sources[[primary_source_index]]$rows
  primary_text <- parsed_sources[[primary_source_index]]$full_text

  fields <- extract_fields_from_rows(primary_rows) |>
    dplyr::mutate(
      rcept_no = rcept_no,
      report_name = report_name,
      event_type = event_type,
      .before = 1L
    )

  field_wide <- fields |>
    dplyr::group_by(field_name) |>
    dplyr::summarise(
      field_value = paste(
        unique(field_value),
        collapse = " || "
      ),
      .groups = "drop"
    ) |>
    tidyr::pivot_wider(
      names_from = field_name,
      values_from = field_value
    )

  primary_match_text <- normalize_for_match(primary_text)

  keyword_financial_reason <- stringr::str_detect(
    primary_match_text,
    stringr::regex(
      paste(
        c(
          "감사의견",
          "의견거절",
          "부적정",
          "감사범위제한",
          "계속기업",
          "자본잠식",
          "부도",
          "은행거래정지",
          "파산",
          "회생절차",
          "사업보고서미제출",
          "반기보고서미제출",
          "법인세비용차감전계속사업손실"
        ),
        collapse = "|"
      ),
      ignore_case = TRUE
    )
  )

  keyword_regulatory_process <- stringr::str_detect(
    primary_match_text,
    stringr::regex(
      paste(
        c(
          "상장적격성실질심사",
          "상장폐지사유",
          "개선기간",
          "기업심사위원회",
          "코스닥시장위원회",
          "시장위원회"
        ),
        collapse = "|"
      ),
      ignore_case = TRUE
    )
  )

  keyword_technical_reason <- stringr::str_detect(
    primary_match_text,
    stringr::regex(
      paste(
        c(
          "주식병합",
          "주식분할",
          "전자등록변경",
          "전자등록말소",
          "신주권변경상장",
          "변경상장일전일",
          "분할합병",
          "주식교환",
          "주식이전",
          "재상장"
        ),
        collapse = "|"
      ),
      ignore_case = TRUE
    )
  )

  keyword_cleanup_trading <- stringr::str_detect(
    primary_match_text,
    stringr::regex(
      "정리매매|상장폐지에따른정리매매개시",
      ignore_case = TRUE
    )
  )

  keyword_normal_resume <- stringr::str_detect(
    primary_match_text,
    stringr::regex(
      "상장유지결정|실질심사대상제외|매매거래재개",
      ignore_case = TRUE
    )
  )

  parse_status <- if (nrow(primary_rows) == 0L) {
    "ok_no_table_rows"
  } else if (nrow(fields) == 0L) {
    "ok_no_named_fields"
  } else {
    "ok"
  }

  summary <- base_summary |>
    dplyr::mutate(
      parse_status = parse_status,
      primary_source_file = primary_source_file,
      n_source_files = length(source_files),
      n_primary_rows = nrow(primary_rows),
      n_extracted_fields = nrow(fields),
      keyword_financial_reason = keyword_financial_reason,
      keyword_regulatory_process = keyword_regulatory_process,
      keyword_technical_reason = keyword_technical_reason,
      keyword_cleanup_trading = keyword_cleanup_trading,
      keyword_normal_resume = keyword_normal_resume,
      technical_only_candidate = (
        keyword_technical_reason &&
          !keyword_financial_reason &&
          !keyword_regulatory_process
      )
    )

  if (ncol(field_wide) > 0L) {
    summary <- dplyr::bind_cols(summary, field_wide)
  }

  list(
    summary = summary,
    fields = fields,
    rows = all_rows,
    source_meta = source_meta |>
      dplyr::select(-.source_index),
    primary_text = primary_text
  )
}

# ------------------------------------------------------------
# 5. 전체 접수번호 파싱: 접수번호별 RDS 저장으로 재개 지원
# ------------------------------------------------------------

empty_parse_log <- function() {
  tibble::tibble(
    rcept_no = character(),
    parse_status = character(),
    parsed_file = character(),
    parsed_at = character(),
    parse_message = character()
  )
}

save_parse_log <- function(parse_log) {
  saveRDS(parse_log, PARSE_LOG_RDS)
  readr::write_csv(parse_log, PARSE_LOG_CSV, na = "")
  invisible(parse_log)
}

upsert_parse_log <- function(parse_log, new_row) {
  dplyr::bind_rows(parse_log, new_row) |>
    dplyr::mutate(.log_order = dplyr::row_number()) |>
    dplyr::group_by(rcept_no) |>
    dplyr::slice_max(.log_order, n = 1L, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(-.log_order)
}

if (file.exists(PARSE_LOG_RDS)) {
  parse_log <- readRDS(PARSE_LOG_RDS)
} else {
  parse_log <- empty_parse_log()
}

for (i in seq_len(nrow(halt_index))) {
  index_row <- halt_index[i, ]
  rcept_no <- index_row$rcept_no[[1]]
  output_path <- parsed_receipt_path(rcept_no)

  if (!OVERWRITE_PARSE && file.exists(output_path)) {
    parse_log <- upsert_parse_log(
      parse_log,
      tibble::tibble(
        rcept_no = rcept_no,
        parse_status = "cached",
        parsed_file = output_path,
        parsed_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S%z"),
        parse_message = "기존 접수번호별 파싱 RDS 사용"
      )
    )
  } else {
    parsed_object <- tryCatch(
      parse_dart_receipt(index_row),
      error = function(e) e
    )

    if (inherits(parsed_object, "error")) {
      parse_log <- upsert_parse_log(
        parse_log,
        tibble::tibble(
          rcept_no = rcept_no,
          parse_status = "parse_error",
          parsed_file = NA_character_,
          parsed_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S%z"),
          parse_message = conditionMessage(parsed_object)
        )
      )
    } else {
      save_rds_atomic(parsed_object, output_path)

      parse_log <- upsert_parse_log(
        parse_log,
        tibble::tibble(
          rcept_no = rcept_no,
          parse_status = parsed_object$summary$parse_status[[1]],
          parsed_file = output_path,
          parsed_at = format(Sys.time(), "%Y-%m-%d %H:%M:%S%z"),
          parse_message = NA_character_
        )
      )
    }
  }

  if (
    i == 1L ||
      i %% PROGRESS_EVERY == 0L ||
      i == nrow(halt_index)
  ) {
    save_parse_log(parse_log)

    message(
      sprintf(
        "[파싱 %s/%s] %s",
        format(i, big.mark = ","),
        format(nrow(halt_index), big.mark = ","),
        rcept_no
      )
    )
  }
}

save_parse_log(parse_log)

# ------------------------------------------------------------
# 6. 접수번호별 RDS를 요약 CSV로 결합
#    전체 raw rows는 각 RDS 내부에 보존하므로 한꺼번에 결합하지 않음
# ------------------------------------------------------------

parsed_files <- list.files(
  PARSED_ROOT,
  pattern = "\\.rds$",
  recursive = TRUE,
  full.names = TRUE
)

safe_read_parsed <- function(path) {
  tryCatch(
    readRDS(path),
    error = function(e) NULL
  )
}

parsed_objects <- purrr::map(parsed_files, safe_read_parsed)
parsed_objects <- parsed_objects[!vapply(parsed_objects, is.null, logical(1))]

parsed_summary <- purrr::map_dfr(parsed_objects, "summary") |>
  dplyr::arrange(dplyr::desc(rcept_no))

parsed_fields <- purrr::map_dfr(parsed_objects, "fields") |>
  dplyr::arrange(dplyr::desc(rcept_no), field_name, match_row_no)

parsed_source_meta <- purrr::map_dfr(parsed_objects, "source_meta") |>
  dplyr::arrange(dplyr::desc(rcept_no), dplyr::desc(is_primary), source_file)

readr::write_csv(parsed_summary, PARSED_SUMMARY_PATH, na = "")
readr::write_csv(parsed_fields, PARSED_FIELDS_PATH, na = "")
readr::write_csv(parsed_source_meta, PARSED_SOURCE_META_PATH, na = "")

cat("\n===== 파싱 상태 =====\n")
print(
  parsed_summary |>
    dplyr::count(event_type, parse_status, sort = TRUE),
  n = Inf
)

cat("\n===== 공시 유형별 추출 필드 =====\n")
print(
  parsed_fields |>
    dplyr::count(event_type, field_name, sort = TRUE),
  n = Inf
)

cat("\n===== 키워드 후보 분포 =====\n")
print(
  parsed_summary |>
    dplyr::summarise(
      financial_keyword_n = sum(keyword_financial_reason %in% TRUE, na.rm = TRUE),
      regulatory_keyword_n = sum(keyword_regulatory_process %in% TRUE, na.rm = TRUE),
      technical_keyword_n = sum(keyword_technical_reason %in% TRUE, na.rm = TRUE),
      technical_only_candidate_n = sum(technical_only_candidate %in% TRUE, na.rm = TRUE),
      cleanup_trading_keyword_n = sum(keyword_cleanup_trading %in% TRUE, na.rm = TRUE),
      normal_resume_keyword_n = sum(keyword_normal_resume %in% TRUE, na.rm = TRUE)
    )
)

# ------------------------------------------------------------
# 7. 접수번호 1건의 원문 행을 확인하는 예시
# ------------------------------------------------------------
# sample_object <- readRDS(
#   parsed_receipt_path("20260723800418")
# )
#
# sample_object$summary
# sample_object$fields
# print(sample_object$rows, n = Inf, width = Inf)
