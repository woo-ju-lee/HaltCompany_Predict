# ============================================================
# 01_download_dart_originals.R
# DART 검색 인덱스 -> OpenDART 공시서류 원문 ZIP 다운로드/압축해제
# - 8천 건 이상을 전제로 중간 저장, 재개, 캐시 검증 지원
# - API 020(요청 제한), 인증키/IP 오류 발생 시 즉시 중단
# ============================================================

library(tidyverse)
library(httr)
library(xml2)

# ------------------------------------------------------------
# 0. 사용자 설정
# ------------------------------------------------------------

PROJECT_DIR <- path.expand("~/HaltCompany_Predict")
INDEX_PATH <- file.path(PROJECT_DIR, "dart_trading_halt_search.csv")
ORIGINAL_ROOT <- file.path(PROJECT_DIR, "dart_original")
ZIP_ROOT <- file.path(ORIGINAL_ROOT, "_zip")

CLASSIFIED_INDEX_PATH <- file.path(
  PROJECT_DIR,
  "dart_trading_halt_index_classified.csv"
)

DOWNLOAD_LOG_RDS <- file.path(
  PROJECT_DIR,
  "dart_download_log.rds"
)

DOWNLOAD_LOG_CSV <- file.path(
  PROJECT_DIR,
  "dart_download_log.csv"
)

# "all": 검색된 8,311건 전체 다운로드
# "research": 거래정지·기간변경·해제·상장폐지·내부결산·기타시장안내 우선 다운로드
DOWNLOAD_SCOPE <- "all"

OVERWRITE <- FALSE
REQUEST_DELAY_SECONDS <- 0.35
CHECKPOINT_EVERY <- 25L
MAX_NETWORK_REQUESTS_THIS_RUN <- 10000L

# 현재 사용 중인 환경변수명에 맞춤
DART_API_KEY <- Sys.getenv("DART_FSS")

if (!nzchar(DART_API_KEY)) {
  stop(
    paste(
      "DART_FSS 환경변수가 설정되지 않았습니다.",
      "예: Sys.setenv(DART_FSS = '본인의_40자리_API_KEY')"
    ),
    call. = FALSE
  )
}

if (!file.exists(INDEX_PATH)) {
  stop("검색 인덱스 파일을 찾을 수 없습니다: ", INDEX_PATH, call. = FALSE)
}

dir.create(PROJECT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(ORIGINAL_ROOT, recursive = TRUE, showWarnings = FALSE)
dir.create(ZIP_ROOT, recursive = TRUE, showWarnings = FALSE)

# ------------------------------------------------------------
# 1. 검색 인덱스 읽기 및 보고서 유형 분류
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

halt_index <- readr::read_csv(
  INDEX_PATH,
  col_types = readr::cols(
    rcept_no = readr::col_character(),
    report_name = readr::col_character(),
    detail_url = readr::col_character(),
    row_text = readr::col_character()
  ),
  show_col_types = FALSE
) |>
  dplyr::distinct(rcept_no, .keep_all = TRUE) |>
  dplyr::mutate(
    rcept_no = stringr::str_trim(rcept_no),
    report_name_clean = normalize_report_name(report_name),
    is_correction = stringr::str_detect(
      tidyr::replace_na(report_name, ""),
      "^(?:\\[(?:기재정정|첨부정정|첨부추가)\\])"
    ),
    event_type = classify_report_type(report_name),
    rcept_date = as.Date(
      stringr::str_sub(rcept_no, 1L, 8L),
      format = "%Y%m%d"
    ),
    download_priority = dplyr::case_when(
      event_type %in% c(
        "HALT_START",
        "HALT_AND_RELEASE",
        "HALT_PERIOD_CHANGE",
        "HALT_RELEASE",
        "DELISTING_RELATED"
      ) ~ 1L,
      event_type %in% c(
        "INTERNAL_SETTLEMENT_WARNING",
        "MARKET_NOTICE"
      ) ~ 2L,
      TRUE ~ 3L
    ),
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
      stringr::str_squish(),
    rcept_no_from_url = stringr::str_match(
      tidyr::replace_na(detail_url, ""),
      "[?&]rcpNo=(\\d{14})"
    )[, 2]
  )

bad_rcept_no <- halt_index |>
  dplyr::filter(!stringr::str_detect(rcept_no, "^\\d{14}$"))

if (nrow(bad_rcept_no) > 0L) {
  stop(
    "14자리 형식이 아닌 rcept_no가 존재합니다: ",
    paste(utils::head(bad_rcept_no$rcept_no, 10L), collapse = ", "),
    call. = FALSE
  )
}

url_mismatch <- halt_index |>
  dplyr::filter(
    !is.na(rcept_no_from_url),
    rcept_no != rcept_no_from_url
  )

if (nrow(url_mismatch) > 0L) {
  stop(
    "rcept_no와 detail_url의 rcpNo가 일치하지 않는 행이 있습니다.",
    call. = FALSE
  )
}

readr::write_csv(
  halt_index,
  CLASSIFIED_INDEX_PATH,
  na = ""
)

message("고유 접수번호: ", format(nrow(halt_index), big.mark = ","), "건")

print(
  halt_index |>
    dplyr::count(event_type, sort = TRUE),
  n = Inf
)

if (identical(DOWNLOAD_SCOPE, "all")) {
  download_index <- halt_index
} else if (identical(DOWNLOAD_SCOPE, "research")) {
  download_index <- halt_index |>
    dplyr::filter(download_priority <= 2L)
} else {
  stop(
    "DOWNLOAD_SCOPE는 'all' 또는 'research'만 사용할 수 있습니다.",
    call. = FALSE
  )
}

# 요청 제한에 걸릴 경우 핵심 공시가 먼저 확보되도록 정렬
download_index <- download_index |>
  dplyr::arrange(
    download_priority,
    dplyr::desc(rcept_date),
    dplyr::desc(rcept_no)
  )

message(
  "이번 실행의 다운로드 대상: ",
  format(nrow(download_index), big.mark = ","),
  "건"
)

# ------------------------------------------------------------
# 2. 로그/파일 보조 함수
# ------------------------------------------------------------

empty_download_log <- function() {
  tibble::tibble(
    rcept_no = character(),
    download_status = character(),
    http_status = integer(),
    api_status = character(),
    api_message = character(),
    n_files = integer(),
    zip_bytes = double(),
    network_request = logical(),
    attempted_at = character()
  )
}

save_rds_atomic <- function(object, path) {
  temp_path <- paste0(path, ".tmp")
  saveRDS(object, temp_path)

  if (file.exists(path)) {
    unlink(path, force = TRUE)
  }

  moved <- file.rename(temp_path, path)

  if (!moved) {
    stop("RDS 로그 파일을 원자적으로 저장하지 못했습니다: ", path)
  }

  invisible(path)
}

save_download_log <- function(download_log) {
  save_rds_atomic(download_log, DOWNLOAD_LOG_RDS)

  readr::write_csv(
    download_log,
    DOWNLOAD_LOG_CSV,
    na = ""
  )

  invisible(download_log)
}

upsert_download_log <- function(download_log, new_row) {
  dplyr::bind_rows(download_log, new_row) |>
    dplyr::mutate(.log_order = dplyr::row_number()) |>
    dplyr::group_by(rcept_no) |>
    dplyr::slice_max(.log_order, n = 1L, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::select(-.log_order)
}

receipt_files <- function(receipt_dir) {
  if (!dir.exists(receipt_dir)) {
    return(character())
  }

  files <- list.files(
    receipt_dir,
    recursive = TRUE,
    full.names = TRUE,
    all.files = FALSE
  )

  files[file.exists(files) & !dir.exists(files)]
}

receipt_cache_is_valid <- function(receipt_dir) {
  files <- receipt_files(receipt_dir)

  source_files <- files[
    stringr::str_detect(
      files,
      stringr::regex("\\.(xml|html?|xhtml)$", ignore_case = TRUE)
    )
  ]

  if (length(source_files) == 0L) {
    return(FALSE)
  }

  file_sizes <- file.info(source_files)$size

  all(!is.na(file_sizes) & file_sizes > 0)
}

read_dart_api_error <- function(file_path) {
  doc <- tryCatch(
    xml2::read_xml(file_path, encoding = "UTF-8"),
    error = function(e) NULL
  )

  if (is.null(doc)) {
    fallback_message <- tryCatch(
      paste(
        readLines(
          file_path,
          warn = FALSE,
          encoding = "UTF-8"
        ),
        collapse = " "
      ),
      error = function(e) NA_character_
    )

    return(
      list(
        status = NA_character_,
        message = fallback_message
      )
    )
  }

  get_node_text <- function(xpath) {
    node <- xml2::xml_find_first(doc, xpath)

    if (inherits(node, "xml_missing")) {
      return(NA_character_)
    }

    stringr::str_squish(xml2::xml_text(node))
  }

  list(
    status = get_node_text(".//status"),
    message = get_node_text(".//message")
  )
}

# ------------------------------------------------------------
# 3. 접수번호 1건 다운로드
# ------------------------------------------------------------

download_dart_original <- function(
    rcept_no,
    api_key,
    root_dir,
    zip_root,
    overwrite = FALSE,
    max_tries = 4L
) {
  rcept_no <- as.character(rcept_no)
  attempted_at <- format(
    Sys.time(),
    "%Y-%m-%d %H:%M:%S%z"
  )

  receipt_dir <- file.path(root_dir, rcept_no)
  zip_path <- file.path(zip_root, paste0(rcept_no, ".zip"))

  if (!overwrite && receipt_cache_is_valid(receipt_dir)) {
    cached_files <- receipt_files(receipt_dir)

    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "cached",
        http_status = NA_integer_,
        api_status = "000",
        api_message = "기존 압축해제 파일 사용",
        n_files = length(cached_files),
        zip_bytes = if (file.exists(zip_path)) {
          as.double(file.info(zip_path)$size)
        } else {
          NA_real_
        },
        network_request = FALSE,
        attempted_at = attempted_at
      )
    )
  }

  temp_download <- tempfile(
    pattern = paste0(rcept_no, "_"),
    tmpdir = zip_root,
    fileext = ".part"
  )

  temp_extract_dir <- file.path(
    root_dir,
    paste0(".", rcept_no, "_extracting_", Sys.getpid())
  )

  on.exit(
    {
      if (file.exists(temp_download)) {
        unlink(temp_download, force = TRUE)
      }

      if (dir.exists(temp_extract_dir)) {
        unlink(temp_extract_dir, recursive = TRUE, force = TRUE)
      }
    },
    add = TRUE
  )

  response <- tryCatch(
    httr::RETRY(
      verb = "GET",
      url = "https://opendart.fss.or.kr/api/document.xml",
      query = list(
        crtfc_key = api_key,
        rcept_no = rcept_no
      ),
      httr::user_agent(
        "HaltCompany_Predict/1.0 (OpenDART original document downloader)"
      ),
      httr::write_disk(temp_download, overwrite = TRUE),
      httr::timeout(60),
      times = max_tries,
      pause_base = 1,
      pause_cap = 8,
      terminate_on = c(400, 401, 403, 404)
    ),
    error = function(e) e
  )

  if (inherits(response, "error")) {
    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "request_error",
        http_status = NA_integer_,
        api_status = NA_character_,
        api_message = conditionMessage(response),
        n_files = 0L,
        zip_bytes = NA_real_,
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  http_status <- httr::status_code(response)

  if (http_status != 200L) {
    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "http_error",
        http_status = as.integer(http_status),
        api_status = NA_character_,
        api_message = paste0("HTTP ", http_status),
        n_files = 0L,
        zip_bytes = if (file.exists(temp_download)) {
          as.double(file.info(temp_download)$size)
        } else {
          NA_real_
        },
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  signature <- readBin(
    temp_download,
    what = "raw",
    n = 4L
  )

  is_zip <- length(signature) >= 2L && identical(
    as.integer(signature[1:2]),
    c(80L, 75L)
  )

  if (!is_zip) {
    api_error <- read_dart_api_error(temp_download)

    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "api_error",
        http_status = as.integer(http_status),
        api_status = api_error$status,
        api_message = api_error$message,
        n_files = 0L,
        zip_bytes = as.double(file.info(temp_download)$size),
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  zip_listing <- tryCatch(
    utils::unzip(temp_download, list = TRUE),
    error = function(e) e
  )

  if (
    inherits(zip_listing, "error") ||
      !is.data.frame(zip_listing) ||
      nrow(zip_listing) == 0L
  ) {
    zip_message <- if (inherits(zip_listing, "error")) {
      conditionMessage(zip_listing)
    } else {
      "ZIP 내부 파일 목록이 비어 있습니다."
    }

    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "invalid_zip",
        http_status = as.integer(http_status),
        api_status = NA_character_,
        api_message = zip_message,
        n_files = 0L,
        zip_bytes = as.double(file.info(temp_download)$size),
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  dir.create(
    temp_extract_dir,
    recursive = TRUE,
    showWarnings = FALSE
  )

  unzip_result <- tryCatch(
    utils::unzip(
      temp_download,
      exdir = temp_extract_dir
    ),
    error = function(e) e
  )

  if (inherits(unzip_result, "error")) {
    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "unzip_error",
        http_status = as.integer(http_status),
        api_status = NA_character_,
        api_message = conditionMessage(unzip_result),
        n_files = 0L,
        zip_bytes = as.double(file.info(temp_download)$size),
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  extracted_files <- receipt_files(temp_extract_dir)

  if (length(extracted_files) == 0L) {
    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "empty_extract",
        http_status = as.integer(http_status),
        api_status = NA_character_,
        api_message = "압축 해제 후 파일이 없습니다.",
        n_files = 0L,
        zip_bytes = as.double(file.info(temp_download)$size),
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  if (dir.exists(receipt_dir)) {
    unlink(receipt_dir, recursive = TRUE, force = TRUE)
  }

  moved <- file.rename(temp_extract_dir, receipt_dir)

  if (!moved) {
    return(
      tibble::tibble(
        rcept_no = rcept_no,
        download_status = "move_error",
        http_status = as.integer(http_status),
        api_status = NA_character_,
        api_message = "임시 압축해제 폴더를 최종 폴더로 이동하지 못했습니다.",
        n_files = 0L,
        zip_bytes = as.double(file.info(temp_download)$size),
        network_request = TRUE,
        attempted_at = attempted_at
      )
    )
  }

  copied <- file.copy(
    temp_download,
    zip_path,
    overwrite = TRUE
  )

  if (!isTRUE(copied)) {
    warning(
      "압축해제는 성공했지만 ZIP 원본 저장에 실패했습니다: ",
      rcept_no,
      call. = FALSE
    )
  }

  final_files <- receipt_files(receipt_dir)

  tibble::tibble(
    rcept_no = rcept_no,
    download_status = "downloaded",
    http_status = as.integer(http_status),
    api_status = "000",
    api_message = "정상",
    n_files = length(final_files),
    zip_bytes = as.double(file.info(temp_download)$size),
    network_request = TRUE,
    attempted_at = attempted_at
  )
}

# ------------------------------------------------------------
# 4. 전체 다운로드: 중간 저장·재개 지원
# ------------------------------------------------------------

if (file.exists(DOWNLOAD_LOG_RDS)) {
  download_log <- readRDS(DOWNLOAD_LOG_RDS)
} else {
  download_log <- empty_download_log()
}

fatal_api_statuses <- c(
  "010", # 등록되지 않은 키
  "011", # 사용할 수 없는 키
  "012", # 접근할 수 없는 IP
  "020", # 요청 제한 초과
  "100", # 부적절한 필드 값
  "101", # 부적절한 접근
  "800", # 시스템 점검
  "901"  # 개인정보 보유기간 만료
)

network_request_count <- 0L

for (i in seq_len(nrow(download_index))) {
  current <- download_index[i, ]
  rcept_no <- current$rcept_no[[1]]

  result_row <- download_dart_original(
    rcept_no = rcept_no,
    api_key = DART_API_KEY,
    root_dir = ORIGINAL_ROOT,
    zip_root = ZIP_ROOT,
    overwrite = OVERWRITE
  ) |>
    dplyr::mutate(
      event_type = current$event_type[[1]],
      report_name = current$report_name[[1]],
      download_priority = current$download_priority[[1]]
    )

  if (isTRUE(result_row$network_request[[1]])) {
    network_request_count <- network_request_count + 1L
  }

  download_log <- upsert_download_log(
    download_log,
    result_row
  )

  if (
    i == 1L ||
      i %% CHECKPOINT_EVERY == 0L ||
      i == nrow(download_index)
  ) {
    save_download_log(download_log)

    message(
      sprintf(
        paste0(
          "[%s/%s] %s | 상태=%s | ",
          "이번 실행 API 요청=%s"
        ),
        format(i, big.mark = ","),
        format(nrow(download_index), big.mark = ","),
        rcept_no,
        result_row$download_status[[1]],
        format(network_request_count, big.mark = ",")
      )
    )
  }

  api_status <- result_row$api_status[[1]]
  http_status <- result_row$http_status[[1]]

  if (!is.na(api_status) && api_status %in% fatal_api_statuses) {
    save_download_log(download_log)

    stop(
      paste0(
        "OpenDART 치명적 오류로 중단합니다. ",
        "rcept_no=", rcept_no,
        ", status=", api_status,
        ", message=", result_row$api_message[[1]],
        "\n로그와 기존 파일은 보존됐으므로 원인 해소 후 같은 스크립트를 다시 실행하면 됩니다."
      ),
      call. = FALSE
    )
  }

  if (!is.na(http_status) && http_status %in% c(401L, 403L)) {
    save_download_log(download_log)

    stop(
      "HTTP 인증/접근 오류로 중단합니다: ",
      http_status,
      call. = FALSE
    )
  }

  if (network_request_count >= MAX_NETWORK_REQUESTS_THIS_RUN) {
    save_download_log(download_log)

    stop(
      paste0(
        "이번 실행의 네트워크 요청 상한 ",
        MAX_NETWORK_REQUESTS_THIS_RUN,
        "건에 도달했습니다. 같은 스크립트를 다시 실행하면 캐시된 건을 건너뛰고 재개합니다."
      ),
      call. = FALSE
    )
  }

  if (
    isTRUE(result_row$network_request[[1]]) &&
      REQUEST_DELAY_SECONDS > 0
  ) {
    Sys.sleep(
      stats::runif(
        1L,
        REQUEST_DELAY_SECONDS,
        REQUEST_DELAY_SECONDS + 0.15
      )
    )
  }
}

save_download_log(download_log)

cat("\n===== 다운로드 상태 요약 =====\n")
print(
  download_log |>
    dplyr::count(download_status, sort = TRUE),
  n = Inf
)

cat("\n===== API 오류 요약 =====\n")
print(
  download_log |>
    dplyr::filter(download_status == "api_error") |>
    dplyr::count(api_status, api_message, sort = TRUE),
  n = Inf
)

cat("\n===== 다운로드/캐시 성공 건수 =====\n")
print(
  download_log |>
    dplyr::summarise(
      success_n = sum(download_status %in% c("downloaded", "cached")),
      total_n = dplyr::n()
    )
)

