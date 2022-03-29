#' Check ID COlumns
#'
#' Description
#'
#' @param .source
#' The Source Dataframe.
#' Must contain a unique column id and the columns you want to match on
#' @param .target
#' The Target Dataframe.
#' Must contain a unique column id and the columns you want to match on
#'
#' @param .error Return Error?
#'
#' @return Either Errors or a list
check_id <- function(.source, .target, .error = TRUE) {
  cols_s_ <- colnames(.source)
  cols_t_ <- colnames(.target)

  .source <- tibble::as_tibble(.source)
  .target <- tibble::as_tibble(.target)

  es_ <- "id" %in% cols_s_
  et_ <- "id" %in% cols_t_

  if (es_) us_ <- !any(duplicated(.source[["id"]])) else us_ <- NA
  if (et_) ut_ <- !any(duplicated(.target[["id"]])) else ut_ <- NA

  if (.error) {
    if (!es_ & !et_) {
      stop("Both datasets must have an 'id' column", call. = FALSE)
    } else if (!es_) {
      stop("Source dataset must have an 'id' column", call. = FALSE)
    } else if (!et_) {
      stop("Target dataset must have an 'id' column", call. = FALSE)
    }

    if (!us_ & !ut_) {
      stop("Both datasets must have unique IDs", call. = FALSE)
    } else if (!us_) {
      stop("Source dataset must have unique IDs", call. = FALSE)
    } else if (!ut_) {
      stop("Target dataset must have aunique IDs", call. = FALSE)
    }
  }

  list(e_s = es_, e_t = et_, u_s = us_, u_t = ut_)
}

#' Check columns for NA values
#'
#' Description
#' @param .source
#' The Source Dataframe.
#' Must contain a unique column id and the columns you want to match on
#' @param .target
#' The Target Dataframe.
#' Must contain a unique column id and the columns you want to match on
#' @param .check
#' Check only column that are also in source, or all columns
#' @return A list with the number of NAs
check_nas <- function(.source, .target, .check = c("source", "all")) {
  check_ <- match.arg(.check, c("source", "all"))

  .source <- tibble::as_tibble(.source)
  .target <- tibble::as_tibble(.target)

  cols_s_ <- stats::setNames(colnames(.source), paste0("s_", colnames(.source)))
  cols_t_ <- stats::setNames(colnames(.target), paste0("t_", colnames(.target)))

  if (check_ == "source") {
    cols_t_ <- cols_t_[cols_t_ %in% cols_s_]
  }


  c(
    purrr::map_int(cols_s_, ~ sum(is.na(.source[[.x]]))),
    purrr::map_int(cols_t_, ~ sum(is.na(.target[[.x]])))
  )
}

#' Check Duplicates
#'
#' Description
#'
#' @param .source
#' The Source Dataframe.
#' Must contain a unique column id and the columns you want to match on
#' @param .target
#' The Target Dataframe.
#' Must contain a unique column id and the columns you want to match on
#' @param .check
#' Check only column that are also in source, or all columns
#' @return A list with duplicates
check_dup <- function(.source, .target, .check = c("source", "all")) {
  check_ <- match.arg(.check, c("source", "all"))

  .source <- tibble::as_tibble(.source)
  .target <- tibble::as_tibble(.target)

  cols_s_ <- stats::setNames(colnames(.source), paste0("s_", colnames(.source)))
  cols_t_ <- stats::setNames(colnames(.target), paste0("t_", colnames(.target)))
  cols_s_ <- cols_s_[!cols_s_ == "id"]
  cols_t_ <- cols_t_[!cols_t_ == "id"]
  cols_t_ <- cols_t_[order(match(cols_t_,cols_s_))]

  if (check_ == "source") {
    cols_t_ <- cols_t_[cols_t_ %in% cols_s_]
  }

  s_ <- tibble::as_tibble(.source)
  t_ <- tibble::as_tibble(.target)



  ind_ <- c(
    purrr::map_int(cols_s_, ~ sum(duplicated(s_[[.x]]))),
    purrr::map_int(cols_t_, ~ sum(duplicated(t_[[.x]])))
  )

  cum_ <- c(
    purrr::map_int(
      .x = stats::setNames(seq_len(length(cols_s_)), names(cols_s_)),
      .f = ~ sum(duplicated(apply(s_[, cols_s_[1:.x]], 1, paste, collapse = "-")))
    ),
    purrr::map_int(
      .x = stats::setNames(seq_len(length(cols_t_)), names(cols_t_)),
      .f = ~ sum(duplicated(apply(t_[, cols_t_[1:.x]], 1, paste, collapse = "-")))
    )
  )

  list(ind = ind_, cum = cum_)

}


#' Match a on a single column
#'
#' Description
#'
#' @param .source
#' The Source Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .target
#' The Target Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .cols_match
#' A character vector of columns to perform fuzzy matching.
#' @param .max_match
#' Maximum number of matches to return (Default = 10)
#' @param .method
#' One of "osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex".\cr
#' See: stringdist-metrics {stringdist}
#' @param .workers
#' Number of cores to utilize (Default all cores determined by future::availableCores())
#'
#' @import data.table
#'
#' @return A Dataframe
match_col <- function(
    .source, .target, .cols_match, .max_match = 10, .method = "osa",
    .workers = future::availableCores()
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-match_col.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  V1 <- value <- id <- name <- id_t <- sim <- NULL

  # Output -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  col_ <- .cols_match[1]
  tab_ <- stringdist::stringsimmatrix(
    a = .source[[col_]],
    b = .target[[col_]],
    method = method_,
    nthread = .workers
  ) %>%
    tibble::as_tibble() %>%
    dtplyr::lazy_dt() %>%
    dplyr::mutate(id = dplyr::row_number(), .before = V1) %>%
    tidyr::pivot_longer(!dplyr::matches("id")) %>%
    dplyr::group_by(id) %>%
    dplyr::arrange(dplyr::desc(value)) %>%
    dplyr::filter(dplyr::row_number() %in% seq_len(.max_match)) %>%
    dplyr::ungroup() %>%
    dplyr::rename(id_s = id, id_t = name) %>%
    dplyr::mutate(id_t = as.integer(gsub("V", "", id_t, fixed = TRUE))) %>%
    suppressWarnings() %>%
    tibble::as_tibble()

  # tab_[[paste0(col_, "_s")]] <- .source[[col_]][tab_[["id_s"]]]
  # tab_[[paste0(col_, "_t")]] <- .target[[col_]][tab_[["id_t"]]]
  tab_[["id_s"]] <- .source[["id"]][tab_[["id_s"]]]
  tab_[["id_t"]] <- .target[["id"]][tab_[["id_t"]]]


  colnames(tab_) <- c("id_s", "id_t", paste0("sim_", col_))
  return(tab_)
}

#' Match a on a single column
#'
#' Description
#'
#' @param .source
#' The Source Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .target
#' The Target Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .cols_match
#' A character vector of columns to perform fuzzy matching.
#' @param .max_match
#' Maximum number of matches to return (Default = 10)
#' @param .method
#' One of "osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex".\cr
#' See: stringdist-metrics {stringdist}
#' @param .workers
#' Number of cores to utilize (Default all cores determined by future::availableCores())
#'
#' @return A Dataframe
match_cols <- function(
    .source, .target, .cols_match, .max_match = 10, .method = "osa",
    .workers = future::availableCores()
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-match_col.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tmp.x <- tmp.y <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Shortcut Functions -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  symp <- function(...) dplyr::sym(paste0(...))

  # Prepare Tables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  s_ <- .source
  t_ <- .target

  # Match all Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  lst_ <- purrr::map(.cols_match, ~ match_col(s_, t_, .x, .max_match, .method, .workers))
  tab_ <- purrr::reduce(lst_, dplyr::full_join, by = c("id_s", "id_t"))

  # Fill Missing Values -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  for (col in .cols_match) {
    tmp_s_ <- purrr::set_names(s_[, c("id", col)], c("id_s", "tmp"))
    tmp_t_ <- purrr::set_names(t_[, c("id", col)], c("id_t", "tmp"))
    tab_ <- tab_ %>%
      #tplyr::lazy_dt() %>%
      dplyr::left_join(tmp_s_, by = "id_s") %>%
      dplyr::left_join(tmp_t_, by = "id_t") %>%
      dplyr::mutate(!!symp("sim_", col) := dplyr::if_else(
        is.na(!!symp("sim_", col)),
        stringdist::stringsim(tmp.x, tmp.y, method = method_),
        !!symp("sim_", col)
      )) %>%
      dplyr::select(-tmp.x, -tmp.y) %>%
      tibble::as_tibble()
  }

  return(tab_)
}

#' Split Inputs
#'
#' @param .tab Either Source or Target Dataframe
#' @param .cols_exact Columns used for Splitting
#' @param .split Maximum Number of Items to process in the Source Dataframe
#' @param .type c("source", "target")
#'
#' @return A nested Dataframe
split_input <- function(.tab, .cols_exact = character(), .split = Inf,
                        .type = c("source", "target")) {


  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  value <- NULL

  if (length(.cols_exact) > 0) {
    vec_ <- tidyr::unite(.tab[, .cols_exact], "tmp", dplyr::everything())[["tmp"]]
    lst_ <- split(.tab, vec_)

    if (.type == "source") {
      lst_ <- purrr::map(lst_, ~ split(.x, ceiling(seq_len(nrow(.x)) / .split)))
      tidyr::unnest(tibble::enframe(lst_, name = "split"), value)
    } else {
      tibble::enframe(lst_, name = "split")
    }

  } else {
    if (.type == "source") {
      lst_ <- split(.tab, ceiling(seq_len(nrow(.tab)) / .split))
    } else {
      lst_ <- list(.tab)
    }

    dplyr::mutate(tibble::enframe(lst_, name = "split"), split = "all")

  }
}

#' Get Available Memory
#'
#' @return An Integer
get_avail_mem <- function() {
  osName <- Sys.info()[["sysname"]]

  if (osName == "Windows") {
    x <- system2("wmic", args = "OS get FreePhysicalMemory /Value", stdout = TRUE)
    x <- x[grepl("FreePhysicalMemory", x)]
    x <- gsub("FreePhysicalMemory=", "", x, fixed = TRUE)
    x <- gsub("\r", "", x, fixed = TRUE)
    return(as.numeric(x) * 1000)
  } else if (osName == "Linux") {
    x <- system2("free", args = "-k", stdout = TRUE)
    x <- strsplit(x[2], " +")[[1]][4]
    return(as.numeric(x) * 1000)
  } else {
    stop("Only supported on Windows and Linux")
  }
}
