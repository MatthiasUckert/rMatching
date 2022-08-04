#' Standardize Strings
#'
#' Description
#'
#' @param .str
#' A string
#' @param .op
#' One of c("space", "punct", "case", "ascii")
#'
#' @return A string
#'
#' @export
standardize_str <- function(.str, .op = c("space", "punct", "case", "ascii")) {
  str_ <- .str
  if ("ascii" %in% .op) {
    str_ <- stringi::stri_trans_general(str_, "Latin-ASCII")
  }
  if ("punct" %in% .op) {
    str_ <- trimws(stringi::stri_replace_all_regex(str_, "\\W", " "))
    str_ <- trimws(stringi::stri_replace_all_regex(str_, "[[:punct:]]", " "))
    if (!"space" %in% .op) {
      str_ <- trimws(stringi::stri_replace_all_regex(str_, "([[:blank:]]|[[:space:]])+", " "))
    }
  }
  if ("space" %in% .op) {
    str_ <- trimws(stringi::stri_replace_all_regex(str_, "([[:blank:]]|[[:space:]])+", " "))
  }
  if ("case" %in% .op) {
    str_ <- toupper(str_)
  }
  return(str_)
}

#' Prepare Table
#'
#' Description
#'
#' @param .tab A dataframe (either the source or target dataframe)
#' @param .cols_match A character vector of columns to perform fuzzy matching.
#'
#' @return A dataframe
prep_tables <- function(.tab, .cols_match) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tmp <- id <- `_id_` <- NULL

  # Output -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  reg_ <- paste(paste0("^", .cols_match, "$"), collapse = "|")

  tab_ <- tibble::as_tibble(.tab[, c("id", .cols_match)]) %>%
    dplyr::mutate(dplyr::across(dplyr::matches(reg_), standardize_str))

  tab_ %>%
    tidyr::unite(tmp, !dplyr::matches("^id$"), remove = FALSE) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(tmp) %>%
    dplyr::summarise(
      `_id_` = list(id),
      id = dplyr::first(id),
      .groups = "drop"
    ) %>%
    dplyr::select(id, `_id_`) %>%
    dplyr::left_join(tab_, by = "id") %>%
    tibble::as_tibble()
}


#' Check ID Columns
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

check_names <- function(.cols_match) {
  names_ <- names(.cols_match)
  if (!all(tolower(names_) %in% c("fuzzy", "exact", "e", "f"))) {
    msg_ <- paste0(
      "\nArgument '.cols_match' must have names.",
      "\nExact Matching: 'exact' or 'e'",
      "\nFuzzy Matching: 'fuzzy' or 'd'",
      "\nNames are case insenstive, both full name or first letter are possibe"
    )
    stop(msg_, call. = FALSE)
  }
}

#' Make Groups
#'
#' @param .vec A Numeric Vector
#' @param .max Maximal groups size
#'
#' @return A Numeric Vectore
make_groups_vec <- function(.vec, .max) {

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  val <- ord <- group <- NULL

  tab_ <- tibble::tibble(val = .vec) %>%
    dplyr::mutate(ord = dplyr::row_number()) %>%
    dplyr::arrange(val) %>%
    dplyr::mutate(sum = NA_integer_)

  group_ <- 1
  for (i in seq_len(nrow(tab_))) {
    sum_ <- ifelse(i == 1, tab_[["val"]][i], tab_[["val"]][i] + tab_[["sum"]][i - 1])
    if (sum_ > .max) {
      sum_ <- tab_[["val"]][i]
      group_ <- group_ + 1
    }
    tab_[["sum"]][i] <- sum_
    tab_[["group"]][i] <- group_
  }

  dplyr::pull(dplyr::arrange(tab_, ord), group)
}

#' Split and Check Input
#'
#' @param .source
#' The Source Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .target
#' The Target Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .cols_match
#' A character vector of columns to perform fuzzy matching.
#' @param .mat_size
#' Maximal Matrix Size
#'
#' @return A dataframe or an error
check_split_input <- function(.source, .target, .cols_match, .mat_size = 1e3) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-check_split_input.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  nchar_s <- nchar_t <- n_s <- n_t <- size <- mat_size <- miss <- group <- NULL

  # Get Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  cols_f_ <- .cols_match[names(.cols_match) %in% c("fuzzy", "f")]
  cols_e_ <- .cols_match[names(.cols_match) %in% c("exact", "e")]
  reg_f_  <- paste(paste0("^", cols_f_, "$"), collapse = "|")
  reg_e_  <- paste(paste0("^", cols_e_, "$"), collapse = "|")


  # Caclulate NChars for Fuzzy COlumns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  source0_ <- .source %>%
    dplyr::mutate(dplyr::mutate(dplyr::across(dplyr::matches(reg_f_), nchar))) %>%
    tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "nchar_s") %>%
    dplyr::count(!!!dplyr::syms(unname(cols_e_)), col, nchar_s, name = "n_s")

  target0_ <- .target %>%
    dplyr::mutate(dplyr::mutate(dplyr::across(dplyr::matches(reg_f_), nchar))) %>%
    tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "nchar_t") %>%
    dplyr::count(!!!dplyr::syms(unname(cols_e_)), col, nchar_t, name = "n_t")

  combin_ <- dplyr::inner_join(source0_, target0_, by = unname(c(cols_e_, "col"))) %>%
    dplyr::mutate(
      diff = abs(nchar_s - nchar_t),
      size = n_s * n_t
    ) %>%
    dplyr::filter(!is.na(size), !is.na(diff)) %>%
    dplyr::select(!!!dplyr::syms(unname(cols_e_)), col, nchar_s, nchar_t, diff, n_s, n_t, size) %>%
    dplyr::group_by(!!!dplyr::syms(unname(cols_e_)), col, nchar_s) %>%
    dplyr::arrange(diff, .by_group = TRUE) %>%
    dplyr::mutate(mat_size = cumsum(size)) %>%
    dplyr::filter(mat_size <= 1.1 * .mat_size) %>%
    dplyr::summarise(
      mat_size = dplyr::last(mat_size),
      nchar_t = list(nchar_t),
      .groups = "drop_last"
    ) %>%
    dplyr::mutate(group = make_groups_vec(mat_size, (1.1 * .mat_size))) %>%
    dplyr::arrange(group, .by_group = TRUE) %>%
    dplyr::group_by(!!!dplyr::syms(unname(cols_e_)), col, group) %>%
    dplyr::summarise(
      nchar_s = list(nchar_s),
      nchar_t = list(unique(unlist(nchar_t))),
      mat_size = sum(mat_size),
      .groups = "drop"
    )

  if (length(cols_e_) > 0) {
    miss_t_ <- dplyr::anti_join(source0_, target0_, by = unname(c(cols_e_, "col"))) %>%
      dplyr::distinct(!!!dplyr::syms(unname(cols_e_))) %>%
      tidyr::unite(miss, !!!dplyr::syms(unname(cols_e_)), sep = " & ")
    miss_s_ <- dplyr::anti_join(target0_, source0_, by = unname(c(cols_e_, "col"))) %>%
      dplyr::distinct(!!!dplyr::syms(unname(cols_e_))) %>%
      tidyr::unite(miss, !!!dplyr::syms(unname(cols_e_)), sep = " & ")
    miss_ <- dplyr::bind_rows(miss_t_, miss_s_)

    if (nrow(miss_) > 0) {
      msg_s_ <- paste0("Source (N: ", nrow(miss_s_), "): ", paste(miss_s_$miss, collapse = ", "))
      msg_t_ <- paste0("Target (N: ", nrow(miss_t_), "): ", paste(miss_t_$miss, collapse = ", "))

      msg_s_ <- if (nchar(msg_s_) > 47) paste(stringi::stri_sub(msg_s_, 1, 47), "...") else msg_s_
      msg_t_ <- if (nchar(msg_t_) > 47) paste(stringi::stri_sub(msg_t_, 1, 47), "...") else msg_t_

      msg_ <- paste0(
        "Non-matching groups found.\n",
        "If in 'Source', those groups can't be matched.\n",
        "If in 'target', those groups won't be used for matching.\n",
        msg_s_, "\n",
        msg_t_
      )

      warning(msg_, call. = FALSE)

    }
  }

  return(combin_)

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
#' @param .col
#' A character vector of columns to perform fuzzy matching.
#' @param .max_match
#' Maximum number of matches to return (Default = 10)
#' @param .method
#' One of "osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex".\cr
#' See: stringdist-metrics {stringdist}
#' @param .workers
#' Number of cores to utilize (Default all cores determined by future::availableCores())
#' @param .join
#' Join Data first?
#'
#' @import data.table
#'
#' @return A Dataframe
match_col <- function(
    .source, .target, .col, .max_match = 10, .method = "osa",
    .workers = floor(future::availableCores() / 4), .join = TRUE
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-match_col.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  id <- NULL


  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  V1 <- value <- name <- id_t <- sim <- id_s <-  NULL

  # In-Function -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  reshape_mat <- function(.mat, .max_match) {
    Var1 <- Var2 <- NULL
    reshape2::melt(.mat) %>%
      dplyr::rename(id_s = Var1, id_t = Var2, sim = value) %>%
      dtplyr::lazy_dt() %>%
      dplyr::filter(sim > 0) %>%
      dplyr::group_by(id_s) %>%
      dplyr::arrange(-sim, .by_group = TRUE) %>%
      dplyr::filter(sim >= dplyr::nth(sim, .max_match)) %>%
      dplyr::ungroup() %>%
      dplyr::arrange(id_s, dplyr::desc(sim)) %>%
      tibble::as_tibble()
  }

  col_ <- unname(.col)
  s_ <- dplyr::select(.source, id, !!dplyr::sym(col_))
  t_ <- dplyr::select(.target, id, !!dplyr::sym(col_))


  if (.join) {
    tab0_ <- s_ %>%
      dplyr::inner_join(t_, by = col_, suffix = c("_s", "_t")) %>%
      dplyr::select(id_s, id_t) %>%
      dplyr::mutate(sim = 1)

    s_ <- dplyr::filter(s_, !id %in% tab0_$id_s)
    if (nrow(s_) == 0 | nrow(t_) == 0) return(tab0_)
  } else {
    tab0_ <- tibble::tibble()
  }

  tab1_ <- stringdist::stringsimmatrix(
    a = s_[[col_]],
    b = t_[[col_]],
    method = .method,
    nthread = .workers
  ) %>% reshape_mat(.max_match) %>%
    dplyr::mutate(
      id_s = s_[["id"]][id_s],
      id_t = t_[["id"]][id_t]
    )
  dplyr::bind_rows(tab0_, tab1_)

}
