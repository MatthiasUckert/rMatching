#' Standardize Strings
#'
#' Description
#'
#' @param .str A character vector
#' @param .op Any of c("space", "punct", "case", "ascii")
#'
#' @return A string
#' @export
#'
#' @examples
#' standardize_str(c("jkldsa   jkdhas   äää  §$ ## #'''"))
#' standardize_str(c("jkldsa   jkdhas   äää  §$ ## #'''"), "space")
standardize_str <- function(.str, .op = c("space", "punct", "case", "ascii")) {
  str_ <- .str

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

  if ("ascii" %in% .op) {
    str_ <- gsub("Ü", "UE", str_, fixed = TRUE)
    str_ <- gsub("Ä", "AE", str_, fixed = TRUE)
    str_ <- gsub("Ö", "OE", str_, fixed = TRUE)
    str_ <- gsub("ß", "SS", str_, fixed = TRUE)
    str_ <- stringi::stri_trans_general(str_, "Latin-ASCII")
  }

  return(str_)
}




#' Match Data
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
#' Number of cores to utilize (Default floor(future::availableCores() / 4))
#' @param .mat_size
#' Maximal Matrix Size
#' @param .verbose
#' Print Additional Information?
#' @param .join
#' Join Data first?
#'
#' @return A Dataframe
#' @export
match_data <- function(
    .source, .target, .cols_match, .max_match = 10, .method = "osa",
    .mat_size = 1e7,
    .workers = floor(future::availableCores() / 4), .verbose = TRUE,
    .join = TRUE
) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-match_data.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  value <- `_id_` <- sim <- values <- valuet <- `_id_s` <- `_id_t` <- id_s <- id_t <-
    group <- len_s <- len_t <- id <- NULL

  # Check if IDs are valid -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  if (.verbose) cat("\nChecking Inputs ...")
  .ci <- check_id(.source, .target)
  .cn <- check_names(.cols_match)
  if (.verbose) cat(" DONE!")

  # Prepare Tables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if (.verbose) cat("\nPreparing Source Dataframe ...")
  source_ <- prep_tables(.source, .cols_match)
  if (.verbose) cat(" DONE!")

  if (.verbose) cat("\nPreparing Target Dataframe ...")
  target_ <- prep_tables(.target, .cols_match)
  if (.verbose) cat(" DONE!")

  # Finding Optimal Groups -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  if (.verbose) cat("\nFinding Optimal Groups ...")
  tg_ <- check_split_input(source_, target_, .cols_match, .mat_size)
  if (.verbose) cat(" DONE!")

  # Get Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  cols_f_ <- .cols_match[names(.cols_match) %in% c("fuzzy", "f")]
  cols_e_ <- .cols_match[names(.cols_match) %in% c("exact", "e")]
  reg_f_  <- paste(paste0("^", cols_f_, "$"), collapse = "|")
  reg_e_  <- paste(paste0("^", cols_e_, "$"), collapse = "|")


  # Get Progress Bar -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  if (.verbose) cat("\nStart Matching ...")
  pb <- if (.verbose) progress::progress_bar$new(total = nrow(tg_))

  source_long_ <- source_ %>%
    tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "value") %>%
    dplyr::mutate(nchar = nchar(value))

  target_long_ <- target_ %>%
    tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "value") %>%
    dplyr::mutate(nchar = nchar(value))

  # Calculate Similarity -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  lst_out_ <- list()
  cols_ <- c(unname(cols_e_), "col")
  for (i in seq_len(nrow(tg_))) {
    if (.verbose) pb$tick()

    source_use_ <- source_long_ %>%
      dplyr::inner_join(tg_[i, cols_], by = cols_) %>%
      dplyr::filter(nchar %in% tg_$nchar_s[[i]])

    target_use_ <- target_long_ %>%
      dplyr::inner_join(tg_[i, cols_], by = cols_) %>%
      dplyr::filter(nchar %in% tg_$nchar_s[[i]])

    lst_out_[[i]] <- match_col(
      .source = source_use_,
      .target = target_use_,
      .col = "value",
      .max_match = .max_match,
      .method = method_,
      .workers = .workers,
      .join = .join
    )
  }

  if (.verbose) cat("\nPreparing Output ...")
  tab_out_ <- purrr::set_names(lst_out_, tg_$col) %>%
    dplyr::bind_rows(.id = "col") %>%
    dtplyr::lazy_dt() %>%
    dplyr::distinct() %>%
    tidyr::pivot_wider(names_from = col, values_from = "sim") %>%
    tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "sim") %>%
    dplyr::left_join(
      y = dplyr::select(source_long_, id, col, value, `_id_`),
      by = c("id_s" = "id", "col"),
      suffix = c("s", "t")
      ) %>%
    dplyr::left_join(
      y = dplyr::select(target_long_, id, col, value, `_id_`),
      by = c("id_t" = "id", "col"),
      suffix = c("s", "t")
    ) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      sim = dplyr::if_else(is.na(sim), stringdist::stringsim(values, valuet, method_), sim),
      values = NULL,
      valuet = NULL
      ) %>%
    tidyr::pivot_wider(names_from = col, names_prefix = "sim_", values_from = "sim") %>%
    dplyr::mutate(
      group = dplyr::row_number(),
      len_s = lengths(`_id_s`),
      len_t = lengths(`_id_t`),
      id_s = `_id_s`,
      id_t = `_id_t`,
      `_id_s` = NULL,
      `_id_t` = NULL
    ) %>%
    tidyr::unnest(id_s) %>%
    tidyr::unnest(id_t) %>%
    dplyr::distinct(id_s, id_t, .keep_all = TRUE) %>%
    dplyr::select(group, len_s, len_t, dplyr::everything())
  if (.verbose) cat(" DONE!")



  return(tab_out_)


}


#' Score Data
#'
#' Description
#'
#' @param .matches
#' Dataframe produced by match_data()
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
#' Number of cores to utilize (Default floor(future::availableCores() / 4))
#' @param .mat_size
#' Maximal Matrix Size
#' @param .verbose
#' Print Additional Information?
#'
#' @return A dataframe
#'
#' @export
uniqueness_data <- function(
    .matches, .target, .source, .cols_match, .max_match = 10, .method = "osa",
    .mat_size = 1e7,
    .workers = floor(future::availableCores() / 4), .verbose = TRUE
    ) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-uniqueness_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  id_s <- id_t <- sim <- uni <- group <- len_s <- len_t <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Check if IDs are valid -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  if (.verbose) cat("\nChecking Inputs ...")
  .ci <- check_id(.source, .target)
  .cn <- check_names(.cols_match)


  # Prepare Data -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  if (.verbose) cat("\nPreparing Source Dataframe ...")

  source_  <- prep_tables(.source, .cols_match)
  if (.verbose) cat("\nPreparing Target Dataframe ...")

  target_  <- prep_tables(.target, .cols_match)
  m_ <- tibble::as_tibble(.matches)

  # Get Exact and Fuzzy Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  cols_f_ <- .cols_match[names(.cols_match) %in% c("fuzzy", "f")]
  cols_e_ <- .cols_match[names(.cols_match) %in% c("exact", "e")]
  cols_f_ <- purrr::set_names(cols_f_, cols_f_)

  # Calculate Uniqueness -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  tmp_s_ <- dplyr::left_join(dplyr::distinct(m_, id_s), source_, by = c("id_s" = "id")) %>%
    dplyr::rename(id = id_s)
  tmp_t_ <- dplyr::left_join(dplyr::distinct(m_, id_t), target_, by = c("id_t" = "id")) %>%
    dplyr::rename(id = id_t)

  if (.verbose) cat("\nCalculating Source Uniqueness ...")
  uni_s_ <- match_data(tmp_s_, tmp_s_, .cols_match, .method = method_, .join = FALSE)
  uni_s_ <- uni_s_ %>%
    dplyr::select(-c(id_t, group, len_s, len_t)) %>%
    dplyr::group_by(id_s) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ mean(.x, .na.rm = TRUE)))
  colnames(uni_s_) <- c("id_s", cols_f_)

  if (.verbose) cat("\nCalculating Target Uniqueness ...")
  uni_t_ <- match_data(tmp_t_, tmp_t_, .cols_match, .method = method_, .join = FALSE)
  uni_t_ <- uni_t_ %>%
    dplyr::select(-c(id_t, group, len_s, len_t)) %>%
    dplyr::rename(id_t = id_s) %>%
    dplyr::group_by(id_t) %>%
    dplyr::summarise(dplyr::across(dplyr::everything(), ~ mean(.x, .na.rm = TRUE)))
  colnames(uni_t_) <- c("id_t", cols_f_)



  m_ <- m_ %>%
    dplyr::left_join(uni_s_, by = "id_s", suffix = c("_s", "_t")) %>%
    dplyr::left_join(uni_t_, by = "id_t", suffix = c("_s", "_t"))

  for (col in cols_f_) {
    m_[[paste0("uni_", col)]] <- 1 - (m_[[paste0(col, "_s")]] + m_[[paste0(col, "_t")]]) / 2
  }

  dplyr::select(m_, id_s, id_t, dplyr::starts_with("uni_"))

}

#' Match Data
#'
#' @param .powers
#' Powers of 2 to test the Memory (default = 5 => 1,2,4,8,16,32)
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
#' @export
test_memory <- function(.powers = 5, .source, .target, .cols_match, .max_match = 10, .method = "osa",
                        .workers = future::availableCores()) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-test_memory.R")
  # mem_ <- get_avail_mem()

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  rows_in <- mem_alloc <- total_time <- rows_out <- NULL

  purrr::map_dfr(
    .x = purrr::set_names(2^(0:.powers), 2^(0:.powers)),
    .f = ~ bench::mark(
      match_data(
        .source = dplyr::mutate(
          .source[sample(1:nrow(.source), .x, TRUE), ], id = dplyr::row_number()
          ),
        .target = .target,
        .cols_match = .cols_match,
        .max_match = .max_match,
        .method = .method,
        .workers = .workers,
        .verbose = FALSE
      ),
      iterations = 1
    ) %>%
      dplyr::mutate(rows_out = purrr::map_dbl(result, nrow)) %>%
      dplyr::select(total_time, mem_alloc, n_gc, rows_out),
    .id = "rows_in"
  ) %>%
    suppressWarnings() %>%
    dplyr::mutate(rows_in = as.integer(rows_in)) %>%
    dplyr::mutate(
      mem_factor = as.numeric(mem_alloc / dplyr::lag(mem_alloc)),
      time_factor = as.numeric(total_time / dplyr::lag(total_time)),
      rows_factor = as.numeric(rows_out / dplyr::lag(rows_out)),
      cum_time = cumsum(total_time)
    )
}

#' Title
#'
#' @param .matches
#' Dataframe generated by match_data()
#' @param .uniqueness
#' Dataframe generated by uniqueness_data()
#' @param .weights
#' Named numeric vectore (Names must correspond to the similarity columns in .matches)
#'
#' @return A Dataframe
#' @export
score_data <- function(.matches, .uniqueness = NULL, .weights = NULL) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-score_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  score <- id_s <- id_t <- id <- NULL

  if (!is.list(.weights) & !is.null(.weights)) {
    stop(".weights must be a list with names 'sim' and optionally 'uni'", call. = FALSE)
  }

  tab_ <- if (is.null(.uniqueness)) {
    .matches
  } else {
    dplyr::left_join(.matches, .uniqueness, by = c("id_s", "id_t"))
  }


  cols_sim_ <- colnames(tab_)[grepl("sim_", colnames(tab_))]
  cols_uni_ <- colnames(tab_)[grepl("uni_", colnames(tab_))]

  if (length(cols_sim_) > 0 & !"sim" %in% names(.weights)) {
    stop(".weights list must have an element named 'sim'", call. = FALSE)
  }

  if (length(cols_uni_) > 0 & !"uni" %in% names(.weights)) {
    stop(".weights list must have an element named 'uni'", call. = FALSE)
  }

  if (!all(gsub("sim_", "", cols_sim_) %in% names(.weights$sim))) {
    cols_ <- paste(paste0("'",gsub("sim_", "", cols_sim_), "'"), collapse = ", ")
    stop(paste0(".weights$sim must have the following names: ", cols_), call. = FALSE)
  }

  if (!all(gsub("uni_", "", cols_uni_) %in% names(.weights$uni))) {
    cols_ <- paste(paste0("'",gsub("sim_", "", cols_sim_), "'"), collapse = ", ")
    stop(paste0(".weights$sim must have the following names: ", cols_), call. = FALSE)
  }

  if (!is.null(.weights)) {
    weights_ <- unlist(.weights)
    weights_ <- purrr::set_names(
      weights_, stringi::stri_replace_first_fixed(names(weights_), ".", "_")
    )
    weights_ <- weights_ / sum(weights_)
  } else {
    weights_ <- rep(1 / length(c(cols_sim_, cols_uni_)), length(c(cols_sim_, cols_uni_)))
    weights_ <- purrr::set_names(weights_, c(cols_sim_, cols_uni_))
  }

  mat_ <- t(t(as.matrix(tab_[, names(weights_)])) * weights_)
  mat_[is.na(mat_)] <- 0
  score_ <- rowSums(mat_)

  dplyr::mutate(tab_, score = score_)

}

#' Select Data
#'
#' @param .scores
#' Dataframe generated by match_data()
#' @param .source
#' The Source Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .target
#' The Target Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .mult_match
#' Allow for multile matches in the Target Dataframe
#' @param .cols_get
#' Column to join
#'
#'
#' @return A Dataframe
#' @export
select_data <- function(
    .scores, .source, .target, .mult_match = FALSE, .cols_get = character()
    ) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-select_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  score <- id_s <- id_t <- id <- NULL


  if (.mult_match) {
    tab_ <- .scores %>%
      dplyr::arrange(dplyr::desc(score)) %>%
      dplyr::distinct(id_s, .keep_all = TRUE)
  } else {
    miss_ <- .scores
    lst_ <- list()
    iter_ <- 0
    while (nrow(miss_) > 0) {
      iter_ <- iter_ + 1
      lst_[[iter_]] <- miss_ %>%
        dplyr::arrange(dplyr::desc(score)) %>%
        dplyr::distinct(id_t, .keep_all = TRUE) %>%
        dplyr::distinct(id_s, .keep_all = TRUE)

      miss_ <- miss_ %>%
        dplyr::filter(!id_s %in% lst_[[iter_]]$id_s) %>%
        dplyr::filter(!id_t %in% lst_[[iter_]]$id_t)
    }
    tab_ <- dplyr::bind_rows(lst_)
  }

  dplyr::bind_rows(
    tab_, dplyr::filter(dplyr::select(.source, id_s = id), !id_s %in% tab_$id_s)
    ) %>%
    dplyr::left_join(
      y = .source[, c("id", .cols_get)],
      by = c("id_s" = "id"),
      suffix = c("_s", "_t")
    ) %>%
    dplyr::left_join(
      y = .target[, c("id", .cols_get)],
      by = c("id_t" = "id"),
      suffix = c("_s", "_t")
    )

}
