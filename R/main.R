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


#' Standardize Data
#'
#' Description
#'
#' @param .tab A dataframe (either the source or target dataframe)
#' @param .cols_match A character vector of columns to perform fuzzy matching.
#' @param .fun Function for standardization, if NULL standardize_str() is used
#'
#' @return A dataframe
#'
#' @export
standardize_data <- function(.tab, .cols_match, .fun = NULL) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-standardized_data.R")

  # Convert to Tibble -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  tab_ <- tibble::as_tibble(.tab)

  # Get Standardization Function -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  f_ <- if (is.null(.fun)) standardize_str else .fun


  # Standardize Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  for (i in .cols_match) tab_[[i]] <- f_(tab_[[i]])

  # Return -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  return(tab_)
}

#' Extract Legal Forms
#'
#' Description
#'
#' @param .tab A dataframe (either the source or target dataframe)
#' @param .col_name The column with firm names
#' @param .col_country Optionally, a column with iso3 country codes
#' @param .legal_forms A dataframe with legal forms
#' @param .workers Number of cores to utilize (Default all cores determined by future::availableCores())
#'
#' @return A dataframe
#'
#' @export
extract_legal_form <- function(
    .tab, .col_name, .col_country = NULL, .legal_forms = data.frame(),
    .workers = future::availableCores()
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-extract_legal_form.R")

  # Shortcut Functions -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  symp <- function(...) dplyr::sym(paste0(...))
  rlf <- stringi::stri_replace_last_fixed
  `:=` <- rlang::`:=`

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tmp <- legal_form_orig <- legal_form_stand <- legal_form <- name <- NULL
  lf_stand <- lf_orig <- NULL

  # Convert to Tibble and Standardize -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  .tab <- tibble::as_tibble(.tab)
  tab_ <- standardize_data(.tab, .col_name)

  # Get Legal Form Table -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  tab_lf_ <- if (nrow(.legal_forms) == 0) get("legal_form_all") else .legal_forms

  if (is.null(.col_country)) {
    tab_lf_ <- tab_lf_ %>%
      dplyr::distinct(legal_form_orig, legal_form_stand) %>%
      dplyr::distinct(legal_form_orig, .keep_all = TRUE)
    join_by_ <- "legal_form_orig"

  } else {
    colnames(tab_lf_) <- c(.col_country, "legal_form_orig", "legal_form_stand")
    join_by_ <- c(.col_country, "legal_form_orig")
  }


  # Extract Legal Forms -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  lf_ <- unique(tab_lf_[["legal_form_orig"]])
  nm_ <- tab_[[.col_name]]

  f_ <- carrier::crate(function(.lf, .nm) which(endsWith(.nm, paste0(" ", .lf))))
  future::plan("multisession", workers = .workers)
  lst_lf_ext_ <- furrr::future_map(
    .x = purrr::set_names(lf_, lf_),
    .f = ~ f_(.x, nm_),
    .options = furrr::furrr_options(seed = TRUE)
  )
  future::plan("default")


  # Reshape List to Dataframe -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tab_lf_ext_ <- lst_lf_ext_ %>%
    purrr::compact() %>%
    tibble::enframe(name = "legal_form_orig", value = "tmp") %>%
    tidyr::unnest(tmp) %>%
    dplyr::arrange(dplyr::desc(nchar(legal_form_orig))) %>%
    dplyr::distinct(tmp, .keep_all = TRUE)

  # Get Output -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tab_ %>%
    dplyr::mutate(tmp = dplyr::row_number()) %>%
    dplyr::left_join(tab_lf_ext_, by = "tmp") %>%
    dplyr::left_join(tab_lf_, by = join_by_) %>%
    dplyr::rename(
      lf_stand = legal_form_stand,
      lf_orig = legal_form_orig
    ) %>%
    dplyr::relocate(lf_stand, .after = !!symp(.col_name)) %>%
    dplyr::relocate(lf_orig, .after = !!symp(.col_name)) %>%
    dplyr::mutate(
      !!symp(.col_name, "_adj") := trimws(rlf(!!symp(.col_name), lf_orig, "")),
      .after = !!symp(.col_name)) %>%
    dplyr::mutate(
      !!symp(.col_name, "_adj") := dplyr::if_else(
        is.na(!!symp(.col_name, "_adj")), !!dplyr::sym(.col_name), !!symp(.col_name, "_adj")
      )) %>%
    dplyr::mutate(
      !!symp(.col_name, "_std") := dplyr::if_else(
        !is.na(lf_stand), paste(!!symp(.col_name, "_adj"), lf_stand), !!symp(.col_name, "_adj")
      ), .after = !!symp(.col_name, "_adj")) %>%
    dplyr::select(-tmp) %>%
    dplyr::mutate(!!dplyr::sym(.col_name) := .tab[[.col_name]])
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
  if (!"_id_" %in% colnames(.tab)) {
    tidyr::unite(
      data = tibble::as_tibble(.tab[, c("id", .cols_match)]),
      col = tmp,
      !dplyr::matches("^id$")
    ) %>%
      dplyr::group_by(tmp) %>%
      dplyr::summarise(
        `_id_` = list(id),
        id = dplyr::first(id),
        .groups = "drop"
      ) %>%
      dplyr::select(id, `_id_`) %>%
      dplyr::left_join(.tab, by = "id")
  } else {
    return(.tab)
  }
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
#' Number of cores to utilize (Default all cores determined by future::availableCores())
#' @param .split
#' Maximum Number of Items to process in the Source Dataframe
#' @param .verbose
#' Print Additional Information?
#'
#' @return A Dataframe
#' @export
match_data <- function(
    .source, .target, .cols_match, .max_match = 10, .method = "osa",
    .workers = future::availableCores(), .split = Inf, .verbose = TRUE
) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-match_data.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Check if IDs are valid -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  .check <- check_id(.source, .target)

  # Prepare Tables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  source_ <- prep_tables(.source, .cols_match)
  target_ <- prep_tables(.target, .cols_match)

  # Split Inputs -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  cols_e_ <- .cols_match[names(.cols_match) == "e"]
  ls_ <- split_input(source_, cols_e_, .split, "source")
  lt_ <- split_input(target_, cols_e_, .split, "target")
  ls_ <- dplyr::filter(ls_, split %in% lt_$split)
  lt_ <- dplyr::filter(lt_, split %in% ls_$split)


  # Get Progress Bar -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  pb <- if (.verbose) progress::progress_bar$new(total = nrow(ls_))

  # Match Data -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  cols_a_ <- .cols_match[names(.cols_match) != "e"]
  purrr::map2_dfr(
    .x = ls_$value,
    .y = ls_$split,
    .f = ~ {
      if (.verbose) pb$tick()
      t_ <- dplyr::filter(lt_, split == .y)$value[[1]]
      match_cols(.x, t_, cols_a_, .max_match, .method, .workers)
    }
  )


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
#'
#' @return A dataframe
#'
#' @export
uniqueness_data <- function(.matches, .source, .target, .cols_match) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-score_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  id_s <- id_t <- sim <- uni <- NULL


  # Prepare Data -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  check_ <- check_id(.source, .target)
  source_  <- prep_tables(.source, .cols_match)
  target_  <- prep_tables(.target, .cols_match)
  m_ <- tibble::as_tibble(.matches)

  # Exact/Match Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  cols_e_ <- .cols_match[names(.cols_match) == "e"]
  cols_a_ <- .cols_match[names(.cols_match) != "e"]
  cols_a_ <- purrr::set_names(cols_a_, cols_a_)

  # Calculate Uniqueness -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  cmeandist <- function(.vec) colMeans(stringdist::stringsimmatrix(.vec), na.rm = TRUE)
  tmp_s_ <- dplyr::left_join(dplyr::distinct(m_, id_s), source_, by = c("id_s" = "id"))
  tmp_t_ <- dplyr::left_join(dplyr::distinct(m_, id_t), target_, by = c("id_t" = "id"))
  uni_s_ <- purrr::map_dfc(cols_a_, ~ cmeandist(tmp_s_[[.x]]))
  uni_t_ <- purrr::map_dfc(cols_a_, ~ cmeandist(tmp_t_[[.x]]))
  tmp_s_ <- dplyr::bind_cols(tmp_s_[, "id_s"], uni_s_)
  tmp_t_ <- dplyr::bind_cols(tmp_t_[, "id_t"], uni_t_)

  m_ <- tibble::as_tibble(.matches)
  m_ <- m_ %>%
    dplyr::left_join(tmp_s_, by = "id_s", suffix = c("_s", "_t")) %>%
    dplyr::left_join(tmp_t_, by = "id_t", suffix = c("_s", "_t"))

  for (col in cols_a_) {
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


#' Select Data
#'
#' @param .matches
#' Dataframe generated by match_data()
#' @param .uniqueness
#' Dataframe generated by uniqueness_data()
#' @param .source
#' The Source Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .target
#' The Target Dataframe.\cr
#' (Must contain a unique column id and the columns you want to match on)
#' @param .weights
#' Named numeric vectore (Names must correspond to the similarity columns in .matches)
#'
#' @return A Dataframe
#' @export
select_data <- function(.matches, .uniqueness = NULL, .source, .target, .weights = NULL) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-select_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  score <- id_s <- id_t <- id <- NULL

  tab_ <- if (is.null(.uniqueness)) {
    .matches
  } else {
    dplyr::left_join(.matches, .uniqueness, by = c("id_s", "id_t"))
  }

  cols_ <- colnames(tab_)
  cols_ <- cols_[grepl("sim_|uni_", cols_)]


  miss_ <- tab_

  lst_ <- list()
  iter_ <- 0
  while (nrow(miss_) > 0) {
    iter_ <- iter_ + 1
    mat_ <- as.matrix(miss_[, cols_])

    w_ <- if (is.null(.weights)) {
      purrr::set_names(rep(1 / length(cols_)), length(cols_))
    } else {
      .weights[order(match(names(.weights), cols_))] / sum(.weights)
    }

    w_ <- if (!is.null(.uniqueness)) rep(w_, 2) / 2 else w_

    out_ <- dplyr::mutate(miss_, score = colSums(t(mat_) * w_, na.rm = TRUE)) %>%
      dplyr::arrange(dplyr::desc(score)) %>%
      dplyr::distinct(id_t, .keep_all = TRUE) %>%
      dplyr::distinct(id_s, .keep_all = TRUE)

    miss_ <- miss_ %>%
      dplyr::filter(!id_s %in% out_$id_s) %>%
      dplyr::filter(!id_t %in% out_$id_t)

    lst_[[iter_]] <- out_
  }

  out_  <- dplyr::bind_rows(lst_)
  miss_ <- .source %>%
    dplyr::select(id_s = id) %>%
    dplyr::filter(!id_s %in% out_$id_s)

  dplyr::arrange(dplyr::bind_rows(out_, miss_), dplyr::desc(score))

}


#
# %>%
#   dplyr::filter(id_s %in% table_matches$id_s) %>%
#   dplyr::left_join((table_matches %>% dplyr::select(id_s, id_t) %>% dplyr::mutate(match = 1)))
#
# sum(a$match, na.rm = TRUE) / nrow(a)
#
# match_ <-
#
#
# tab_ <- dplyr::left_join(tab_, match_, by = c("id_s", "id_t")) %>%
#   tidyr::replace_na(list(match = 0)) %>%
#   dplyr::filter(id_s %in% table_matches$id_s)
#
#
# sum(out1_$match) / nrow(out1_)
# sum(out2_$match) / nrow(out2_)
#
# fml_ <- as.formula(paste0("match ~ ", paste(cols_, collapse = " + ")))
# fit_ <- glm(fml_, family = binomial(), dplyr::slice_sample(tab_, prop = .1))
#
# out1_ <- dplyr::mutate(tab_, score = predict(fit_, newdata = tab_, type = "response")) %>%
#   dplyr::arrange(dplyr::desc(score)) %>%
#   dplyr::left_join(.source[, c("id", "name")], by = c("id_s" = "id")) %>%
#   dplyr::left_join(.target[, c("id", "name")], by = c("id_t" = "id")) %>%
#   dplyr::distinct(id_t, .keep_all = TRUE) %>%
#   dplyr::distinct(id_s, .keep_all = TRUE)
#
# sum(out1_$match) / nrow(out1_)
