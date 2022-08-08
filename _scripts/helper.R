#' Check for Duplicates
#'
#' @param .tab A dataframe
#' @param ... Any number of columns of the dataframe
#'
#' @return A dataframe
#' @export
filter_dups <- function(.tab, ...) {
  tmp_  <- NULL
  vars_ <- dplyr::enquos(...)
  .tab %>%
    dtplyr::lazy_dt() %>%
    dplyr::mutate(`_tmp_` = paste0(!!!vars_)) %>%
    dplyr::filter(duplicated(`_tmp_`) |duplicated(`_tmp_`, fromLast = TRUE)) %>%
    dplyr::arrange(`_tmp_`) %>%
    dplyr::group_by(`_tmp_`) %>%
    dplyr::mutate(dup_id = dplyr::cur_group_id()) %>%
    dplyr::ungroup() %>%
    dplyr::select(dup_id, dplyr::everything(), -`_tmp_`) %>%
    tibble::as_tibble()
}


#' Check for named vector
#'
#' @param .tab
#' A dataframe (either the source or target dataframe)
#'
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#'
#' @return Nothing or an error
check_names <- function(.tab, .cols) {
  if (!all(.cols %in% colnames(.tab))) {
    col_ <- paste(.cols[!.cols %in% colnames(.tab)], collapse = ", ")
    msg_ <- glue::glue("Columns: {col_} are not present in the dataframe")
    stop(msg_, call. = FALSE)
  }

  names_ <- names(.cols)
  if (!all(tolower(names_) %in% c("fuzzy", "exact", "e", "f"))) {
    msg_ <- paste0(
      "\nArgument '.cols' must have names.",
      "\nExact Matching: 'exact' or 'e'",
      "\nFuzzy Matching: 'fuzzy' or 'd'",
      "\nNames are case insenstive, both full name or first letter are possibe"
    )
    stop(msg_, call. = FALSE)
  }
}


#' Prepare Table
#'
#' Description
#'
#' @param .tab
#' A dataframe (either the source or target dataframe)
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#' @param .fstd
#' Standardization Function
#' @param .dir
#' Directory to store Tables
#' @param .type
#' Either s (source) or t (target)
#' @return A dataframe
#'
#' @examples
#'
#' library(rMatching)
#'
#' source_ <- prep_table
prep_table <- function(.tab, .cols, .fstd = standardize_str, .dir, .type = c("s", "t")) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  source("_debug/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

  # Check if columns are named -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  check_names(.tab, .cols)

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  type_ <- match.arg(.type, c("s", "t"))

  # Check for Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  dup_ <- filter_dups(.tab, !!!dplyr::syms(.cols))
  if (nrow(dup_) > 0) {
    name_tab_ <- deparse(substitute(.tab))
    name_col_ <- deparse(substitute(.cols))
    name_col_ <- gsub("(f|e)\\s?\\=\\s?", "", name_col_)
    msg_ <- glue::glue(
      "Input data contains duplicates. Please run the following code to check for duplicates:

      dups <- help_filter_dups(
        .tab = {name_tab_},
        !!!dplyr::syms({name_col_})
      )"
    )
    stop(msg_, call. = FALSE)
  }

  # Assign new names to columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  nm_ <- tibble::tibble(name = names(.cols), col_old = .cols) %>%
    dplyr::group_by(name) %>%
    dplyr::mutate(col_new = paste0(name, dplyr::row_number())) %>%
    dplyr::ungroup()
  cn_ <- nm_$col_new

  # Prepare table (1) -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  tab_ <- tibble::as_tibble(.tab[, .cols]) %>%
    `colnames<-`(cn_) %>%
    dplyr::mutate(
      hash = purrr::map_chr(paste0(!!!dplyr::syms(cn_)), fastdigest::fastdigest)
    ) %>%
    dplyr::select(hash, dplyr::everything())

  # Standardize columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if (!is.null(.fstd)) {
    tab_ <- dplyr::mutate(tab_, dplyr::across(c(!!!dplyr::syms(cn_)), .fstd))
  }

  # Get Duplicates after Standardization -- -- -- -- -- -- -- -- -- -- -- -- ---
  dup_ <- filter_dups(tab_, !!!dplyr::syms(cn_)) %>%
    dplyr::group_by(dup_id) %>%
    dplyr::mutate(hash_use = first(hash)) %>%
    dplyr::ungroup() %>%
    dplyr::select(hash_use, hash_dup = hash) %>%
    dplyr::filter(!hash_use == hash_dup)

  # Prepare table (2) -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  out_ <- tab_ %>%
    dplyr::filter(!hash %in% dup_$hash_dup) %>%
    tidyr::pivot_longer(dplyr::starts_with("f"), names_to = "col", values_to = "val") %>%
    dplyr::mutate(nc = nchar(val), .before = val) %>%
    dplyr::select(hash, col, dplyr::everything()) %>%
    dplyr::filter(!is.na(val))

  # Make Groups -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  groups_ <- out_ %>%
    dplyr::count(col, !!!dplyr::syms(cn_[startsWith(cn_, "e")]), nc) %>%
    dplyr::arrange(col, !!!dplyr::syms(cn_[startsWith(cn_, "e")]), nc)

  # Save Output -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  fst::write_fst(out_, file.path(.dir, paste0(type_, "data.fst")), 100)
  fst::write_fst(groups_, file.path(.dir, paste0(type_, "group.fst")), 100)
  fst::write_fst(dup_, file.path(.dir, paste0(type_, "dups.fst")), 100)
  fst::write_fst(nm_, file.path(.dir, paste0(type_, "names.fst")), 100)
}

#' Title
#'
#' @param .dir
#' @param .range
#' @param .max
#'
#' @return
#' @export
#'
#' @examples
make_groups <- function(.dir, .range = Inf) {
  source("_debug/debug-make_groups.R")

  sgroups <- fst::read_fst(file.path(.dir, "sgroup.fst"))
  tgroups <- fst::read_fst(file.path(.dir, "tgroup.fst"))
  cols_ <- colnames(sgroups)[!colnames(sgroups) %in% c("nc", "n")]

  groups_ <- sgroups %>%
    # Join with Target Dataframe
    dplyr::inner_join(tgroups, by = c(cols_), suffix = c("s", "t")) %>%
    # Filter Nchars according to range
    dplyr::filter(nct >= ncs - .range & nct <= ncs + .range) %>%
    dplyr::arrange(!!!dplyr::syms(cols_), ncs, nct) %>%
    # Get Initial Start and Stop Nchars
    dplyr::group_by(!!!dplyr::syms(cols_), ncs) %>%
    dplyr::summarise(
      ncs1 = ncs[1], ncs2 = ncs[1],
      nct1 = min(nct), nct2 = max(nct),
      ns = ns[1], nt = sum(nt),
      size = ns * nt,
      .groups = "drop_last"
    ) %>%
    dplyr::select(-ncs) %>%
    dplyr::arrange(size, .by_group = TRUE)

  fst::write_fst(groups_, file.path(.dir, "_groups.fst"), 100)
}

#' Title
#'
#' @param .source
#' @param .target
#' @param .cols
#' @param .fstd
#' @param .dir
#'
#' @return
#' @export
#'
#' @examples
prep_tables <- function(.source, .target, .cols, .fstd = standardize_str, .dir, .range = Inf) {
  source("_debug/debug-prep_tables.R")

  if (dir.exists(.dir)) stop("Please choose an non-existing directory", call. = FALSE)


  prep_table(.tab = .source, .cols = .cols, .fstd = .fstd, .dir = .dir, .type = "s")
  prep_table(.tab = .target, .cols = .cols, .fstd = .fstd, .dir = .dir, .type = "t")

  make_groups(.dir = .dir, .range = .range)
}




#' Title
#'
#' @param .dir
#' @param .row
#'
#' @return
#' @export
#'
#' @examples
filter_groups <- function(.dir, .row) {
  # .groups <- fst::read_fst("_debug/debug-prep_tables/_groups.fst")
  # .row <- .groups[100, ]
  sdata <- tidyft::parse_fst(file.path(.dir, "sdata.fst"))
  tdata <- tidyft::parse_fst(file.path(.dir, "tdata.fst"))
  ce_ <- colnames(sdata)[startsWith(colnames(sdata), "e")]

  tab_ <- tibble::as_tibble(sdata)
  col_  <- .row$col
  ncs1_ <- .row$ncs1
  ncs2_ <- .row$ncs2
  nct1_ <- .row$nct1
  nct2_ <- .row$nct2

  if (length(ce_) > 0) {
    es_ <- paste(purrr::map_chr(ce_, ~ paste0(.x, " == ", "'", .row[[.x]], "'")), collapse = " & ")

    strs_ <- as.character(glue::glue('col == "{col_}" & {es_} & nc >= {ncs1_} & nc <= {ncs2_}'))
    strt_ <- as.character(glue::glue('col == "{col_}" & {es_} & nc >= {nct1_} & nc <= {nct2_}'))
  } else {
    strs_ <- as.character(glue::glue('col == "{col_}" & nc >= {ncs1_} & nc <= {ncs2_}'))
    strt_ <- as.character(glue::glue('col == "{col_}" & nc >= {nct1_} & nc <= {nct2_}'))
  }

  f <- function(ft, dot_string) {
    ft_names <- names(ft)
    old <- ft_names[stringr::str_detect(dot_string, ft_names)]
    new <- paste0("ft$", old)
    for (i in seq_along(old)) dot_string <- gsub(old[i], new[i], dot_string)
    eval(parse(text = stringr::str_glue("ft[{dot_string},] %>% tidyft::as.data.table()")))
  }

  stab <- f(sdata, strs_)
  ttab <- f(tdata, strt_)

  return(list(s = stab, t = ttab))


}


#' Title
#'
#' @param .lst
#' @param .max_match
#' @param .method
#' @param .workers
#' @param .join
#'
#' @return
#' @export
#'
#' @examples
match_group <- function(
    .lst, .max_match = 10,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .workers = floor(future::availableCores() / 4), .join = TRUE
) {
  # source("test-debug/debug-match_col.R")


  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  s_ <- dplyr::select(dtplyr::lazy_dt(.lst$s), hash, val)
  t_ <- dplyr::select(dtplyr::lazy_dt(.lst$t), hash, val)

  if (.join) {
    exact_ <- s_ %>%
      dplyr::inner_join(t_, by = "val", suffix = c("_s", "_t")) %>%
      dplyr::select(hash_s, hash_t) %>%
      dplyr::mutate(sim = 1) %>%
      tibble::as_tibble()

    s_ <- tibble::as_tibble(dplyr::filter(s_, !hash %in% exact_$hash_s))
    t_ <- tibble::as_tibble(t_)
    if (nrow(s_) == 0 | nrow(t_) == 0) return(exact_)
  } else {
    s_ <- tibble::as_tibble(s_)
    t_ <- tibble::as_tibble(t_)
    exact_ <- tibble::tibble()
  }

  fuzzy_ <- stringdist::stringsimmatrix(
    a = s_[["val"]],
    b = t_[["val"]],
    method = .method,
    nthread = .workers
  ) %>%
    reshape2::melt() %>%
    dplyr::rename(hash_s = Var1, hash_t = Var2, sim = value) %>%
    dtplyr::lazy_dt() %>%
    dplyr::filter(sim > 0) %>%
    dplyr::group_by(hash_s) %>%
    dplyr::arrange(-sim, .by_group = TRUE) %>%
    dplyr::filter(sim >= dplyr::nth(sim, .max_match)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(hash_s, dplyr::desc(sim)) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      hash_s = s_[["hash"]][hash_s],
      hash_t = t_[["hash"]][hash_t]
    )

  dplyr::bind_rows(exact_, fuzzy_)
}


match_data <- function(
    .dir, .max_match = 10,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .workers = floor(future::availableCores() / 4), .join = TRUE, .verbose = TRUE
) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  source("_debug/debug-match_data.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  tab_groups_ <- dplyr::mutate(fst::read.fst(file.path(.dir, "_groups.fst")), group = dplyr::row_number())
  lst_groups_ <- split(tab_groups_, seq_len(nrow(tab_groups_)))

  if (.verbose) cat("\nStart Matching ...")
  pb <- if (.verbose) progress::progress_bar$new(total = length(lst_groups_))

  if (.workers == 1) {
    tmp_match_ <- purrr::map_dfr(
      .x = lst_groups_,
      .f = ~ {
        if (.verbose) pb$tick()
        match_group(filter_groups(.dir, .x), .max_match, method_, .workers, .join)
      },
      .id = "group"
    )
  } else {
    future::plan("multisession", workers = .workers)
    tmp_match_ <- furrr::future_map_dfr(
      .x = lst_groups_,
      .f = ~ match_group(filter_groups(.dir, .x), .max_match, method_, .workers, .join),
      .options = furrr::furrr_options(seed = TRUE),
      .progress = .verbose,
      .id = "group"
    )
    future::plan("default")
    on.exit(future::plan("default"))
  }

  sdata <- fst::read_fst(file.path(.dir, "sdata.fst"))
  tdata <- fst::read_fst(file.path(.dir, "tdata.fst"))


  sim_ <- tmp_match_ %>%
    dtplyr::lazy_dt() %>%
    dplyr::mutate(group = as.integer(group)) %>%
    dplyr::left_join(dplyr::select(tab_groups_, group, col, dplyr::starts_with("e")), by = "group") %>%
    dplyr::select(-group) %>%
    tidyr::pivot_wider(names_from = col, values_from = sim) %>%
    tidyr::pivot_longer(cols = dplyr::starts_with("f"), names_to = "col", values_to = "sim") %>%
    dplyr::left_join(
      y = dplyr::select(sdata, hash_s = hash, col, vals = val),
      by = c("hash_s", "col")
    ) %>%
    dplyr::left_join(
      y = dplyr::select(tdata, hash_t = hash, col, valt = val),
      by = c("hash_t", "col")
    ) %>%
    dplyr::mutate(
      sim = dplyr::if_else(
        condition = is.na(sim) & !is.na(vals) & !is.na(valt),
        true = stringdist::stringsim(vals, valt, method_, nthread = .workers),
        false = sim
      )
    ) %>%
    dplyr::select(-vals, -valt)

  uni_ <- sim_ %>%
    dplyr::arrange(hash_s, col) %>%
    dplyr::group_by(hash_s, col) %>%
    dplyr::summarise(uni = mean(sim, na.rm = TRUE), .groups = "drop_last") %>%
    dplyr::mutate(uni = uni / sum(uni)) %>%
    dplyr::ungroup()

  match_ <- dplyr::left_join(sim_, uni_, by = c("hash_s", "col")) %>%
    dplyr::mutate(score = sim * uni) %>%
    tidyr::pivot_wider(names_from = col, values_from = c(sim, uni, score)) %>%
    tibble::as_tibble()


  fst::write_fst(match_, file.path(.dir, "_matches.fst"))
  return(match_)
}

.weights <- c(name = .7, city = .2, address = .1)
score_data <- function(.dir, .weights = NULL, .max_match = 10) {

  weight_ <- tibble::tibble(col_old = names(.weights), weight = .weights)
  names_ <- fst::read_fst(file.path(.dir, "snames.fst")) %>%
    dplyr::filter(name == "f") %>%
    dplyr::left_join(weight_, by = "col_old") %>%
    dplyr::mutate(weight = weight / sum(weight)) %>%
    dplyr::arrange(col_new)

  matches_ <- fst::read_fst(file.path(.dir, "_matches.fst"))
  int_ <- which(startsWith(colnames(matches_), "score"))
  for (i in seq_len(length(int_))) {
    matches_[, int_[i]] <- matches_[, int_[i]] * names_$weight[i]
  }

  scores_ <- dplyr::mutate(matches_, score = rowSums(matches_[, int_], na.rm = TRUE)) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(hash_s) %>%
    dplyr::arrange(dplyr::desc(score), .by_group = TRUE) %>%
    dplyr::mutate(rank = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(rank <= .max_match) %>%
    tibble::as_tibble()

  fst::write_fst(scores_, file.path(.dir, "_scores.fst"), 100)

}

select_data <- function(.dir, .rank = 1) {
  scores_ <- fst::read_fst(file.path(.dir, "_scores.fst"))
  sdata_  <- fst::read_fst(file.path(.dir, "sdata.fst")) %>%
    dplyr::select(hash_s = hash, col, val) %>%
    dplyr::mutate(col = paste0(col, "_s")) %>%
    tidyr::pivot_wider(names_from = col, values_from = val)
  tdata_  <- fst::read_fst(file.path(.dir, "tdata.fst")) %>%
    dplyr::select(hash_t = hash, col, val) %>%
    dplyr::mutate(col = paste0(col, "_t")) %>%
    tidyr::pivot_wider(names_from = col, values_from = val)

  names_  <- fst::read_fst(file.path(.dir, "snames.fst"))

  a <- scores_ %>%
    dplyr::filter(rank <= .rank) %>%
    tibble::as_tibble() %>%
    dplyr::select(hash_s, hash_t, score, dplyr::starts_with("e")) %>%
    dplyr::left_join(sdata_, by = "hash_s") %>%
    dplyr::left_join(tdata_, by = "hash_t") %>%
    `colnames<-`(stringi::stri_replace_all_fixed(colnames(.), names_$col_new, names_$col_old, FALSE))


}
