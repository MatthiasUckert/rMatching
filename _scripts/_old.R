
#' Score Data
#'
#' @param .dir
#' Directory to store Tables
#' @param .weights
#' A named vector with weight, if null all columns will be equal weighted
#' @param .max_match
#' Maximum number of matches
#' @param .method
#' c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param .return
#' Return a saved file(s)?
#' @param .verbose
#' Print additional Information? (Default: TRUE)
#'
#'
#' @import data.table
#'
#' @return A dataframe (saved in .dir)
#' @export
score_data <- function(
    .dir,
    .weights = NULL,
    .max_match = 10,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .return = FALSE,
    .verbose = TRUE
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug_vars/debug-score_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  name <- weight <- col_new <- hash_s <- score <- score_f1 <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Check if Matching already exists -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  path_scores_ <- file.path(.dir, method_, "_scores.fst")
  if (file.exists(path_scores_)) {
    msg_verbose("Scoring exists already, it won't be recalculated", .verbose)
    if (.return) {
      return(fst::read_fst(path_scores_))
    } else {
      return(NULL)
    }
  }

  # Get weights -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  names_ <- fst::read_fst(file.path(.dir, "tables", "snames.fst")) %>%
    dplyr::filter(name == "f")
  if (!is.null(.weights)) {
    weight_ <- tibble::tibble(col_old = names(.weights), weight = .weights)
    weight_ <- names_ %>%
      dplyr::left_join(weight_, by = "col_old") %>%
      dplyr::mutate(weight = weight / sum(weight)) %>%
      dplyr::arrange(col_new)
  } else {
    weight_ <- names_ %>%
      dplyr::mutate(weight = 1 / dplyr::n()) %>%
      dplyr::arrange(col_new)
  }

  # Read Matches -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  matches_ <- fst::read_fst(file.path(.dir, method_, "_matches.fst"))


  if (length(.weights) == 1) {
    scores_ <- matches_ %>%
      dplyr::mutate(
        score1 = score_f1,
        score2 = sim_f1
      )
  } else {

    int_sco_ <- which(startsWith(colnames(matches_), "score"))
    int_sim_ <- which(startsWith(colnames(matches_), "sim_"))
    for (i in seq_len(length(int_sco_))) {
      matches_[, int_sco_[i]] <- matches_[, int_sco_[i]] * weight_$weight[i]
    }

    scores_ <- matches_ %>%
      dplyr::mutate(
        score1 = rowSums(matches_[, int_sco_], na.rm = TRUE),
        score2 = rowMeans(matches_[, int_sim_], na.rm = TRUE)
      )
  }



  scores_ <- scores_ %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(hash_s) %>%
    dplyr::arrange(dplyr::desc(score1), .by_group = TRUE) %>%
    dplyr::mutate(rank1 = dplyr::row_number()) %>%
    dplyr::arrange(dplyr::desc(score2), .by_group = TRUE) %>%
    dplyr::mutate(rank2 = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(rank1 <= .max_match | rank2 <= .max_match) %>%
    tibble::as_tibble()

  fst::write_fst(scores_, path_scores_, 100)

  if (.return) {
    return(scores_)
  } else {
    msg_verbose("Data is stored ...", .verbose)
  }
}

#' Select Data
#'
#' @param .dir Directory to store Tables
#' @param .rank Up to which rank should the data be retrieved?
#' @param .method c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param score 1 for scoring including uniqueness, 2 for scoreing only on similarity
#'
#' @return A dataframe
#' @export
select_data <- function(
    .dir,
    .rank = 1,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .score = c(1, 2)
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug_vars/debug-select_data.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  hash <- val <- hash_s <- hash_t <- score <- . <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  scores_ <- fst::read_fst(file.path(.dir, method_, "_scores.fst"))
  sdata_ <- fst::read_fst(file.path(.dir, "tables", "sorig.fst")) %>%
    dplyr::select(hash_s = hash, dplyr::everything())

  tdata_ <- fst::read_fst(file.path(.dir, "tables", "torig.fst")) %>%
    dplyr::select(hash_t = hash, dplyr::everything())

  names_ <- fst::read_fst(file.path(.dir, "tables", "snames.fst"))


  if (.score == 1) {
    tmp_ <- dplyr::filter(scores_, rank1 <= .rank)
  } else {
    tmp_ <- dplyr::filter(scores_, rank2 <= .rank)
  }

  tmp_ %>%
    tibble::as_tibble() %>%
    dplyr::select(hash_s, hash_t, score1, score2, dplyr::starts_with("e")) %>%
    dplyr::left_join(sdata_, by = "hash_s", suffix = c("_s", "_t")) %>%
    dplyr::left_join(tdata_, by = "hash_t", suffix = c("_s", "_t")) %>%
    `colnames<-`(stringi::stri_replace_all_regex(colnames(.), paste0("^", names_$col_new, "$"), names_$col_old, FALSE))
}


#' Helper Function: Filter data from fst files
#'
#' @param .dir Directory to store Tables
#' @param .group One row corrsponds to a group
#'
#' @return Filtered Dataframe
filter_groups <- function(.dir, .group) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

  sdata <- tidyft::parse_fst(file.path(.dir, "sdata.fst"))
  tdata <- tidyft::parse_fst(file.path(.dir, "tdata.fst"))
  ce_ <- colnames(sdata)[startsWith(colnames(sdata), "e")]

  tab_ <- tibble::as_tibble(sdata)
  col_ <- .group$col
  ncs1_ <- .group$ncs1
  ncs2_ <- .group$ncs2
  nct1_ <- .group$nct1
  nct2_ <- .group$nct2

  if (length(ce_) > 0) {
    es_ <- paste(purrr::map_chr(ce_, ~ paste0(.x, " == ", "'", .group[[.x]], "'")), collapse = " & ")

    strs_ <- as.character(glue::glue('col == "{col_}" & {es_} & nc >= {ncs1_} & nc <= {ncs2_}'))
    strt_ <- as.character(glue::glue('col == "{col_}" & {es_} & nc >= {nct1_} & nc <= {nct2_}'))
  } else {
    strs_ <- as.character(glue::glue('col == "{col_}" & nc >= {ncs1_} & nc <= {ncs2_}'))
    strt_ <- as.character(glue::glue('col == "{col_}" & nc >= {nct1_} & nc <= {nct2_}'))
  }

  stab <- filter_fst_adj(sdata, strs_)
  ttab <- filter_fst_adj(tdata, strt_)

  return(list(s = stab, t = ttab))
}

#' Helper Function: Match a single Group
#'
#' @param .lst A list produced by filter_groups()
#' @param .max_match MAximum number of matches
#' @param .method c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param .workers workers to use
#'
#' @import data.table
#'
#' @return A dataframe
match_group <- function(.lst, .max_match = 10,
                        .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
                        .workers = floor(future::availableCores() / 4)) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  hash <- val <- hash_s <- hash_t <- Var1 <- Var2 <- value <- sim <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  s_ <- dplyr::select(dtplyr::lazy_dt(.lst$s), hash, val)
  t_ <- dplyr::select(dtplyr::lazy_dt(.lst$t), hash, val)

  exact_ <- s_ %>%
    dplyr::inner_join(t_, by = "val", suffix = c("_s", "_t")) %>%
    dplyr::select(hash_s, hash_t) %>%
    dplyr::mutate(sim = 1) %>%
    tibble::as_tibble()

  s_ <- dplyr::filter(tibble::as_tibble(s_), !hash %in% exact_$hash_s)
  t_ <- tibble::as_tibble(t_)

  if (nrow(s_) == 0 | nrow(t_) == 0) {
    return(exact_)
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

#' Match Data
#'
#' @param .dir Directory to store Tables
#' @param .max_match MAximum number of matches
#' @param .method c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param .workers Workers to use
#' @param .return
#' Return a saved file(s)?
#' @param .verbose
#' Print additional Information? (Default: TRUE)
#'
#' @import data.table
#'
#' @return A dataframe (saved in .dir)
#' @export
match_data <- function(
    .dir, .max_match = 10,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .workers = floor(future::availableCores() / 4),
    .return = FALSE,
    .verbose = TRUE
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug_vars/debug-match_data.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  # Check if Matching already exists -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  path_matches_ <- file.path(.dir, method_, "_matches.fst")
  dir.create(dirname(path_matches_), FALSE, TRUE)

  if (file.exists(path_matches_)) {
    msg_verbose("Matching exists already, it won't be recalculated", .verbose)
    if (.return) {
      return(fst::read_fst(path_scores_))
    } else {
      return(NULL)
    }
  }

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  group <- sim <- hash <- val <- vals <- valt <- hash_s <- hash_t <- uni <- score <- NULL

  # Reading Groups -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  fil_groups_ <- file.path(.dir, "tables", "_groups.fst")
  tab_groups_ <- dplyr::mutate(fst::read.fst(fil_groups_), group = dplyr::row_number())
  lst_groups_ <- split(tab_groups_, seq_len(nrow(tab_groups_)))

  # Start Matching -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  msg_verbose("Matching source table to target table ...", .verbose)
  pb <- if (.verbose) progress::progress_bar$new(total = length(lst_groups_))

  if (.workers == 1) {
    tmp_match_ <- purrr::map_dfr(
      .x = lst_groups_,
      .f = ~ {
        if (.verbose) pb$tick()
        match_group(filter_groups(.dir, .x), .max_match, method_, .workers)
      },
      .id = "group"
    )
  } else {
    future::plan("multisession", workers = .workers)
    tmp_match_ <- furrr::future_map_dfr(
      .x = lst_groups_,
      .f = ~ match_group(
        .lst = filter_groups(.dir, .x),
        .max_match = .max_match,
        .method = method_,
        .workers = .workers
      ),
      .options = furrr::furrr_options(
        seed = TRUE,
        globals = c(
          "method_", ".max_match", ".workers", "match_group", "filter_groups",
          ".dir", "filter_fst_adj"
        )
      ),
      .progress = .verbose,
      .id = "group"
    )
    future::plan("default")
    on.exit(future::plan("default"))
  }

  # Read Transormed Data -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  sdata <- fst::read_fst(file.path(file.path(.dir, "tables"), "sdata.fst"))
  tdata <- fst::read_fst(file.path(file.path(.dir, "tables"), "tdata.fst"))


  msg_verbose("Adjusting similarity scores", .verbose)
  sim_ <- tmp_match_ %>%
    dtplyr::lazy_dt() %>%
    dplyr::filter(!is.na(hash_s), !is.na(hash_t)) %>%
    dplyr::mutate(group = as.integer(group)) %>%
    dplyr::left_join(dplyr::select(tab_groups_, group, col, dplyr::starts_with("e")), by = "group") %>%
    dplyr::select(-group) %>%
    tidyr::pivot_wider(names_from = col, values_from = sim) %>%
    tidyr::pivot_longer(cols = dplyr::starts_with("f"), names_to = "col", values_to = "sim", ) %>%
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
        true = stringdist::stringsim(vals, valt, method_),
        false = sim
      )
    ) %>%
    dplyr::select(-vals, -valt)

  msg_verbose("Calculating uniqness scores", .verbose)
  uni_ <- sim_ %>%
    dplyr::arrange(hash_s, col) %>%
    dplyr::group_by(hash_s, col) %>%
    dplyr::summarise(uni = 1 - mean(sim, na.rm = TRUE), .groups = "drop_last") %>%
    dplyr::mutate(uni = uni / sum(uni)) %>%
    dplyr::ungroup()

  msg_verbose("Finalizing output ...", .verbose)
  match_ <- dplyr::left_join(sim_, uni_, by = c("hash_s", "col")) %>%
    dplyr::mutate(score = sim * uni) %>%
    dplyr::distinct(hash_s, hash_t, dplyr::across(dplyr::matches("^e\\d+$")), col, .keep_all = TRUE) %>%
    tidyr::pivot_wider(names_from = col, values_from = c(sim, uni, score)) %>%
    tibble::as_tibble()


  fst::write_fst(match_, path_matches_)

  if (.return) {
    return(match_)
  } else {
    msg_verbose("Data is stored ...", .verbose)
  }

}


#' Helper Function: Prepare Table
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
prep_table <- function(.tab, .cols, .fstd = standardize_str, .dir, .type = c("s", "t")) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  name <- hash <- dup_id <- hash_use <- hash_dup <- val <- nc <- NULL

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
  co_ <- nm_$col_old
  cn_ <- nm_$col_new

  # Prepare table (1) -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  tab_orig_ <- tibble::as_tibble(.tab) %>%
    dplyr::mutate(
      hash = purrr::map_chr(paste0(!!!dplyr::syms(co_)), fastdigest::fastdigest)
    ) %>%
    dplyr::select(hash, dplyr::everything())


  tab_ <- `colnames<-`(tab_orig_[, c("hash", co_)], c("hash", cn_))

  # Standardize columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if (!is.null(.fstd)) {
    tab_ <- dplyr::mutate(tab_, dplyr::across(c(!!!dplyr::syms(cn_)), .fstd))
  }

  # Get Duplicates after Standardization -- -- -- -- -- -- -- -- -- -- -- -- ---
  dup_ <- filter_dups(tab_, !!!dplyr::syms(cn_)) %>%
    dplyr::group_by(dup_id) %>%
    dplyr::mutate(hash_use = dplyr::first(hash)) %>%
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
  dir.create(file.path(.dir, "tables"), FALSE, TRUE)

  fst::write_fst(tab_orig_, file.path(.dir, "tables", paste0(type_, "orig.fst")), 100)
  fst::write_fst(out_, file.path(.dir, "tables", paste0(type_, "data.fst")), 100)
  fst::write_fst(groups_, file.path(.dir, "tables", paste0(type_, "group.fst")), 100)
  fst::write_fst(dup_, file.path(.dir, "tables", paste0(type_, "dups.fst")), 100)
  fst::write_fst(nm_, file.path(.dir, "tables", paste0(type_, "names.fst")), 100)
}

#' Helper Function: Make Groups
#'
#' @param .dir Directory to store Tables
#' @param .range Character range
#'
#' @return A table with Groups (saved in .dir)
make_groups <- function(.dir, .range = Inf) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug/debug-make_groups.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  nct <- ncs <- ns <- nt <- size <- NULL

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
      ncs1 = ncs[1],
      ncs2 = ncs[1],
      nct1 = min(nct),
      nct2 = max(nct),
      ns = ns[1],
      nt = sum(nt),
      size = ns * nt,
      .groups = "drop_last"
    ) %>%
    dplyr::select(-ncs) %>%
    dplyr::arrange(size, .by_group = TRUE)

  fst::write_fst(groups_, file.path(.dir, "_groups.fst"), 100)
}

#' Prepare Tables for Matching
#'
#' @param .source
#' Source Dataframe
#' @param .target
#' Target Dataframe
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#' @param .fstd
#' Standardization Function (Default: standardize_str())
#' @param .dir
#' Directory to store Tables
#' @param .range
#' A range of characters as an integer, e.g. the name of the source dataframe is 5 characters long and the
#' .range argument is 3, then all names in the target dataframe between 2 - 8 characters are used for the matching
#' @param .return
#' Return a saved file(s)?
#' @param .verbose
#' Print additional Information? (Default: TRUE)
#'
#' @return Dataframes (saved in .dir)
#'
#' @export
prep_tables <- function(.source, .target, .cols, .fstd = standardize_str, .dir,
                        .range = Inf, .return = FALSE, .verbose = TRUE) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  source("_debug_vars/debug-prep_tables.R")

  # Create Directory -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  dir.create(.dir, FALSE, TRUE)

  fs_tab_ <- file.path(.dir, "tables")

  if (dir.exists(fs_tab_)) {
    # Check if files already exist
    nm_ <- c("data", "dups", "group", "names", "orig")
    ex_ <- tibble::tibble(doc_id = c(paste0("s", nm_), paste0("t", nm_), "_groups"))
    fs_ <- dplyr::full_join(lft(fs_tab_), ex_, by = "doc_id")
    na_ <- sum(is.na(fs_$path))

    # If files are present but incomplete throw and error
    if (na_ > 0 & na_ < 11) {
      stop(".dir contains files but is incomplete", call. = FALSE)
    }

    # If files are complete, notify
    if (all(ex_$doc_id %in% fs_$doc_id)) {
      msg_verbose("Stored data is complete, tables won't be prepared again", .verbose)
    }


  } else {

    msg_verbose("Preparing Source Table ...", .verbose)
    prep_table(.tab = .source, .cols = .cols, .fstd = .fstd, .dir = .dir, .type = "s")

    msg_verbose("Preparing Target Table ...", .verbose)
    prep_table(.tab = .target, .cols = .cols, .fstd = .fstd, .dir = .dir, .type = "t")

    msg_verbose("Calculating Groups ...", .verbose)
    make_groups(.dir = file.path(.dir, "tables"), .range = .range)
  }

  if (.return) {
    fil_ <- lft(fs_tab_)
    return(purrr::map(purrr::set_names(fil_$path, fil_$doc_id), fst::read_fst))
  } else {
    msg_verbose("Data is stored ...", .verbose)
  }

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
    .mat_size = 1e7, .range = Inf,
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

  # Check for Matrix Size -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  check_ <- split_input(.source = s_, .target = t_, .cols_match, .mat_size, .range)

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





lst_ <- dplyr::full_join(
  tibble::enframe(split(source_, source_$tmp_), name = "split", value = "s"),
  tibble::enframe(split(target_, target_$tmp_), name = "split", value = "t"),
  by = "split"
) %>% dplyr::mutate(
  t = dplyr::if_else(split == "_NOT_AVAIL_S_", list(.target), t)
)






# .source = lst_$s[[1]]
# .target = lst_$t[[1]]
# .col = cols_f_[[1]]
# .mat_size = .mat_size
make_groups_tab <- function(.source, .target, .col, .mat_size) {
  rle_s_ <- rle(sort(nchar(.source[[.col]])))
  rle_t_ <- rle(sort(nchar(.target[[.col]])))

  tab_t_ <- tibble::tibble(nt = rle_t_$values, t = rle_t_$lengths)

  out_tab_ <- tibble::tibble(ns = rle_s_$values, s = rle_s_$lengths) %>%
    dplyr::mutate( # Get Nchars (nt) and number of items (t)
      nt = purrr::map(ns, ~ tab_t_$nt),
      t  = purrr::map(ns, ~ tab_t_$t),
    ) %>%
    tidyr::unnest(c(nt, t)) %>%
    dplyr::group_by(ns) %>%
    dplyr::arrange(abs(ns - nt), .by_group = TRUE) %>%
    dplyr::mutate(ct = cumsum(t), ms = s * ct) %>%
    dplyr::filter(ms <= .mat_size) %>%
    dplyr::summarise(
      nt = list(sort(nt)),
      s = first(s),
      ms = last(ms)
    ) %>%
    dplyr::mutate(group = make_groups_vec(ms, .mat_size, 1000)) %>%
    dplyr::group_by(group) %>%
    dplyr::summarise(
      ns = list(ns),
      nt = list(sort(unique(unlist(nt)))),
      s = sum(s),
      ms = sum(ms)
    )

  return(out_tab_)
}

make_groups_all <- function(.lst, .cols, .mat_size, .workers) {
  .cols <- unname(.cols)
  opts_ <- furrr::furrr_options(seed = TRUE)
  lst_out_ <- list()
  future::plan("multisession", workers = .workers)
  for (col in .cols) {
    lst_out_[[col]] <- furrr::future_map2(
      .x = purrr::set_names(.lst$s, .lst$split),
      .y = purrr::set_names(.lst$t, .lst$split),
      .f = ~ make_groups_tab(.x, .y, col, .mat_size),
      .options = opts_
    )
  }
  future::plan("default")
  return(lst_out_)
}

tab_ <- make_groups_all(lst_, cols_f_, .mat_size, .workers) %>%
  tibble::enframe(name = "col") %>%
  dplyr::mutate(value = purrr::map(value, ~ dplyr::bind_rows(.x, .id = "split"))) %>%
  tidyr::unnest(value) %>%
  dplyr::select(group, split, col, ns, nt, ms)


make_groups_vec <- function(.vec, .max, .iter = 1000) {
  min_groups_ <- ceiling(sum(.vec) / .max)
  lst_ <- list()
  for (iter in seq_len(.iter)) {
    set.seed(iter)
    idx_ <- sample(seq_len(length(.vec)))
    vec_ <- .vec[idx_]

    groups_ <- integer()
    group_ <- sum_ <- 0
    for (i in seq_len(length(.vec))) {
      v0_ <- vec_[i]
      v1_ <- ifelse(is.na(vec_[i + 1]), 0, vec_[i + 1])
      sum_ <- sum_ + v0_ + v1_

      if (sum_ > .max) {
        group_ <- group_ + 1
        sum_ <- 0
      }

      groups_[i] <- group_
    }
    lst_[[iter]] <- tibble::tibble(group = groups_, idx = idx_, vec = vec_) %>%
      dplyr::group_by(group) %>%
      dplyr::summarise(idx = list(idx), sum = sum(vec))

    if (length(unique(groups_)) == min_groups_) break
  }

  out_vec_ <- dplyr::bind_rows(lst_, .id = "iter") %>%
    dplyr::mutate(iter = as.integer(iter)) %>%
    dplyr::group_by(iter) %>%
    dplyr::mutate(n = dplyr::n(), sd = stats::sd(sum), check = all(sum <= .max)) %>%
    tidyr::replace_na(list(sd = 0)) %>%
    dplyr::ungroup() %>%
    dplyr::filter(check) %>%
    dplyr::filter(n == min(n)) %>%
    dplyr::filter(sd == min(sd)) %>%
    dplyr::filter(iter == min(iter)) %>%
    tidyr::unnest(idx) %>%
    dplyr::group_by(tmp = group) %>%
    dplyr::mutate(group = dplyr::cur_group_id()) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(idx) %>%
    dplyr::pull(group) %>%
    suppressWarnings()

  if (length(out_vec_) != length(.vec)) {
    return(seq_len(length(.vec)))
  } else {
    return(out_vec_)
  }
}



%>%
  dplyr::group_by(tmp_, col, nchar_t) %>%
  dplyr::mutate(group_t = dplyr::cur_group_id()) %>%
  dplyr::ungroup()

target3_ <- dplyr::count(target2_, tmp_, col, nchar_t, group_t, name = "n_t")


.vec <- sample(100:200, 10)



order(sort(.vec))

groups_ <- integer()
group_ <- sum_ <- 0
for (i in seq_len(length(.vec))) {
  v0_ <- vec_[i]
  v1_ <- ifelse(is.na(vec_[i + 1]), 0, vec_[i + 1])
  sum_ <- sum_ + v0_ + v1_

  if (sum_ > .max) {
    group_ <- group_ + 1
    sum_ <- 0
  }

  groups_[i] <- group_
}
lst_[[iter]] <- tibble::tibble(group = groups_, idx = idx_, vec = vec_) %>%
  dplyr::group_by(group) %>%
  dplyr::summarise(idx = list(idx), sum = sum(vec))

if (length(unique(groups_)) == min_groups_) break
}


# source2_ <- dplyr::select(source1_, tmp_, id, dplyr::matches(reg_f_)) %>%
#   tidyr::pivot_longer(dplyr::matches(reg_f_), names_to = "col", values_to = "nchar_s") %>%
#   dplyr::left_join(target3_, by = c("tmp_", "col")) %>%
#   dplyr::filter(!is.na(nchar_t)) %>%
#   dplyr::mutate(diff_char = abs(nchar_s - nchar_t)) %>%
#   dplyr::group_by(tmp_, col) %>%
#   dplyr::arrange(nchar_s, diff_char, .by_group = TRUE) %>%
#   dplyr::mutate(
#     cn_t = cumsum(n_t),
#     cn_s = dplyr::row_number()
#     ),
#     mat_size = cn_t * cn_s,
#     tmp_group = ceiling(mat_size / .mat_size),
#     ) %>%
#   dplyr::ungroup() %>%
#   dplyr::group_by(tmp_, tmp_group, col) %>%
#   dplyr::mutate(group_s = dplyr::cur_group_id()) %>%
#   dplyr::ungroup()

source_out_ <- dplyr::select(source2_, group_s, id_s = id)
target_out_ <- dplyr::select(target2_, group_t, id_t = id)
combin_out <- dplyr::distinct(source2_, group_s, group_t) %>%
  dplyr::left_join(dplyr::count(source_out_, group_s, name = "n_s")) %>%
  dplyr::left_join(dplyr::count(target_out_, group_t, name = "n_t")) %>%
  dplyr::mutate(mat_size = n_s * n_t)


%>%
  dplyr::group_by(group, tmp_, col, nchar_t) %>%
  dplyr::summarise(id_s = list(id), .groups = "drop") %>%
  dplyr::left_join(
    target2_ %>%
      dplyr::group_by(tmp_, col, nchar_t) %>%
      dplyr::summarise(id_t = list(id), .groups = "drop")
  ) %>%
  dplyr::mutate(mat_size = lengths(id_s) * lengths(id_t)) %>%
  dplyr::select(group, tmp_, col, mat_size)


source_out_ <- source2_ %>%
  dplyr::group_by(group) %>%
  dplyr::summarise(id_s = list(id))



target_out_ <- target2_ %>%
  dplyr::group_by(tmp_, col, nchar_t) %>%
  dplyr::summarise(id = list(id)) %>%
  dplyr::left_join(dplyr::distinct(source2_, tmp_, col, nchar_t, group)) %>%
  dplyr::group_by(group) %>%
  dplyr::summarise(id_t = list(unlist(id)))

out_ <- dplyr::left_join(source_out_, target_out_) %>%
  dplyr::mutate(mat_size = lengths(id_s) * lengths(id_t)) %>%
  dplyr::select(group, mat_size)

#
#   %>%
#     dplyr::select(-id)
#   tab_ <-
#
#   target_out_ <-



source3_ <- dplyr::count(source2_, tmp_, col, nchar_s, name = "n_s")



# Prepare Target so every Non Split SOurce Group is ... -- -- -- -- -- -- -- -- -- -- --



tab0_ <- dplyr::full_join(source3_, target5_, by = c("tmp_", "col")) %>%
  dplyr::mutate(diff_char = abs(nchar_s - nchar_t), mat_size = n_s * n_t) %>%
  dplyr::select(tmp_, col, nchar_s, nchar_t, n_s, n_t, diff_char, mat_size) %>%
  dplyr::filter(!is.na(nchar_t), !is.na(mat_size))

max_ <- max(max(tab0_$mat_size), .mat_size)

tab1_ <- tab0_ %>%
  dplyr::group_by(tmp_, col, nchar_s) %>%
  dplyr::arrange(diff_char, .by_group = TRUE) %>%
  dplyr::filter(cumsum(mat_size) <= max_) %>%
  dplyr::summarise(nchar_t = list(nchar_t), mat_size = sum(mat_size), .groups = "drop_last")

# %>%
#   dplyr::group_split()


tab_more_ <- dplyr::filter(tab1_, mat_size > .mat_size)
tab_less_ <- dplyr::filter(tab1_, mat_size <= .mat_size)
source_more_ <- source2_ %>%
  dplyr::inner_join(dplyr::select(tab_more_, tmp_, col, nchar_s), by = c("tmp_", "col", "nchar_s"))



rows_ <- which(tab0_$mat_size > .mat_size)
if (length(rows_) > 0) {
  large_ <- dplyr::slice(dplyr::filter(tab0_[rows_, ], mat_size == max(mat_size)), 1)

  msg_ <- paste0(
    "\nMatrix Size is too big!\n",
    "Largest Group: ", large_$tmp_, "-", large_$col, " (", scales::comma(large_$mat_size), " Elements).\n",
    "Consider increasing .mat_size (currently: ", scales::comma(.mat_size), " Elements)\n"
  )
  stop(msg_, call. = FALSE)
}


opts_ <- furrr::furrr_options(seed = TRUE)
future::plan("multisession", workers = .workers)
tab2_ <- furrr::future_map_dfr(
  .x = tab1_,
  .f = ~ dplyr::mutate(.x, group = make_groups_vec(mat_size, max_)),
  .options = opts_
) %>%
  dplyr::group_by(group, split = tmp_, col) %>%
  dplyr::summarise(
    nchar_s = list(sort(nchar_s)),
    nchar_t = list(sort(unique(unlist(nchar_t)))),
    mat_size = sum(mat_size),
    .groups = "drop"
  )
future::plan("default")

return(tab2_)


reshape_mat <- function(.mat, .max_match) {

  mat0_ <- .mat
  mat1_ <- Rfast::rowSort(mat0_, descending = TRUE, parallel = TRUE)
  mat1_ <- if (all(class(mat1_) == "numeric")) matrix(mat1_, nrow = 1) else mat1_
  mat1_ <- mat1_[, seq_len(min(ncol(mat1_), .max_match))]
  mat1_ <- if (all(class(mat1_) == "numeric")) matrix(mat1_, nrow = 1) else mat1_

  lst0_ <- purrr::map(seq_len(nrow(mat0_)), ~ mat0_[.x, ])
  lst1_ <- purrr::map(seq_len(nrow(mat1_)), ~ mat1_[.x, ])
  idx_  <- purrr::map2(lst0_, lst1_, ~ which(.x %in% .y))
  val_  <- purrr::map2(lst0_, idx_, ~ .x[.y])

  tibble::tibble(
    id_s = rep(seq_len(length(lst0_)), lengths(idx_)),
    id_t = unlist(idx_),
    sim = unlist(val_)
  )
}


reshape_mat <- function(.mat, .max_match) {

  mat0_ <- .mat
  lst_ <- list()
  for (i in seq_len(.max_match)) {
    val_ <- Rfast::rowMaxs(mat0_, TRUE)
    idx_ <- which(mat0_ == val_, TRUE)
    lst_[[i]] <- tibble::tibble(
      id_s = idx_[, 1],
      id_t = idx_[, 2],
      sim  = mat0_[idx_]
    )
    mat0_[idx_] <- -100
  }
  tab1_ <- dplyr::bind_rows(lst_) %>%
    dplyr::filter(!sim == -100, !sim == 0) %>%
    dplyr::arrange(id_s, dplyr::desc(sim))

  tab2_ <- reshape2::melt(.mat) %>%
    dplyr::rename(id_s = Var1, id_t = Var2, sim = value) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(id_s) %>%
    dplyr::filter(dplyr::dense_rank(-sim) <= .max_match) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble() %>%
    dplyr::arrange(id_s, dplyr::desc(sim))

}



f1 <- function(.int, .max_match) {
  set.seed(123)
  mat0_ <- matrix(ceiling(runif(.int^2, 1, 10000)), .int, .int)
  lst_ <- list()
  for (i in seq_len(.max_match)) {
    val_ <- Rfast::rowMaxs(mat0_, TRUE)
    idx_ <- which(mat0_ == val_, TRUE)
    lst_[[i]] <- tibble::tibble(
      id_s = idx_[, 1],
      id_t = idx_[, 2],
      sim  = mat0_[idx_]
    )
    mat0_[idx_] <- -100
  }
  dplyr::bind_rows(lst_) %>%
    dplyr::filter(!sim == -100, !sim == 0) %>%
    dplyr::arrange(id_s, dplyr::desc(sim))
}

f2 <- function(.int, .max_match) {
  set.seed(123)
  mat0_ <- matrix(ceiling(runif(.int^2, 1, 10000)), .int, .int)
  reshape2::melt(mat0_) %>%
    dplyr::rename(id_s = Var1, id_t = Var2, sim = value) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(id_s) %>%
    dplyr::filter(dplyr::dense_rank(-sim) <= .max_match) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(id_s, dplyr::desc(sim))
  tibble::as_tibble()

}


prs_ <- bench::press(
  .int = c(10, 25, 50),
  .max_match = c(5, 10, 15),
  bench::mark(
    f1 = f1(.int, .max_match),
    f2 = f2(.int, .max_match),
    check = FALSE
  )
)[, 1:12]

a <- bench::mark(
  f1 = f1(10000, 50),
  f2 = f2(10000, 50),
)[, 1:9]

a <- f1(500, 50)
b <- f2(500, 50)

all.equal(a, b)


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

#' Helper Function: Check for named vector
#'
#' @param .tab
#' A dataframe (either the source or target dataframe)
#'
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#'
#' @return Nothing or an error
old_check_names <- function(.tab, .cols) {
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
  )
  tab1_ <- reshape_mat(tab1_, .max_match) %>%
    dplyr::mutate(
      id_s = s_[["id"]][id_s],
      id_t = t_[["id"]][id_t]
    )
  dplyr::bind_rows(tab0_, tab1_)

}


# Helper Functions -------------------------------------------------------------
#' Helper Function: Check for named vector
#'
#' @param .tab
#' A dataframe (either the source or target dataframe)
#'
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#'
#' @return Nothing or an error
old_check_names <- function(.tab, .cols) {
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

check_id <- function(.tab, .col, .type = c("s", "t")) {
  check_ <- .col %in% colnames(.tab)
  type_ <- match.arg(.type, c("s", "t"))
  type_ <- ifelse(type_ == "s", ".source", ".target")

  if (!check_) {
    msg_ <- glue::glue("{type_} dataframe must contain column '{.col}'")
    stop(msg_, call. = FALSE)
  }
}


check_dups <- function(.tab, .col) {
  check_ <- any(duplicated(.tab[[.col]]))
  if (check_) {
    msg_ <- glue::glue("Column '{.col}' must not have duplicates")
    stop(msg_, call. = FALSE)
  }
}

check_matching_cols <- function(.path, .cols, .type = c("s", "t")) {
  cols_ <- fst::metadata_fst(.path)[["columnNames"]]
  type_ <- match.arg(.type, c("s", "t"))
  type_ <- ifelse(type_ == "s", ".source", ".target")


  if (!all(.cols %in% scols_)) {
    msg_ <- glue::glue("Not all matching columns are present in the {type_} dataframe")
    stop(msg_, call. = FALSE)
  }
}

check_names <- function(.cols) {
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
