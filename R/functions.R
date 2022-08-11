# Helper Functions -------------------------------------------------------------
#' Helper Function: List Files in Table
#'
#' @param .dirs Vector/List or single paths to directory/directories
#' @param .reg Regular Expression to find files (defaults to '*' all files)
#' @param .id Column name containing the file name (defaults to 'doc_id')
#' @param .rec Should the directories be searched recursively?
#'
#' @return A dataframe with file paths
lft <- function(.dirs, .reg = NULL, .id = "doc_id", .rec = FALSE) {
  path <- file_ext <- id <- NULL

  purrr::map_dfr(
    .x = .dirs,
    .f = ~ tibble::tibble(path = list.files(.x, .reg, FALSE, TRUE, .rec))
  ) %>%
    dplyr::mutate(
      file_ext = paste0(".", tools::file_ext(path)),
      id = stringi::stri_replace_last_fixed(basename(path), file_ext, ""),
      path = purrr::set_names(path, id)
    ) %>%
    dplyr::select(id, file_ext, path) %>%
    `colnames<-`(c(.id, "file_ext", "path"))
}

#' Check for Duplicates
#'
#' @param .tab A dataframe
#' @param ... Any number of columns of the dataframe
#'
#' @import data.table
#'
#' @return A dataframe
#' @export
filter_dups <- function(.tab, ...) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  `_tmp_` <- dup_id <- NULL

  vars_ <- dplyr::enquos(...)
  .tab %>%
    dtplyr::lazy_dt() %>%
    dplyr::mutate(`_tmp_` = paste(!!!vars_, sep = "<><><>")) %>%
    dplyr::filter(duplicated(`_tmp_`) | duplicated(`_tmp_`, fromLast = TRUE)) %>%
    dplyr::arrange(`_tmp_`) %>%
    dplyr::group_by(`_tmp_`) %>%
    dplyr::mutate(dup_id = dplyr::cur_group_id()) %>%
    dplyr::ungroup() %>%
    dplyr::select(dup_id, dplyr::everything(), -`_tmp_`) %>%
    tibble::as_tibble()
}


#' Helper Function: Print Messages
#'
#' @param .msg A character string
#' @param .verbose TRUE/FALSE
#'
#' @return A message
msg_verbose <- function(.msg, .verbose) {
  n_ <- 80 - nchar(.msg)
  msg_ <- paste0("\n", .msg, paste(rep(" ", n_), collapse = ""))

  if (.verbose) message(msg_)
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

#' Helper Function: tidyft::filter_fst
#'
#' @param ft An ft object
#' @param dot_string Filter string
#'
#' @return Filtered Dataframe
filter_fst_adj <- function(ft, dot_string) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

  ft_names <- names(ft)
  old <- ft_names[stringr::str_detect(dot_string, ft_names)]
  new <- paste0("ft$", old)
  for (i in seq_along(old)) dot_string <- gsub(old[i], new[i], dot_string)
  eval(parse(text = stringr::str_glue("ft[{dot_string},] %>% tidyft::as.data.table()")))
}


#' Helper Function: Filter data from fst files
#'
#' @param .dir Directory to store Tables
#' @param .row One row corrsponds to a group
#'
#' @return Filtered Dataframe
filter_groups <- function(.dir, .row) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

  sdata <- tidyft::parse_fst(file.path(.dir, "tables", "sdata.fst"))
  tdata <- tidyft::parse_fst(file.path(.dir, "tables", "tdata.fst"))
  ce_ <- colnames(sdata)[startsWith(colnames(sdata), "e")]

  tab_ <- tibble::as_tibble(sdata)
  col_ <- .row$col
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

  stab <- filter_fst_adj(sdata, strs_)
  ttab <- filter_fst_adj(tdata, strt_)

  return(list(s = stab, t = ttab))
}

# Main Functions ----------------------------------------------------------
#' Get Method Names
#'
#' @return A string with the methods used for matching
#' @export
get_method_names <- function() {
  methods_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  purrr::set_names(methods_, methods_)
}

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
#' standardize_str(c("jkldsa   jkdhas   sa  §$ ## #'''"))
#' standardize_str(c("jkldsa   jkdhas   fsd  §$ ## #'''"), "space")
standardize_str <- function(.str, .op = c("space", "punct", "case", "ascii")) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
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


#' Extract Legal Forms
#'
#' Description
#'
#' @param .tab A dataframe (either the source or target dataframe)
#' @param .col The column with firm names
#' @param .legal_forms A dataframe with legal forms
#' @param .workers Number of cores to utilize (Default all cores determined by future::availableCores())
#'
#' @return A dataframe
#'
#' @export
extract_legal_form <- function(
    .tab, .col, .legal_forms = data.frame(),
    .workers = future::availableCores() / 2
) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("_debug/debug-extract_legal_form.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  id <- lfid <- NULL

  # Shortcut Functions -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  symp <- function(...) dplyr::sym(paste0(...))
  rlf <- stringi::stri_replace_last_fixed
  `:=` <- rlang::`:=`

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tmp <- lfo <- lfs <- legal_form <- name <-  lf_stand <- lf_orig <- NULL

  # Convert to Tibble and Standardize -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  tab_ <- tibble::as_tibble(.tab)
  tab_[[.col]] <- standardize_str(tab_[[.col]])

  # Get Legal Form Table -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  tab_lf_ <- if (nrow(.legal_forms) == 0) {
    get("legal_form_all")
  } else {
    .legal_forms
  }

  # Extract Legal Forms -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  lf_ <- unique(tab_lf_[["lfo"]])
  nm_ <- tab_[[.col]]

  f_ <- carrier::crate(function(.lf, .nm) which(endsWith(.nm, paste0(" ", .lf))))
  future::plan("multisession", workers = .workers)
  lst_lf_ext_ <- furrr::future_map(
    .x = purrr::set_names(lf_, lf_),
    .f = ~ f_(.x, nm_),
    .options = furrr::furrr_options(seed = TRUE, globals = c("f_", "nm_"))
  )
  future::plan("default")


  # Reshape List to Dataframe -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tab_lf_ext_ <- lst_lf_ext_ %>%
    purrr::compact() %>%
    tibble::enframe(name = "lfo", value = "tmp") %>%
    tidyr::unnest(tmp) %>%
    dplyr::arrange(dplyr::desc(nchar(lfo))) %>%
    dplyr::distinct(tmp, .keep_all = TRUE)

  # Get Output -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tab_ %>%
    dplyr::mutate(tmp = dplyr::row_number()) %>%
    dplyr::left_join(tab_lf_ext_, by = "tmp") %>%
    dplyr::left_join(dplyr::rename(tab_lf_, lfid = id), by = c("iso3", "lfo")) %>%
    dplyr::mutate(
      !!symp(.col, "_adj") := trimws(rlf(!!symp(.col), lfo, "")),
      .after = !!symp(.col)) %>%
    dplyr::mutate(
      !!symp(.col, "_adj") := dplyr::if_else(
        is.na(!!symp(.col, "_adj")), !!dplyr::sym(.col), !!symp(.col, "_adj")
      )) %>%
    dplyr::mutate(
      !!symp(.col, "_std") := dplyr::if_else(
        !is.na(lfs), paste(!!symp(.col, "_adj"), lfs), !!symp(.col, "_adj")
      ), .after = !!symp(.col, "_adj")) %>%
    dplyr::select(-tmp) %>%
    dplyr::relocate(lfid, .after = lfs)
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
  # source("_debug_vars/debug-prep_tables.R")

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
    return(NULL)
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
    dplyr::summarise(uni = mean(sim, na.rm = TRUE), .groups = "drop_last") %>%
    dplyr::mutate(uni = uni / sum(uni)) %>%
    dplyr::ungroup()

  msg_verbose("Finalizing output ...", .verbose)
  match_ <- dplyr::left_join(sim_, uni_, by = c("hash_s", "col")) %>%
    dplyr::mutate(score = sim * uni) %>%
    tidyr::pivot_wider(names_from = col, values_from = c(sim, uni, score)) %>%
    tibble::as_tibble()


  fst::write_fst(match_, path_matches_)

  if (.return) {
    return(match_)
  } else {
    msg_verbose("Data is stored ...", .verbose)
  }

}


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
    return(NULL)
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
    scores_ <- dplyr::mutate(matches_, score = score_f1)
  } else {
    int_ <- which(startsWith(colnames(matches_), "score"))
    for (i in seq_len(length(int_))) {
      matches_[, int_[i]] <- matches_[, int_[i]] * names_$weight[i]
    }

    scores_ <- dplyr::mutate(matches_, score = rowSums(matches_[, int_], na.rm = TRUE))
  }


  scores_ <- scores_ %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(hash_s) %>%
    dplyr::arrange(dplyr::desc(score), .by_group = TRUE) %>%
    dplyr::mutate(rank = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::filter(rank <= .max_match) %>%
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
#' @param .rank Up to which rank should teh data be retrieved?
#' @param .method c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#'
#' @return A dataframe
#' @export
select_data <- function(
    .dir,
    .rank = 1,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
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

  scores_ %>%
    dplyr::filter(rank <= .rank) %>%
    tibble::as_tibble() %>%
    dplyr::select(hash_s, hash_t, score, dplyr::starts_with("e")) %>%
    dplyr::left_join(sdata_, by = "hash_s", suffix = c("_s", "_t")) %>%
    dplyr::left_join(tdata_, by = "hash_t", suffix = c("_s", "_t")) %>%
    `colnames<-`(stringi::stri_replace_all_regex(colnames(.), paste0("^", names_$col_new, "$"), names_$col_old, FALSE))
}
