# Helper Functions -------------------------------------------------------------
get_method <- function(.method) {
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  match.arg(.method, choices_)
}

get_dir_store <- function(.dir, .cols, .range, .max_match, .method) {
  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  value <- name <- cols <- max_match <- method <- hash <- NULL

  tab_ <- tibble::enframe(.cols) %>%
    dplyr::arrange(value) %>%
    dplyr::mutate(cols = paste0(name, ": ", value)) %>%
    dplyr::summarise(cols = paste(cols, collapse = ", ")) %>%
    dplyr::mutate(range = .range, max_match = .max_match, method = .method) %>%
    dplyr::mutate(hash = digest::digest(paste0(cols, range, max_match, method))) %>%
    dplyr::select(hash, cols, range, max_match, method)

  dir_ <- file.path(.dir, tab_$hash)
  dir.create(dir_, FALSE, TRUE)

  fil_ <- file.path(.dir, "variants.fst")
  if (file.exists(fil_)) {
    tab_ <- dplyr::distinct(dplyr::bind_rows(fst::read_fst(fil_), tab_))
  }

  fst::write_fst(tab_, fil_, 100)
  return(dir_)
}

get_dirs_and_files <- function(.dir, .cols, .range, .max_match, .method) {
  method_ <- get_method(.method)
  dir_tabs_ <- file.path(.dir, "tables")
  dir_store_ <- get_dir_store(.dir, .cols, .range, .max_match, .method = method_)
  fil_match_ <- file.path(dir_store_, "matches.fst")

  list(dir_tabs = dir_tabs_, dir_store = dir_store_, fil_match = fil_match_)
}

get_globals <- function() {
  c(
    "method_", ".max_match", ".workers", "match_group", "filter_groups",
    ".dir", "filter_fst_adj", "dirs_"
  )
}


#' Helper Function: Check if table has a column called 'id'
#'
#' @param .tab A dataframe
#' @param .type Either s or t
#'
#' @return Nothing or Error
check_id <- function(.tab, .type = c("s", "t")) {
  check_ <- "id" %in% colnames(.tab)
  type_ <- match.arg(.type, c("s", "t"))
  type_ <- ifelse(type_ == "s", ".source", ".target")

  if (!check_) {
    msg_ <- glue::glue("{type_} dataframe must contain column '{.col}'")
    stop(msg_, call. = FALSE)
  }
}

#' Helper Function: Check if column contains duplicates
#'
#' @param .tab A dataframe
#' @param .col A column as string
#'
#' @return Nothing or Error
check_dups <- function(.tab, .col) {
  check_ <- any(duplicated(.tab[[.col]]))
  if (check_) {
    msg_ <- glue::glue("Column '{.col}' must not have duplicates")
    stop(msg_, call. = FALSE)
  }
}

#' Helper Function: Check if all matching columns are present
#'
#' @param .path A path to the stored tables
#' @param .cols matching columns
#' @param .type Either s or t
#'
#' @return Nothing or Error
check_matching_cols <- function(.path, .cols, .type = c("s", "t")) {
  cols_ <- fst::metadata_fst(.path)[["columnNames"]]
  type_ <- match.arg(.type, c("s", "t"))
  type_ <- ifelse(type_ == "s", ".source", ".target")


  if (!all(.cols %in% cols_)) {
    msg_ <- glue::glue("Not all matching columns are present in the {type_} dataframe")
    stop(msg_, call. = FALSE)
  }
}

#' Helper Function: Check if matching vector is names
#'
#' @param .cols columns in a vectore
#'
#' @return Nothing or Error
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

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("_debug_vars/debug-filter_dups.R")

  # Get Quosures -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  vars_ <- dplyr::enquos(...)

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  id_dup <- n_dup <- id <- NULL

  # Get Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  .tab %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(!!!vars_) %>%
    dplyr::mutate(
      id_dup  = dplyr::cur_group_id(),
      n_dup   = dplyr::n()
    ) %>%
    dplyr::ungroup() %>%
    dplyr::filter(n_dup > 1) %>%
    dplyr::arrange(id_dup) %>%
    dplyr::select(id, id_dup, n_dup, dplyr::everything()) %>%
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



#' Helper Function: Prepare Table
#'
#' Description
#'
#' @param .tab
#' A dataframe (either the source or target dataframe)
#' @param .fstd
#' Standardization Function
#' @param .dir
#' Directory to store Tables
#' @param .type
#' Either s (source) or t (target)
#' @return A dataframe
prep_table <- function(.tab, .fstd = NULL, .dir, .type = c("s", "t")) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("_debug_vars/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  name <- hash <- dup_id <- hash_use <- hash_dup <- val <- nc <- id <- NULL

  # Create Directories -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  dir_tabs_ <- file.path(.dir, "tables")
  if (!dir.exists(dir_tabs_)) dir.create(dir_tabs_, FALSE, TRUE)

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  type_ <- match.arg(.type, c("s", "t"))

  # Checks -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  check_id(.tab = .tab, .type = type_)
  check_dups(.tab = .tab, .col = "id")

  # Save Original Table -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tab_orig_ <- dplyr::select(.tab, id, dplyr::everything())
  fst::write_fst(tab_orig_, file.path(dir_tabs_, paste0(type_, "orig.fst")), 100)

  # Transform Table -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  tab_out_ <- .tab %>%
    tidyr::pivot_longer(!dplyr::matches("^id$"), names_to = "col", values_to = "val")

  if (!is.null(.fstd)) {
    tab_out_ <- dplyr::mutate(tab_out_, val = .fstd(val))
  }
  tab_out_ <- dplyr::mutate(tab_out_, nc = nchar(val))

  fst::write_fst(tab_out_, file.path(dir_tabs_, paste0(type_, "data.fst")), 100)
}


transform_input <- function(.tab, .cols_e, .cols_f) {
  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  val <- id <- group <- nc <- tmp <- NULL

  group_ <- .tab %>%
    dtplyr::lazy_dt() %>%
    dplyr::filter(col %in% .cols_e) %>%
    dplyr::mutate(val = dplyr::if_else(is.na(val), "_none_", val)) %>%
    dplyr::group_by(id) %>%
    dplyr::summarise(group = paste(val, collapse = "<>"), .groups = "drop") %>%
    dplyr::mutate(group = dplyr::if_else(grepl("_none_", group, fixed = TRUE), "_none_", group)) %>%
    tibble::as_tibble()
  .tab %>%
    dplyr::filter(col %in% .cols_f) %>%
    dtplyr::lazy_dt() %>%
    dplyr::left_join(group_, by = "id") %>%
    dplyr::group_by(group, col, val, nc) %>%
    dplyr::mutate(tmp = dplyr::cur_group_id()) %>%
    dplyr::arrange(tmp) %>%
    dplyr::select(tmp, id, group, col, val, nc) %>%
    dplyr::mutate(group = dplyr::if_else(is.na(group), "_none_", group)) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble()
}


#' Helper Function: Match a single Group
#'
#' @param .lst A list produced by filter_groups()
#' @param .max_match Maximum number of matches
#' @param .method c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#'
#' @import data.table
#'
#' @return A dataframe
match_group <- function(
    .lst, .max_match = 10,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
    ) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug/debug-prep_tables.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  id <- val <- tmp_s <- tmp_t <- Var1 <- Var2 <- value <- sim <- val_s <- val_t <- tmp <- NULL

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  choices_ <- c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
  method_ <- match.arg(.method, choices_)

  s_ <- dplyr::select(dtplyr::lazy_dt(.lst$s), tmp, val)
  t_ <- dplyr::select(dtplyr::lazy_dt(.lst$t), tmp, val)

  exact_ <- s_ %>%
    dplyr::inner_join(t_, by = "val", suffix = c("_s", "_t")) %>%
    dplyr::mutate(val_s = val, val_t = val) %>%
    dplyr::mutate(sim = 1) %>%
    dplyr::select(tmp_s, tmp_t, val_s, val_t, sim) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(col = .lst$col) %>%
    dplyr::select(col, tmp_s, tmp_t, val_s, val_t, sim)

  s_ <- dplyr::filter(tibble::as_tibble(s_), !tmp %in% exact_$tmp_s)
  t_ <- tibble::as_tibble(t_)

  if (nrow(s_) == 0 | nrow(t_) == 0) {
    return(exact_)
  }


  fuzzy_ <- stringdist::stringsimmatrix(
    a = s_[["val"]],
    b = t_[["val"]],
    method = .method
  ) %>%
    reshape2::melt() %>%
    dplyr::rename(tmp_s = Var1, tmp_t = Var2, sim = value) %>%
    # dtplyr::lazy_dt() %>%
    dplyr::filter(sim > 0) %>%
    dplyr::group_by(tmp_s) %>%
    dplyr::arrange(-sim, .by_group = TRUE) %>%
    dplyr::filter(sim >= dplyr::nth(sim, .max_match, default = 0.00001)) %>%
    dplyr::ungroup() %>%
    dplyr::arrange(tmp_s, dplyr::desc(sim)) %>%
    tibble::as_tibble() %>%
    dplyr::mutate(
      val_s = s_[["val"]][tmp_s],
      val_t = t_[["val"]][tmp_t],
      tmp_s = s_[["tmp"]][tmp_s],
      tmp_t = t_[["tmp"]][tmp_t],
    )

  dplyr::bind_rows(exact_, fuzzy_) %>%
    dplyr::mutate(col = .lst$col) %>%
    dplyr::select(col, tmp_s, tmp_t, val_s, val_t, sim)
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
#' @param .group One row corrsponds to a group
#'
#' @return Filtered Dataframe
filter_groups <- function(.dir, .group) {
  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -

  sdata <- tidyft::parse_fst(file.path(.dir, "sdata.fst"))
  tdata <- tidyft::parse_fst(file.path(.dir, "tdata.fst"))

  col_ <- .group$col
  grp_ <- .group$group
  ncs1_ <- .group$ncs1
  ncs2_ <- .group$ncs2
  nct1_ <- .group$nct1
  nct2_ <- .group$nct2
  id1_ <- .group$id1
  id2_ <- .group$id2

  strs_ <- as.character(
    glue::glue('col == "{col_}" & group == "{grp_}" & nc >= {ncs1_} & nc <= {ncs2_} & tmp >= {id1_} & tmp <= {id2_}')
    )
  strt_ <- as.character(
    glue::glue('col == "{col_}" & group == "{grp_}" & nc >= {nct1_} & nc <= {nct2_}')
    )

  stab <- filter_fst_adj(sdata, strs_)
  ttab <- filter_fst_adj(tdata, strt_)

  return(list(s = stab, t = ttab, group = .group$group, col = col_))
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
#' @param .fstd
#' Standardization Function (Default: standardize_str())
#' @param .dir
#' Directory to store Tables
#' @param .return
#' Return a saved file(s)?
#' @param .verbose
#' Print additional Information? (Default: TRUE)
#'
#' @return Dataframes (saved in .dir)
#'
#' @export
prep_tables <- function(.source, .target, .fstd = NULL, .dir, .return = FALSE, .verbose = TRUE) {

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  doc_id <- NULL

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  # source("_debug_vars/debug-prep_tables.R")

  # Create Directory -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  dir_tabs_ <- file.path(.dir, "tables")

  fils_   <- lft(dir_tabs_)
  expect_ <- c("sorig", "sdata", "torig", "tdata")
  fils_   <- dplyr::filter(fils_, doc_id %in% expect_)

  if (!all(expect_ %in% fils_$doc_id)) {
    msg_verbose("Preparing Source Table ...", .verbose)
    prep_table(.tab = .source, .fstd = .fstd, .dir = .dir, .type = "s")

    msg_verbose("Preparing Target Table ...", .verbose)
    prep_table(.tab = .target, .fstd = .fstd, .dir = .dir, .type = "t")

    msg_verbose("Data is stored ...", .verbose)
  } else {
    if (.return) {
      fil_ <- lft(dir_tabs_)
      return(purrr::map(purrr::set_names(fil_$path, fil_$doc_id), fst::read_fst))
    } else {
      msg_verbose("Data is aready stored ...", .verbose)
    }
  }
}

#' Helper Function: Make Groups
#'
#' @param .dir
#' Directory to store Tables
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#' @param .range
#' A range of characters as an integer, e.g. the name of the source dataframe is 5 characters long and the
#' .range argument is 3, then all names in the target dataframe between 2 - 8 characters are used for the matching
#' @param .max_match
#' Maximum number of matches
#' @param .method
#' c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param .mat_size
#' Maximum Matrix Size for Fuzzy Matching
#'
#' @return A table with Groups (saved in .dir)
#'
#' @export
make_groups <- function(.dir, .cols, .range = Inf, .max_match, .method, .mat_size = 1e7) {

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  tmp <- id <- val <- group <- nc <- nct <- ncs <- ns <- nt <- ids <- size <- check <- NULL

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("_debug_vars/debug-match_data.R")

  method_ <- get_method(.method)

  # Get Directories -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  dirs_ <- get_dirs_and_files(.dir, .cols, .range, .max_match, .method = method_)

  dir.create(file.path(dirs_$dir_store, "_tmp"), FALSE, TRUE)

  cols_e_ <- .cols[names(.cols) %in% c("e", "exact")]
  cols_f_ <- .cols[names(.cols) %in% c("f", "fuzzy")]

  tabs_ <- fst::read_fst(file.path(dirs_$dir_tabs, "sdata.fst"))
  tabs_ <- transform_input(tabs_, cols_e_, cols_f_)
  ids_  <- dplyr::select(tabs_, tmp, id)
  tabs_ <- dplyr::distinct(tabs_, tmp, group, col, val, nc)

  fst::write_fst(ids_, file.path(dirs_$dir_store, "_tmp", "sids.fst"), 100)
  fst::write_fst(tabs_, file.path(dirs_$dir_store, "_tmp", "sdata.fst"), 100)


  tabt_ <- fst::read_fst(file.path(dirs_$dir_tabs, "tdata.fst"))
  tabt_ <- transform_input(tabt_, cols_e_, cols_f_)
  idt_  <- dplyr::select(tabt_, tmp, id)
  tabt_ <- dplyr::distinct(tabt_, tmp, group, col, val, nc)
  tabt_ <- dplyr::bind_rows(tabt_, dplyr::mutate(tabt_, group = "_none_"))
  tabt_ <- tabt_ %>%
    dtplyr::lazy_dt() %>%
    dplyr::distinct(tmp, group, .keep_all = TRUE) %>%
    dplyr::arrange(tmp) %>%
    tibble::as_tibble()


  fst::write_fst(idt_, file.path(dirs_$dir_store, "_tmp", "tids.fst"), 100)
  fst::write_fst(tabt_, file.path(dirs_$dir_store, "_tmp", "tdata.fst"), 100)


  ncs_ <- tabs_ %>%
    dplyr::group_by(group, col, nc) %>%
    dplyr::summarise(n = dplyr::n(), ids = list(tmp), .groups = "drop")
  nct_ <- dplyr::count(tabt_, group, col, nc)

  group_ <- ncs_ %>%
    # Join with Target Dataframe
    dplyr::inner_join(nct_, by = c("group", "col"), suffix = c("s", "t")) %>%
    # Filter Nchars according to range
    dplyr::filter(nct >= ncs - .range & nct <= ncs + .range) %>%
    dplyr::group_by(group, col, ncs) %>%
    dplyr::summarise(
      ncs1 = ncs[1],
      ncs2 = ncs[1],
      nct1 = min(nct),
      nct2 = max(nct),
      ns = ns[1],
      nt = sum(nt),
      size = as.numeric(ns) * as.numeric(nt),
      ids = list(sort(unique(unlist(ids)))),
      .groups = "drop"
    ) %>%
    dplyr::mutate(check = ceiling(size / .mat_size)) %>%
    dplyr::arrange(-size) %>%
    dplyr::mutate(
      tmp = purrr::map2(
        .x = ids,
        .y = check,
        .f = ~ split(.x, rep(seq_len(.y), each = ceiling(length(.x)/.y))[seq_len(length(.x))])
      )) %>%
    tidyr::unnest(tmp) %>%
    dplyr::mutate(
      id1 = purrr::map_int(tmp, dplyr::first),
      id2 = purrr::map_int(tmp, dplyr::last),
      ns = lengths(tmp),
      size = as.numeric(ns) * as.numeric(nt)
    ) %>%
    dplyr::select(-ids, -tmp, -check, -ncs) %>%
    dplyr::arrange(dplyr::desc(size))

  return(group_)
}




#' Match Data
#'
#' @param .dir
#' Directory to store Tables
#' @param .cols
#' A named character, with the columns names as string you want to match.\cr
#' The vector must be named wit either fuzzy (f) of exact (e).
#' @param .range
#' A range of characters as an integer, e.g. the name of the source dataframe is 5 characters long and the
#' .range argument is 3, then all names in the target dataframe between 2 - 8 characters are used for the matching
#' @param .weights
#' A named vector with weight, if null all columns will be equal weighted
#' @param .max_match
#' Maximum number of matches
#' @param .allow_mult Aloow multiple matches?
#' @param .method
#' c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
#' @param .workers
#' Workers to use
#' @param .mat_size
#' Maximum Matrix Size for Fuzzy Matching
#' @param .verbose
#' Print additional Information? (Default: TRUE)
#'
#' @import data.table
#'
#' @return A dataframe (saved in .dir)
#' @export
match_data <- function(
    .dir, .cols, .range = Inf, .weights = NULL, .max_match = 10, .allow_mult = TRUE,
    .method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex"),
    .workers = floor(future::availableCores() / 4),
    .mat_size = 1e7,
    .verbose = TRUE
) {

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  id_s <- id_t <- id <- val <- val_s <- val_t <- group <- weigth <- sim <- score <- weight <-
    rank_new <- . <- size <- rank_old <- tmp <- tmp_s <- tmp_t <- NULL

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("_debug_vars/debug-match_data.R")

  # Match Arguments -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  method_ <- get_method(.method)

  # checks -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  check_matching_cols(file.path(.dir, "tables", "sorig.fst"), .cols, "s")
  check_matching_cols(file.path(.dir, "tables", "sorig.fst"), .cols, "t")
  check_names(.cols)

  # Get Directories -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
  dirs_ <- get_dirs_and_files(.dir, .cols, .range, .max_match, .method = method_)

  if (file.exists(dirs_$fil_match)) {
    msg_verbose("Matching already exists", .verbose)
    match_ <- fst::read_fst(dirs_$fil_match)
  } else {

    # Make Groups -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    msg_verbose("Transforming tables and retrieving groups", .verbose)
    # .dir_tabs = dir_tabs_; .dir_store = dir_store_; .cols = .cols; .range = .range
    tab_group_ <- make_groups(.dir, .cols, .range, .max_match, method_)
    lst_group_ <- split(tab_group_, seq_len(nrow(tab_group_)))
    lst_group_ <- purrr::set_names(lst_group_, tab_group_$group)

    # .group = group_[[1]]
    # .lst = filter_groups(file.path(dir_store_, "_tmp"), group_[[1]])

    # Start Matching -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
    msg_verbose("Matching source table to target table ...", .verbose)
    future::plan("multisession", workers = .workers)
    tmp_match_ <- furrr::future_map_dfr(
      .x = lst_group_,
      .f = ~ match_group(
        .lst = filter_groups(.dir = file.path(dirs_$dir_store, "_tmp"), .group = .x),
        .max_match = .max_match,
        .method = method_
      ),
      .options = furrr::furrr_options(seed = TRUE, globals = get_globals()),
      .progress = .verbose,
      .id = "group"
    )
    future::plan("default")
    on.exit(future::plan("default"))

    # Read Transformed Data -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
    sdata <- fst::read_fst(file.path(dirs_$dir_tabs, "sdata.fst"))
    tdata <- fst::read_fst(file.path(dirs_$dir_tabs, "tdata.fst"))
    sids  <- fst::read_fst(file.path(dirs_$dir_store, "_tmp", "sids.fst"))
    tids  <- fst::read_fst(file.path(dirs_$dir_store, "_tmp", "tids.fst"))

    # Joining Back IDs -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
    match0_ <- tmp_match_ %>%
      dplyr::arrange(dplyr::desc(sim)) %>%
      dplyr::distinct(col, tmp_s, tmp_t, .keep_all = TRUE) %>%
      dplyr::left_join(sids, by = c("tmp_s" = "tmp"), suffix = c("_s", "_t"), relationship = "many-to-many") %>%
      dplyr::left_join(tids, by = c("tmp_t" = "tmp"), suffix = c("_s", "_t"), relationship = "many-to-many") %>%
      dplyr::select(group, col, id_s, id_t, val_s, val_t, sim) %>%
      tibble::as_tibble()

    # Retrieving Missing Scores -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
    msg_verbose("Adjusting similarity scores", .verbose)
    miss_ <- dplyr::distinct(match0_, id_s, id_t) %>%
      tidyr::expand_grid(dplyr::distinct(match0_, col)) %>%
      dtplyr::lazy_dt() %>%
      dplyr::anti_join(match0_[, c("id_s", "id_t", "col")], by = c("id_s", "id_t", "col")) %>%
      dplyr::left_join(dplyr::select(sdata, id_s = id, col, val_s = val), by = c("id_s", "col")) %>%
      dplyr::left_join(dplyr::select(tdata, id_t = id, col, val_t = val), by = c("id_t", "col")) %>%
      dplyr::filter(!is.na(val_s), !is.na(val_t)) %>%
      tibble::as_tibble() %>%
      dplyr::mutate(sim = stringdist::stringsim(val_s, val_t, method_)) %>%
      dplyr::mutate(group = "_missing_")

    # Combining Initial and Missing Matches -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -
    msg_verbose("Finalizing output ...", .verbose)
    match_ <- dplyr::bind_rows(match0_, miss_) %>%
      dplyr::arrange(dplyr::desc(sim)) %>%
      dplyr::distinct(col, id_s, id_t, .keep_all = TRUE) %>%
      dplyr::select(group, col, id_s, id_t, val_s, val_t, sim) %>%
      tibble::as_tibble()

    # Write Match -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
    fst::write_fst(match_, file.path(dirs_$dir_store, "matches.fst"))
    unlink(file.path(dirs_$dir_store, "_tmp"), force = TRUE)
  }


  # Get weights -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  if (!is.null(.weights)) {
    weight_ <- tibble::tibble(col = names(.weights), weight = .weights) %>%
      dplyr::mutate(weight = weight / sum(weight))
  } else {
    cols_f_ <- .cols[names(.cols) %in% c("f", "fuzzy")]
    weight_ <- tibble::tibble(col = cols_f_) %>%
      dplyr::mutate(weight = 1 / dplyr::n())
  }

  sorig <- fst::read_fst(file.path(dirs_$dir_tabs, "sorig.fst"), columns = c("id", .cols))
  torig <- fst::read_fst(file.path(dirs_$dir_tabs, "torig.fst"), columns = c("id", .cols))

  msg_verbose("Calculating scores", .verbose)
  score_ <- match_ %>%
    dplyr::mutate(sim = dplyr::if_else(is.na(sim), 0, sim)) %>%
    dplyr::left_join(weight_, by = "col") %>%
    dplyr::mutate(score = sim * weight) %>%
    dplyr::group_by(id_s, id_t) %>%
    dplyr::summarise(score = sum(score, na.rm = TRUE), .groups = "drop_last") %>%
    dplyr::arrange(dplyr::desc(score), .by_group = TRUE) %>%
    dplyr::mutate(rank_old = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    tibble::as_tibble()

  if (!.allow_mult) {
    msg_verbose("Adjusting scores", .verbose)
    tmp_ <- score_ %>%
      dplyr::arrange(dplyr::desc(score)) %>%
      dplyr::filter(!duplicated(id_t)) %>%
      dplyr::group_by(id_s) %>%
      dplyr::arrange(dplyr::desc(score), .by_group = TRUE) %>%
      dplyr::mutate(rank_new = dplyr::row_number()) %>%
      dplyr::select(-score) %>%
      tibble::as_tibble() %>%
      dplyr::filter(rank_new <= .max_match)

    msg_verbose("Finalizing output", .verbose)
    out_ <- score_  %>%
      dplyr::inner_join(tmp_, by = c("id_s", "id_t", "rank_old")) %>%
      dplyr::arrange(id_s, rank_new) %>%
      dplyr::left_join(sorig, by = c("id_s" = "id"), suffix = c("_s", "_t")) %>%
      dplyr::left_join(torig, by = c("id_t" = "id"), suffix = c("_s", "_t")) %>%
      tibble::as_tibble()

  } else{
    msg_verbose("Finalizing output", .verbose)
    out_ <- score_  %>%
      dplyr::mutate(rank_new = rank_old) %>%
      dplyr::filter(rank_new <= .max_match) %>%
      dplyr::arrange(id_s, rank_new) %>%
      dplyr::left_join(sorig, by = c("id_s" = "id"), suffix = c("_s", "_t")) %>%
      dplyr::left_join(torig, by = c("id_t" = "id"), suffix = c("_s", "_t")) %>%
      tibble::as_tibble()
  }

  miss_ <- sorig %>%
    dplyr::filter(!id %in% out_$id_s) %>%
    `colnames<-`(paste0(colnames(.), "_s"))

  col_order_ <- c(
    "id_s", "id_t", "score", "rank_old", "rank_new",
    paste0(rep(.cols, each = 2), c("_s", "_t"))
    )

  out_ <- dplyr::bind_rows(out_, miss_)
  out_ <- out_[, col_order_]
  return(out_)

}

