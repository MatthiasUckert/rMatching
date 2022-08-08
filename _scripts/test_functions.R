# devtools::load_all(".")
.vec <- table_source$name
.fun <- "rle"
hmatch_split_sort <- function(.vec, .fun = c("rle", "cat")) {
  fun_ <- match.arg(.fun, c("rle", "cat"))
  fun_ <- switch(fun_,
    "cat" = function(.x) stringi::stri_join(unique(stringi::stri_sort(.x)), collapse = ""),
    "rle" = function(.x) rle(stringi::stri_sort(.x))
  )

  lst_ <- purrr::map(stringi::stri_extract_all_regex(.vec, "\\p{L}"), fun_)
  lst_ <- purrr::transpose(lst_)
  tab_ <- tibble::tibble(
    name = .vec,
    len  = lst_$lengths,
    val  = lst_$values
  ) %>%
    tidyr::unnest(c(len, val)) %>%
    tidyr::pivot_wider(names_from = val, values_from = len, values_fill = 0, names_sort = TRUE)
}


a <- t(as.matrix(tab_[, -1]))
b <- coop::cosine(a)
b[lower.tri(b, TRUE)] <- NA
rowMeans(b, TRUE)

lst_ <- hmatch_split_sort()


make_groups <- function(.source, .target, .range = 3, .dir) {
  source("test-debug/debug-make_groups.R")
  es_ <- colnames(.source)[startsWith(colnames(.source), "e")]
  et_ <- colnames(.target)[startsWith(colnames(.target), "e")]

  fs_ <- colnames(.source)[startsWith(colnames(.source), "f")]
  ft_ <- colnames(.target)[startsWith(colnames(.target), "f")]



  lsts_ <- purrr::map(
    .x = purrr::set_names(fs_, fs_),
    .f = ~ .source %>%
      dplyr::select(hash, !!dplyr::sym(.x), !!dplyr::sym(paste0("n", .x)), !!dplyr::sym(es_)) %>%
      dplyr::rename(n := !!dplyr::sym(paste0("n", .x)))
  ) %>%
    purrr::map(
      .x = .,
      .f = ~ {
        tab_ <- dplyr::mutate(.x, group = paste(!!!dplyr::syms(es_), stringi::stri_pad_left(n, 4, 0), sep = "--"))
        split(dplyr::select(tab_, hash, dplyr::starts_with("f")), tab_$group)
      }
    ) %>%
    unlist(., recursive = FALSE)

  lstt_ <- purrr::map(
    .x = purrr::set_names(fs_, fs_),
    .f = ~ .target %>%
      dplyr::select(hash, !!dplyr::sym(.x), !!dplyr::sym(paste0("n", .x)), !!dplyr::sym(es_)) %>%
      dplyr::rename(n := !!dplyr::sym(paste0("n", .x)))
  )

  lst_ <-

    tab_ <-
    lst_ <-

    f1 <- split(lst_$f1, )


  source_ <- .source %>%
    dplyr::group_by(!!!dplyr::syms(es_)) %>%
    tidyr::nest()

}
sort_cat3 <- function(.strings) {

}


.str1 <- "Matthias Uckert"
.str2 <- "Susana Isabel Martinez Guzman"
stringi::stri_compare(.str1, .str2)


tab_ <- bench::mark(
  stringdist::stringsim(.str1, .str2)
)




stringdist::stringsim(, )


a <- bench::mark(
  sort_cat3(rep(table_source$name, 1)),
  sort_cat3(rep(table_source$name, 10)),
  sort_cat3(rep(table_source$name, 100)),
  check = FALSE
)
sort_cat3



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
#' @param .cols
#' A character vector of columns to perform fuzzy matching.
#' @param .mat_size
#' Maximal Matrix Size
#'
#' @return A dataframe or an error
check_split_input <- function(.source, .target, .cols, .mat_size = 1e3) {

  # DEBUG -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  # source("test-debug/debug-check_split_input.R")

  # Assign NULL to Global Variables -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- ---
  nchar_s <- nchar_t <- n_s <- n_t <- size <- mat_size <- miss <- group <- NULL

  # Get Columns -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
  cols_f_ <- .cols[names(.cols) %in% c("fuzzy", "f")]
  cols_e_ <- .cols[names(.cols) %in% c("exact", "e")]
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
