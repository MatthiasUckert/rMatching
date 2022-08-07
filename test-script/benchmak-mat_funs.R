reshape_mat0 <- function(.mat) {

  desc <- dplyr::desc
  .mat %>%
    tibble::as_tibble() %>%
    dtplyr::lazy_dt() %>%
    dplyr::mutate(id = dplyr::row_number(), .before = V1) %>%
    tidyr::pivot_longer(!dplyr::matches("id")) %>%
    dplyr::group_by(id) %>%
    dplyr::slice_max(order_by = value, n = 10) %>%
    dplyr::ungroup() %>%
    dplyr::rename(id_s = id, id_t = name) %>%
    dplyr::mutate(id_t = as.integer(gsub("V", "", id_t, fixed = TRUE))) %>%
    tibble::as_tibble() %>%
    suppressWarnings()
}

reshape_mat1 <- function(.mat) {
  desc <- dplyr::desc
  tibble::tibble(
    id_s = rep(seq_len(nrow(.mat)), ncol(.mat)),
    id_t = rep(seq_len(ncol(.mat)), each = nrow(.mat)),
    value = as.numeric(.mat)
  ) %>%
    dtplyr::lazy_dt() %>%
    dplyr::group_by(id_s) %>%
    dplyr::arrange(dplyr::desc(value)) %>%
    dplyr::filter(value >= dplyr::nth(value, n = 10)) %>%
    dplyr::ungroup() %>%
    dplyr::as_tibble()
}
reshape_mat2 <- function(.mat) {
  mat0_ <- .mat
  mat1_ <- Rfast::rowSort(mat0_, descending = TRUE, parallel = TRUE, stable = TRUE)[, 1:10]
  lst0_ <- purrr::map(seq_len(nrow(mat0_)), ~ mat0_[.x, ])
  lst1_ <- purrr::map(seq_len(nrow(mat1_)), ~ mat1_[.x, ])
  idx_  <- purrr::map2(lst0_, lst1_, ~ which(.x %in% .y))
  val_  <- purrr::map2(lst0_, idx_, ~ .x[.y])

  tibble::tibble(
    id_s = rep(seq_len(length(lst0_)), lengths(idx_)),
    id_t = unlist(idx_),
    value = unlist(val_)
  )
}
