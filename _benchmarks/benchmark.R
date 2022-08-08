# devtools::load_all(".")

f1 <- function(.vec1, .vec2) {
  stringdist::stringsimmatrix(.vec1, .vec2)
}

f2 <- function(.vec1, .vec2) {
  purrr::reduce(purrr::map(.vec1, ~ stringdist::stringsimmatrix(.x, .vec2)), rbind)
}

.bench <- bench::press(
  len = seq(50, 200, 50),
  {
    dat <- table_source$name[seq_len(len)]
    bench::mark(
      f1(dat, table_target$name[1:1000]),
      f2(dat, table_target$name[1:1000])
    )
  }
)
bench_ <- dplyr::select(.bench, expression:total_time)
