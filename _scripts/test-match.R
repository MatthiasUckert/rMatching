devtools::load_all(".")
cols_ <- c(f = "name", e = "iso3", f = "city", f = "address")
match_ <- match_data(
  .source = table_source,
  .target = table_target,
  .cols_match = cols_,
  .max_match = 5,
  .method = "osa",
  .verbose = TRUE,
  .mat_size = 1e8,
  .workers = 10,
  .join = TRUE
)
readr::write_rds(match_, "test-script/test-match.rds", "gz")
