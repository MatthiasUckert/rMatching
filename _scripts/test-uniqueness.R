devtools::load_all(".")
uni_ <- uniqueness_data(
  .matches = readr::read_rds("test-script/test-match.rds"),
  .source = table_source,
  .target = table_target,
  .cols_match = c(f = "name", e = "iso3", f = "city", f = "address"),
  .max_match = 10,
  .method = "osa",
  .mat_size = 1e7
)
readr::write_rds(uni_, "test-script/test-uniquness.rds", "gz")
