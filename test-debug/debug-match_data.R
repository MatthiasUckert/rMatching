.source <- table_source
.target <- table_target
.cols_match <- c(fuzzy = "name", exact = "iso3", fuzzy = "city", fuzzy = "address")
# .source <- fst::read_fst("test-script/source.fst")
# .target <- fst::read_fst("test-script/target.fst")
# .cols_match <- c(fuzzy = "city")

.max_match <- 10
.method <- "osa"
.workers = floor(future::availableCores() / 4)
.verbose = TRUE
.mat_size = 1e8
.join = TRUE
