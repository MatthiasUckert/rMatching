.source <- table_source
.target <- table_target
.cols_match <- c(fuzzy = "name", fuzzy = "iso3", fuzzy = "city", fuzzy = "address")
.max_match <- 10
.method <- "osa"
.workers = floor(future::availableCores() / 4)
.verbose = TRUE
.mat_size = 1e6
