.cols_match <- c(fuzzy = "name", exact = "iso3", fuzzy = "city", fuzzy = "address")
.col <- "name"
.source <- prep_tables(table_source, .cols_match)
.target <- prep_tables(table_target, .cols_match)
.max_match <- 10
.method <- "osa"
.workers <- future::availableCores()
.join = TRUE
