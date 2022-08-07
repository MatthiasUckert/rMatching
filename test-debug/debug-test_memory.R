.source <- dplyr::bind_rows(table_source, dplyr::mutate(table_source[1:100, ], id = paste0(id, "-1")))
.target <- table_target
.cols_match <- c(a = "name", e = "iso3", a = "city", a = "address")
.max_match <- 10
.method <- "osa"
.workers <- 5
.powers = 5
