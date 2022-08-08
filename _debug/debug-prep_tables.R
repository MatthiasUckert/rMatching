# Without Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
.source  <- table_source
.target  <- table_target
.tab     <- table_source
.cols    <- c(f = "name", e = "iso3", f = "city", f = "address")
.fstd    <- standardize_str
.dir     <- "_debug/test"; dir.create(.dir, FALSE, TRUE)
.type    <- c("s", "t")
.range   <- 5


# With Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
# .tab        <- dplyr::bind_rows(table_source, table_source[1:10, ])
# .cols_match <- c(f = "name", e = "iso3", f = "city", f = "address")
# .fun_std    <- standardize_str


# With Duplicates after standardization -- -- -- -- -- -- -- -- -- -- -- -- -- -
# .tab        <- dplyr::bind_rows(table_source, dplyr::mutate(table_source[1:10, ], name = paste0(name, " ")))
# .cols_match <- c(f = "name", e = "iso3", f = "city", f = "address")
# .fun_std    <- standardize_str
