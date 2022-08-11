# Without Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
.source  = table_source
.target  = table_target
.fstd    = standardize_str
.cols    = c(f = "name", e = "iso3", f = "city", f = "address")

.dir     = "_debug_data0/"
.range   = 5
.return  = FALSE
.verbose = TRUE


.tab     = table_source
.type    = c("s", "t")
