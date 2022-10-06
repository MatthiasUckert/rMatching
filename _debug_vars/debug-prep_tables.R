devtools::load_all(".")

# Without Duplicates -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --
.source  = table_source
.target  = table_target
.fstd    = standardize_str
.cols      = c(f = "name|5-5", e = "iso3", e = "size", f = "city|7-7", f = "address|10-10", n = "revenue|50-50")

.dir     = "_debug_data/script"
.range   = 5
.return  = FALSE
.verbose = TRUE


.tab     = table_source
.type    = c("s", "t")
