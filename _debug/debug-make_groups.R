# .source <- prep_table(table_source, c(f = "name", e = "iso3", f = "city", f = "address"))
# .source <- .source$tab
#
# .target <- prep_table(table_target, c(f = "name", e = "iso3", f = "city", f = "address"))
# .target <- .target$tab

.dir   <- "_debug/debug-prep_tables"; dir.create(.dir, FALSE, TRUE)
.range <- Inf
.max   <- 1e6
