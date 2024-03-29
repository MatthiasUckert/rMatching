.dir       = "_debug_data/script"
.cols      = c(f = "name", e = "iso3", e = "size", f = "city", f = "address")
# .cols      = c(f = "name")
.weights   = c(name = .7, city = .1, address = .2)
# .weights   = c(name = 1)
.max_match = 10
.range     = 5
.method    = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
.workers   = floor(future::availableCores() / 4)
.verbose   = TRUE
.allow_mult = TRUE
.mat_size = 1e3
