.dir = "_debug/test/"
.max_match = 10
.method = c("osa", "lv", "dl", "hamming", "lcs", "qgram", "cosine", "jaccard", "jw", "soundex")
.workers = floor(future::availableCores() / 4)
.verbose = TRUE
