.matches <- readr::read_rds("test-script/test-match.rds")
.uniqueness <- readr::read_rds("test-script/test-uniquness.rds")
.uniqueness <- NULL
.weights <- list(
  sim = c(name = .7, city = .2, address = .1)
  )
