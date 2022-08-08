.matches <- readr::read_rds("test-script/test-match.rds")
.uniqueness <- readr::read_rds("test-script/test-uniquness.rds")
.source <- table_source
.target <- table_target
.weights <- list(
  sim = c(name = .7, city = .2, address = .1),
  uni = c(name = .2, city = .6, address = .2)
  )

.scores <- score_data(.matches, .uniqueness, .weights)
.mult_match <- TRUE
