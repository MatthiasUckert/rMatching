library(tidyverse)
library(openxlsx)
library(countrycode)
set.seed(123)

table_target <- openxlsx::read.xlsx("data-raw/firms/orbis.xlsx") %>%
  dplyr::distinct(isin, .keep_all = TRUE) %>%
  dplyr::select(-name2) %>%
  dplyr::rename(name = name1) %>%
  dplyr::filter(!is.na(name)) %>%
  dplyr::mutate(iso3 = countrycode::countrycode(iso2, origin = "iso2c", destination = "iso3c")) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(id = digest::digest(paste(isin, 1), "xxhash32")) %>%
  dplyr::ungroup() %>%
  dplyr::select(id, isin, name, iso3, city, address) %>%
  dplyr::distinct(id, .keep_all = TRUE) %>%
  dplyr::mutate(dplyr::across(where(is.character), toupper)) %>%
  dplyr::mutate(iso3 = dplyr::if_else(dplyr::row_number() %in% c(20, 50, 70, 99), NA_character_, iso3)) %>%
  dplyr::mutate(
    revenue = sample(100:400, nrow(.), replace = TRUE),
    size = dplyr::case_when(
      revenue <= 200 ~ "small",
      revenue <= 300 ~ "middle",
      revenue <= 400 ~ "large"
    )) %>%
  dplyr::mutate(iso3 = dplyr::if_else(dplyr::row_number() %in% c(12:24, 1000:1050, 4000:4012), NA_character_, iso3))


table_source <- openxlsx::read.xlsx("data-raw/firms/compustat.xlsx") %>%
  dplyr::distinct(isin, .keep_all = TRUE) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(id = digest::digest(paste(isin, 2), "xxhash32")) %>%
  dplyr::ungroup() %>%
  dplyr::select(id, isin, name, iso3, city, address) %>%
  dplyr::distinct(id, .keep_all = TRUE) %>%
  dplyr::mutate(dplyr::across(where(is.character), toupper)) %>%
  dplyr::mutate(iso3 = dplyr::if_else(dplyr::row_number() %in% c(21, 55, 71:74), NA_character_, iso3)) %>%
  dplyr::left_join(table_target[, c("isin", "revenue", "size")], by = "isin")

table_matches <- dplyr::inner_join(table_source, table_target, by = "isin", suffix = c("_s", "_t")) %>%
  dplyr::select(
    id_s, id_t, name_s, name_t, iso3_s, iso3_t, city_s, city_t, address_s, address_t
  ) %>%
  dplyr::mutate(match = 1) %>%
  dplyr::filter(iso3_s == iso3_t)

table_target <- dplyr::select(table_target, -isin) %>%
  dplyr::select(-revenue)
table_source <- dplyr::select(table_source, -isin) %>%
  dplyr::select(-revenue)

usethis::use_data(table_source, overwrite = TRUE)
usethis::use_data(table_target, overwrite = TRUE)
usethis::use_data(table_matches, overwrite = TRUE)
