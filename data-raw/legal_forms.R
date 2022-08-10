library(tidyverse)
library(stringi)
library(janitor)
library(countrycode)

id_merge <- function(.id, .val) {
  if (any(is.na(.id))) stop(".id MUST not contain NAs", call. = FALSE)
  if (any(is.na(.val))) stop(".val MUST not contain NAs", call. = FALSE)
  if (length(.id) != length(.val)) stop(".id and .val MUST have the same length", call. = FALSE)
  gr <- igraph::graph_from_data_frame(tibble::tibble(id = .id, val = .val))
  as.vector(igraph::components(gr)$membership[.val])
}

url_gle <- "https://raw.githubusercontent.com/Gawaboumga/iso-20275-python/master/iso20275/Cleaned%20-%20with%20additional%20-%20ISO-20275%20-%202021-09-23.csv"

legal_form_gle <- read_csv(url_gle) %>%
  clean_names() %>%
  select(
    id = elf_code,
    iso3 = country_code_iso_3166_1,
    local_full = entity_legal_form_name_local_name,
    local_abbr = abbreviations_local_language,
    trans_full = entity_legal_form_name_transliterated_name_per_iso_01_140_10,
    trans_abbr = abbreviations_transliterated
  ) %>%
  pivot_longer(local_full:trans_abbr, names_to = "type", values_to = "legal_form") %>%
  filter(!is.na(legal_form)) %>%
  mutate(legal_form = stri_split_fixed(legal_form, ";")) %>%
  unnest(legal_form) %>%
  filter(stri_enc_isascii(legal_form)) %>%
  mutate(legal_form = standardize_str(legal_form)) %>%
  filter(nchar(legal_form) > 1) %>%
  distinct(legal_form, id, .keep_all = TRUE) %>%
  mutate(id_new = id_merge(id, paste0(iso3, legal_form))) %>%
  distinct(legal_form, id_new, .keep_all = TRUE) %>%
  mutate(iso3 = countrycode(iso3, "iso2c", "iso3c")) %>%
  group_by(id_new, iso3 = iso3) %>%
  group_by(id_new, iso3) %>%
  arrange(nchar(legal_form)) %>%
  mutate(lfs = first(legal_form)) %>%
  select(id = id_new, iso3, lfo = legal_form, lfs) %>%
  filter(!is.na(iso3)) %>%
  mutate(id = paste0("G", stri_pad_left(id, 4, 0))) %>%
  ungroup()

filter_dups(legal_form_gle, lfo, iso3)

legal_form_ecb <- openxlsx::read.xlsx("data-raw/legal_forms/List_of_legal_forms.xlsx", 3, startRow = 2) %>%
  clean_names() %>%
  select(
    id = legal_form,
    iso3 = country_iso_code,
    local_full = extensive_title_description,
    trans_full = english_name_description,
    trans_abbr = legal_form_acronym_in_the_country_of_origin_if_applicable
  )  %>%
  pivot_longer(local_full:trans_abbr, names_to = "type", values_to = "legal_form") %>%
  filter(!is.na(legal_form)) %>%
  mutate(legal_form = stri_split_fixed(legal_form, "/")) %>%
  unnest(legal_form) %>%
  filter(stri_enc_isascii(legal_form)) %>%
  mutate(legal_form = standardize_str(legal_form)) %>%
  filter(nchar(legal_form) > 1) %>%
  mutate(lfo = legal_form, lfs = legal_form) %>%
  mutate(iso3 = countrycode(iso3, "iso2c", "iso3c")) %>%
  filter(!is.na(iso3)) %>%
  group_by(id) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  mutate(lfs = first(lfs)) %>%
  ungroup() %>%
  select(id, iso3 = iso3, lfo, lfs) %>%
  distinct() %>%
  mutate(
    id_new = id_merge(id, paste0(iso3, lfo)),
    id_new = paste0("E", stringi::stri_pad_left(id_new, 4, 0))
  ) %>%
  group_by(id_new, iso3, lfo) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  select(id = id_new, iso3, lfo, lfs) %>%
  distinct(lfo, .keep_all = TRUE) %>%
  ungroup()

filter_dups(legal_form_ecb, lfo, iso3)

legal_form_all <- bind_rows(legal_form_ecb, legal_form_gle) %>%
  dplyr::distinct(iso3, lfo, lfs, .keep_all = TRUE) %>%
  dplyr::arrange(iso3, id) %>%
  ungroup()
dups <- filter_dups(legal_form_all, lfo, iso3) %>%
  filter(startsWith(id, "E"))
legal_form_all <- filter(legal_form_all, !id %in% dups$id)

usethis::use_data(legal_form_all, overwrite = TRUE)
usethis::use_data(legal_form_ecb, overwrite = TRUE)
usethis::use_data(legal_form_gle, overwrite = TRUE)

filter_dups(legal_form_ecb, lfo, iso3)
filter_dups(legal_form_gle, lfo, iso3)
filter_dups(legal_form_all, lfo, iso3)
#
#
# tab_ <- RFgen::filter_duplicates(legal_form_gleif, lfo, iso3) %>%
#   arrange(lfo, iso3)
#
#
# tab_ <- filter(legal_form_gleif, id %in% tab_$id) %>%
#   mutate(lfo = legal_form, lfs = legal_form) %>%
#   mutate(iso3 = countrycode(iso3, "iso2c", "iso3c")) %>%
#   filter(!is.na(iso3)) %>%
#   group_by(id) %>%
#   arrange(nchar(lfs), .by_group = TRUE) %>%
#   mutate(lfs = first(lfs)) %>%
#   ungroup() %>%
#   select(id, iso3 = iso3, lfo, lfs, source) %>%
#   distinct() %>%
#   mutate(
#     id_new = id_merge(id, paste0(iso3, lfo)),
#     id_new = paste0(iso3, stringi::stri_pad_left(id_new, 5, 0))
#   ) %>%
#   group_by(id_new, iso3, lfo) %>%
#   arrange(nchar(lfs), .by_group = TRUE) %>%
#   summarise(
#     source = paste(sort(unique(source)), collapse = "; "),
#     ids = paste(sort(unique(id)), collapse = "; "),
#     lfs = first(lfs),
#     .groups = "drop_last"
#   ) %>%
#   arrange(nchar(lfs), .by_group = TRUE) %>%
#   mutate(lfs = first(lfs)) %>%
#   ungroup() %>%
#   select(id = id_new, iso3, lfo, lfs, source, ids)
#
# tab_ <- RFgen::filter_duplicates(legal_form_gleif, lfo, iso3) %>%
#   arrange(lfo, iso3)
