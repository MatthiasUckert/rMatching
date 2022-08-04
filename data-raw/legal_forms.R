library(tidyverse); library(stringi); library(janitor); library(countrycode)

id_merge <- function(.id, .val) {

  if (any(is.na(.id))) stop(".id MUST not contain NAs", call. = FALSE)
  if (any(is.na(.val))) stop(".val MUST not contain NAs", call. = FALSE)
  if (length(.id) != length(.val)) stop(".id and .val MUST have the same length", call. = FALSE)
  gr <- igraph::graph_from_data_frame(tibble::tibble(id = .id, val = .val))
  as.vector(igraph::components(gr)$membership[.val])

}
legal_form_gleif <- read_csv(
  "https://raw.githubusercontent.com/Gawaboumga/iso-20275-python/master/iso20275/Cleaned%20-%20with%20additional%20-%20ISO-20275%20-%202021-09-23.csv"
  ) %>%
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
  # select(-type) %>%
  filter(!is.na(legal_form)) %>%
  mutate(source = "gleif") %>%
  filter(stri_enc_isascii(legal_form)) %>%
  mutate(legal_form = standardize_str(legal_form)) %>%
  filter(nchar(legal_form) > 1) %>%
  distinct(legal_form, id, .keep_all = TRUE) %>%
  mutate(
    id_new = id_merge(id, paste0(iso3, legal_form))
    ) %>%
  distinct(legal_form, id_new, .keep_all = TRUE) %>%
  mutate(iso3 = countrycode(iso3, "iso2c", "iso3c")) %>%
  group_by(id_new, country_code = iso3) %>%
  # mutate(ids = paste(unique(sort(id)), collapse = "; ")) %>%
  group_by(id_new, country_code) %>%
  arrange(nchar(legal_form)) %>%
  mutate(lfs = first(legal_form)) %>%
  select(id_new, country_code, lfo = legal_form, lfs)

tab_ <- RFgen::filter_duplicates(legal_form_gleif, legal_form, iso3) %>%
  arrange(legal_form, iso3)
tab_ <- filter(legal_form_gleif, id %in% tab_$id)

%>%
  mutate(lfo = legal_form, lfs = legal_form) %>%
  mutate(iso3 = countrycode(iso3, "iso2c", "iso3c")) %>%
  filter(!is.na(iso3)) %>%
  group_by(id) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  mutate(lfs = first(lfs)) %>%
  ungroup() %>%
  select(id, country_code = iso3, lfo, lfs, source) %>%
  distinct() %>%
  mutate(
    id_new = id_merge(id, paste0(country_code, lfo)),
    id_new = paste0(country_code, stringi::stri_pad_left(id_new, 5, 0))
  ) %>%
  group_by(id_new, country_code, lfo) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  summarise(
    source = paste(sort(unique(source)), collapse = "; "),
    ids = paste(sort(unique(id)), collapse = "; "),
    lfs = first(lfs),
    .groups = "drop_last"
  ) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  mutate(lfs = first(lfs)) %>%
  ungroup() %>%
  select(id = id_new, country_code, lfo, lfs, source, ids)

tab_ <- RFgen::filter_duplicates(legal_form_gleif, lfo, country_code) %>%
  arrange(lfo, country_code)

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
  mutate(source = "ecb") %>%
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
  select(id, country_code = iso3, lfo, lfs, source) %>%
  distinct() %>%
  mutate(
    id_new = id_merge(id, paste0(country_code, lfo)),
    id_new = paste0(country_code, stringi::stri_pad_left(id_new, 5, 0))
  ) %>%
  group_by(id_new, country_code, lfo) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  summarise(
    source = paste(sort(unique(source)), collapse = "; "),
    ids = paste(sort(unique(id)), collapse = "; "),
    lfs = first(lfs),
    .groups = "drop"
  ) %>%
  select(id = id_new, country_code, lfo, lfs, source, ids)



legal_form_all <- bind_rows(legal_form_ecb, legal_form_gleif) %>%
  mutate(
    id_new = id_merge(id, paste0(country_code, lfo)),
    id_new = paste0(country_code, stringi::stri_pad_left(id_new, 5, 0))
    ) %>%
  group_by(id_new, country_code, lfo) %>%
  arrange(nchar(lfs), .by_group = TRUE) %>%
  summarise(
    source = paste(sort(unique(source)), collapse = "; "),
    ids = paste(sort(unique(id)), collapse = "; "),
    lfs = first(lfs),
    .groups = "drop"
    ) %>%
  select(id = id_new, country_code, lfo, lfs, source, ids)


usethis::use_data(legal_form_all, overwrite = TRUE)
usethis::use_data(legal_form_ecb, overwrite = TRUE)
usethis::use_data(legal_form_gleif, overwrite = TRUE)

