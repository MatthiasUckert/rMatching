
<!-- README.md is generated from README.Rmd. Please edit that file -->

# rMatching

<!-- badges: start -->
<!-- badges: end -->

## Installation

You can install the development version of rMatching from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("MatthiasUckert/rMatching")
```

## Build-In Tables

Below The basic steps how to match two data sets by company names.

``` r

library(rMatching); library(tidyverse); library(tictoc)
```

The package contains three build-in datasets:

- `table_source`: The source table with company names

- `table_target`: The target table with company names

- `table_matches`: A table in which source and target table are already
  matched

### table_source

<table class=" lightable-paper table" style="font-family: &quot;Arial Narrow&quot;, arial, helvetica, sans-serif; margin-left: auto; margin-right: auto; font-size: 10px; margin-left: auto; margin-right: auto;">
<thead>
<tr>
<th style="text-align:left;">
id
</th>
<th style="text-align:left;">
name
</th>
<th style="text-align:left;">
iso3
</th>
<th style="text-align:left;">
city
</th>
<th style="text-align:left;">
address
</th>
<th style="text-align:left;">
size
</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:left;">
291C5CB8
</td>
<td style="text-align:left;">
ASM INTERNATIONAL NV
</td>
<td style="text-align:left;">
NLD
</td>
<td style="text-align:left;">
ALMERE
</td>
<td style="text-align:left;">
VERSTERKERSTRAAT 8
</td>
<td style="text-align:left;">
large
</td>
</tr>
<tr>
<td style="text-align:left;">
097A6454
</td>
<td style="text-align:left;">
TELEFONAKTIEBOLAGET LM ERICS
</td>
<td style="text-align:left;">
SWE
</td>
<td style="text-align:left;">
STOCKHOLM
</td>
<td style="text-align:left;">
TORSHAMNSGATAN 21, KISTA
</td>
<td style="text-align:left;">
small
</td>
</tr>
<tr>
<td style="text-align:left;">
0CA8A1F4
</td>
<td style="text-align:left;">
NOVO NORDISK A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
BAGSVAERD
</td>
<td style="text-align:left;">
NOVO ALLE 1
</td>
<td style="text-align:left;">
small
</td>
</tr>
</tbody>
</table>

### table_target

<table class=" lightable-paper table" style="font-family: &quot;Arial Narrow&quot;, arial, helvetica, sans-serif; margin-left: auto; margin-right: auto; font-size: 10px; margin-left: auto; margin-right: auto;">
<thead>
<tr>
<th style="text-align:left;">
id
</th>
<th style="text-align:left;">
name
</th>
<th style="text-align:left;">
iso3
</th>
<th style="text-align:left;">
city
</th>
<th style="text-align:left;">
address
</th>
<th style="text-align:left;">
size
</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:left;">
40D62BF9
</td>
<td style="text-align:left;">
VOLKSWAGEN AG
</td>
<td style="text-align:left;">
DEU
</td>
<td style="text-align:left;">
WOLFSBURG
</td>
<td style="text-align:left;">
BRIEFFACH 1849
</td>
<td style="text-align:left;">
middle
</td>
</tr>
<tr>
<td style="text-align:left;">
18162F6F
</td>
<td style="text-align:left;">
DAIMLER AG
</td>
<td style="text-align:left;">
DEU
</td>
<td style="text-align:left;">
STUTTGART
</td>
<td style="text-align:left;">
MERCEDESSTRASSE 120
</td>
<td style="text-align:left;">
small
</td>
</tr>
<tr>
<td style="text-align:left;">
47F0DB5C
</td>
<td style="text-align:left;">
BAYERISCHE MOTOREN WERKE AG
</td>
<td style="text-align:left;">
DEU
</td>
<td style="text-align:left;">
MUENCHEN
</td>
<td style="text-align:left;">
PETUELRING 130
</td>
<td style="text-align:left;">
middle
</td>
</tr>
</tbody>
</table>

### table_matches

<table class=" lightable-paper table" style="font-family: &quot;Arial Narrow&quot;, arial, helvetica, sans-serif; margin-left: auto; margin-right: auto; font-size: 10px; margin-left: auto; margin-right: auto;">
<thead>
<tr>
<th style="text-align:left;">
id_s
</th>
<th style="text-align:left;">
id_t
</th>
<th style="text-align:left;">
name_s
</th>
<th style="text-align:left;">
name_t
</th>
<th style="text-align:left;">
iso3_s
</th>
<th style="text-align:left;">
iso3_t
</th>
<th style="text-align:left;">
city_s
</th>
<th style="text-align:left;">
city_t
</th>
<th style="text-align:left;">
address_s
</th>
<th style="text-align:left;">
address_t
</th>
<th style="text-align:right;">
match
</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:left;">
291C5CB8
</td>
<td style="text-align:left;">
1147DBEB
</td>
<td style="text-align:left;">
ASM INTERNATIONAL NV
</td>
<td style="text-align:left;">
ASM INTERNATIONAL NV
</td>
<td style="text-align:left;">
NLD
</td>
<td style="text-align:left;">
NLD
</td>
<td style="text-align:left;">
ALMERE
</td>
<td style="text-align:left;">
ALMERE
</td>
<td style="text-align:left;">
VERSTERKERSTRAAT 8
</td>
<td style="text-align:left;">
VERSTERKERSTRAAT 8
</td>
<td style="text-align:right;">
1
</td>
</tr>
<tr>
<td style="text-align:left;">
0CA8A1F4
</td>
<td style="text-align:left;">
BACB9C1F
</td>
<td style="text-align:left;">
NOVO NORDISK A/S
</td>
<td style="text-align:left;">
NOVO NORDISK A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
BAGSVAERD
</td>
<td style="text-align:left;">
BAGSVAERD
</td>
<td style="text-align:left;">
NOVO ALLE 1
</td>
<td style="text-align:left;">
NOVO ALLE
</td>
<td style="text-align:right;">
1
</td>
</tr>
<tr>
<td style="text-align:left;">
80DC386E
</td>
<td style="text-align:left;">
C201D476
</td>
<td style="text-align:left;">
KONINKLIJKE PHILIPS NV
</td>
<td style="text-align:left;">
KONINKLIJKE PHILIPS N.V.
</td>
<td style="text-align:left;">
NLD
</td>
<td style="text-align:left;">
NLD
</td>
<td style="text-align:left;">
AMSTERDAM
</td>
<td style="text-align:left;">
AMSTERDAM
</td>
<td style="text-align:left;">
PHILIPS CENTER, AMSTELPLEIN 2
</td>
<td style="text-align:left;">
AMSTELPLEIN 2
</td>
<td style="text-align:right;">
1
</td>
</tr>
</tbody>
</table>

# Name Matching Pipeline

## Step 1: Prepare Tables

Tables have to be prepared with the function: **prep_tables()**

**Inputs:**

1.  **`.source`**: The Source Table - This is the table containing the
    names you want to match. These names will be compared to those in
    the target table to identify potential matches.

2.  **`.target`**: The Target Table - This table contains the names to
    which the source table names will be matched. The function aims to
    find corresponding names in the target table for each name in the
    source table.

3.  **`.fstd`**: Standardization Function - This is a user-defined or
    built-in function used to standardize the names in both source and
    target tables. By default, the function uses the built-in
    **`standardize_str`** function for this purpose.

4.  **`.dir`**: Data Storage Directory - This parameter specifies the
    directory where the processed data will be stored, allowing for easy
    access and review of the intermediate data files.

5.  **`.return`**: Return Tables as List - This is a boolean flag that
    determines whether the processed source and target tables should be
    returned as a list. If set to **`TRUE`**, the function will return
    the tables; otherwise, it will only store them in the specified
    directory.

6.  **`.verbose`**: Verbose Output - This boolean flag controls the
    display of additional information during the function’s execution.
    If set to **`TRUE`**, the function will print extra details, which
    can be useful for troubleshooting and understanding the matching
    process.

``` r
prep_tables(
  .source = table_source,
  .target = table_target,
  .fstd = standardize_str,
  .dir = "_debug_data",
  .return = FALSE,
  .verbose = TRUE
)
#> 
#> Preparing Source Table ...
#> 
#> Preparing Target Table ...
#> 
#> Data is stored ...
```

The relevant tables are stored in the directory (.dir)

<table class=" lightable-paper table" style="font-family: &quot;Arial Narrow&quot;, arial, helvetica, sans-serif; margin-left: auto; margin-right: auto; font-size: 10px; margin-left: auto; margin-right: auto;">
<thead>
<tr>
<th style="text-align:left;">
type
</th>
<th style="text-align:left;">
file
</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:left;">
Source Table
</td>
<td style="text-align:left;">
sdata.fst
</td>
</tr>
<tr>
<td style="text-align:left;">
Source Table
</td>
<td style="text-align:left;">
sorig.fst
</td>
</tr>
<tr>
<td style="text-align:left;">
Groups
</td>
<td style="text-align:left;">
tdata.fst
</td>
</tr>
<tr>
<td style="text-align:left;">
Groups
</td>
<td style="text-align:left;">
torig.fst
</td>
</tr>
</tbody>
</table>

## Step 2: Match Data

The Matching can be now performed with the function **match_data()**

Inputs:

1.  **`.dir`** Data Storage Directory - This parameter specifies the
    directory in which the processed data will be stored. It enables
    easy access and review of the intermediate data files created during
    the matching process.

2.  **`.cols`** Named Vector for Columns - This parameter is a named
    vector containing the columns to be considered for matching. The
    names in the vector should be either “e” or “exact” for an exact
    comparison of the strings, or “f” or “fuzzy” for a fuzzy matching
    approach as specified by the .method argument. Names in the vector
    can be either quoted or unquoted, while the values must be quoted.

3.  **`.range`** Character Range - This parameter defines the range of
    characters to be considered when matching names. For example, if a
    name in the source table has 10 characters and .range is set to 5,
    any name within the 5-15 character range in the target table will be
    considered for matching.

4.  **`.weights`** Column Weights - This named vector assigns weights to
    each column used for matching. If not specified, all columns will be
    assigned equal weights by default.

5.  **`.max_match`** Maximum Matches - This parameter determines the
    maximum number of matches to be returned for each record in the
    source table.

6.  **`.allow_mult`** Multiple Match Permission - This boolean parameter
    indicates whether multiple matches are allowed for each record in
    the target table. If you want a 1-1 match, set it to FALSE; for a
    1-n match, set it to TRUE.

7.  **`.method`** Matching Method - This parameter specifies the method
    to be used for matching records. It can be one of the following:
    “osa”, “lv”, “dl”, “hamming”, “lcs”, “qgram”, “cosine”, “jaccard”,
    “jw”, or “soundex”. For more information on these methods, refer to
    the stringdist-metrics documentation in the {stringdist} package.

8.  **`.workers`** Parallel Workers - This parameter defines the number
    of workers to be used for parallelization during the matching
    process.

9.  **`.mat_size`** Maximum Matrix Size - This parameter sets the
    maximum size of the similarity matrix created during the fuzzy
    matching process. If you are dealing with a large number of names to
    match, the matrix can become very large. Adjust this value to avoid
    overloading your system’s memory.

10. **`.verbose`** Verbose Output - This boolean flag determines whether
    additional information is displayed during the execution of the
    function. If set to TRUE, the function will output extra details,
    which can be helpful for troubleshooting and gaining insights into
    the matching process.

``` r
tictoc::tic("Match 1")
```

``` r
match1 <- match_data(
  .dir = "_debug_data",
  .cols = c(f = "name", e = "iso3", f = "city", f = "address", e = "size"),
  .range = 10,
  .weights = c(name = 0.8, city = 0.1, city = 0.1),
  .max_match = 25,
  .allow_mult = FALSE,
  .method = "osa",
  .workers = 4,
  .mat_size = 1e6,
  .verbose = TRUE
)
#> 
#> Transforming tables and retrieving groups
#> 
#> Matching source table to target table ...
#> 
#> Adjusting similarity scores
#> 
#> Finalizing output ...
#> 
#> Calculating scores
#> 
#> Adjusting scores
#> 
#> Finalizing output
```

``` r
tictoc::toc()
#> Match 1: 54.73 sec elapsed
```

The output looks the following:

<table class=" lightable-paper table" style="font-family: &quot;Arial Narrow&quot;, arial, helvetica, sans-serif; margin-left: auto; margin-right: auto; font-size: 10px; margin-left: auto; margin-right: auto;">
<thead>
<tr>
<th style="text-align:left;">
id_s
</th>
<th style="text-align:left;">
id_t
</th>
<th style="text-align:right;">
score
</th>
<th style="text-align:right;">
rank_old
</th>
<th style="text-align:right;">
rank_new
</th>
<th style="text-align:left;">
name_s
</th>
<th style="text-align:left;">
name_t
</th>
<th style="text-align:left;">
iso3_s
</th>
<th style="text-align:left;">
iso3_t
</th>
<th style="text-align:left;">
city_s
</th>
<th style="text-align:left;">
city_t
</th>
<th style="text-align:left;">
address_s
</th>
<th style="text-align:left;">
address_t
</th>
<th style="text-align:left;">
size_s
</th>
<th style="text-align:left;">
size_t
</th>
</tr>
</thead>
<tbody>
<tr>
<td style="text-align:left;">
000F8750
</td>
<td style="text-align:left;">
E48EB751
</td>
<td style="text-align:right;">
1.0000000
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:left;">
NTR HOLDING A/S
</td>
<td style="text-align:left;">
NTR HOLDING A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
COPENHAGEN
</td>
<td style="text-align:left;">
COPENHAGEN
</td>
<td style="text-align:left;">
BREDGADE 30
</td>
<td style="text-align:left;">
SANKT ANNAE PLADS 13 3
</td>
<td style="text-align:left;">
middle
</td>
<td style="text-align:left;">
middle
</td>
</tr>
<tr>
<td style="text-align:left;">
000F8750
</td>
<td style="text-align:left;">
1749518D
</td>
<td style="text-align:right;">
0.6686869
</td>
<td style="text-align:right;">
2
</td>
<td style="text-align:right;">
2
</td>
<td style="text-align:left;">
NTR HOLDING A/S
</td>
<td style="text-align:left;">
NEWCAP HOLDING A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
COPENHAGEN
</td>
<td style="text-align:left;">
KOBENHAVN K
</td>
<td style="text-align:left;">
BREDGADE 30
</td>
<td style="text-align:left;">
BREDGADE 30
</td>
<td style="text-align:left;">
middle
</td>
<td style="text-align:left;">
middle
</td>
</tr>
<tr>
<td style="text-align:left;">
000F8750
</td>
<td style="text-align:left;">
6A2ED2E8
</td>
<td style="text-align:right;">
0.5789474
</td>
<td style="text-align:right;">
4
</td>
<td style="text-align:right;">
3
</td>
<td style="text-align:left;">
NTR HOLDING A/S
</td>
<td style="text-align:left;">
LOYAL SOLUTIONS A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
COPENHAGEN
</td>
<td style="text-align:left;">
COPENHAGEN
</td>
<td style="text-align:left;">
BREDGADE 30
</td>
<td style="text-align:left;">
ROBERT JACOBSENS VEJ 68
</td>
<td style="text-align:left;">
middle
</td>
<td style="text-align:left;">
middle
</td>
</tr>
<tr>
<td style="text-align:left;">
002FCAB5
</td>
<td style="text-align:left;">
FF136D09
</td>
<td style="text-align:right;">
1.0000000
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:left;">
VIROGATES A/S
</td>
<td style="text-align:left;">
VIROGATES A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
BIRKEROD
</td>
<td style="text-align:left;">
BIRKEROD
</td>
<td style="text-align:left;">
BANEVAENGET 13
</td>
<td style="text-align:left;">
BLOKKEN 45
</td>
<td style="text-align:left;">
large
</td>
<td style="text-align:left;">
large
</td>
</tr>
<tr>
<td style="text-align:left;">
002FCAB5
</td>
<td style="text-align:left;">
45C4AE7D
</td>
<td style="text-align:right;">
0.3942308
</td>
<td style="text-align:right;">
3
</td>
<td style="text-align:right;">
2
</td>
<td style="text-align:left;">
VIROGATES A/S
</td>
<td style="text-align:left;">
PHOTOCAT A/S
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
DNK
</td>
<td style="text-align:left;">
BIRKEROD
</td>
<td style="text-align:left;">
ROSKILDE
</td>
<td style="text-align:left;">
BANEVAENGET 13
</td>
<td style="text-align:left;">
LANGEBJERG 4
</td>
<td style="text-align:left;">
large
</td>
<td style="text-align:left;">
large
</td>
</tr>
<tr>
<td style="text-align:left;">
0051857E
</td>
<td style="text-align:left;">
9E944051
</td>
<td style="text-align:right;">
0.7600000
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:right;">
1
</td>
<td style="text-align:left;">
GAUMONT SA
</td>
<td style="text-align:left;">
GAUMONT
</td>
<td style="text-align:left;">
FRA
</td>
<td style="text-align:left;">
FRA
</td>
<td style="text-align:left;">
NEUILLY-SUR-SEINE
</td>
<td style="text-align:left;">
NEUILLY-SUR-SEINE
</td>
<td style="text-align:left;">
30, AVENUE CHARLES DE GAULLE
</td>
<td style="text-align:left;">
30, AVENUE CHARLES DE GAULLE
</td>
<td style="text-align:left;">
middle
</td>
<td style="text-align:left;">
middle
</td>
</tr>
</tbody>
</table>

The output contains the following relevant columns: id_s: The ID of the
source table id_t: The ID of the target table score: similarity score
rank_old: Unadjusted Rank rank_new: Adjusted Rank (only relevant if
.allow_mult = FALSE, than the rank will consider the best match of a
name considering all the other matches)

Note \_t stands for the column in the target table and \_s for the
columns in the source table

The matching is now already stored, so if you just want to change for
example the weights, the calculation will be almost instant.

``` r
tictoc::tic("Match 2")
```

``` r
match2 <- match_data(
  .dir = "_debug_data",
  .cols = c(f = "name", e = "iso3", f = "city", f = "address", e = "size"),
  .range = 10,
  .weights = c(name = 0.5, city = 0.4, city = 0.1), # Changed Weights
  .max_match = 25,
  .allow_mult = FALSE,
  .method = "osa",
  .workers = 4,
  .mat_size = 1e6,
  .verbose = TRUE
)
#> 
#> Matching already exists
#> 
#> Calculating scores
#> 
#> Adjusting scores
#> 
#> Finalizing output
```

``` r
tictoc::toc()
#> Match 2: 0.73 sec elapsed
```

But if you want to change the columns, the function has to cache another
matching

``` r
tictoc::tic("Match 1")
```

``` r
match3 <- match_data(
  .dir = "_debug_data",
  .cols = c(f = "name", e = "iso3", f = "city", f = "address"),
  .range = 10,
  .weights = c(name = 0.5, city = 0.4, city = 0.1),
  .max_match = 25,
  .allow_mult = FALSE,
  .method = "osa",
  .workers = 4,
  .mat_size = 1e6,
  .verbose = TRUE
)
#> 
#> Transforming tables and retrieving groups
#> 
#> Matching source table to target table ...
#> 
#> Adjusting similarity scores
#> 
#> Finalizing output ...
#> 
#> Calculating scores
#> 
#> Adjusting scores
#> 
#> Finalizing output
```

``` r
tictoc::toc()
#> Match 1: 33.19 sec elapsed
```

# Deduplicating Matches

The Outputs of the matching tables is still not unique. We can easily
deduplicate them by setting rank_new ==

``` r
match1_unique <- filter(match1, rank_new == 1)
match2_unique <- filter(match2, rank_new == 1)
match3_unique <- filter(match3, rank_new == 1)
```

we can quickly check that we have unique 1-1 matches with the function
filter_dups() which return s a dataframe with all duplicates

``` r
nrow(filter_dups(match1_unique, id_s, id_t))
#> [1] 0
nrow(filter_dups(match2_unique, id_s, id_t))
#> [1] 0
nrow(filter_dups(match3_unique, id_s, id_t))
#> [1] 0
```

# Matching Comparison

Let us now see how good the matching library performs. We calculated 3
different matches which we stored in match1, match2, and match3.

First we load the pre-stored matches and combine all the matches to a
single table

``` r
.matches <- select(mutate(table_matches, match = 1), id_s, id_t, match)
```

``` r
tab_matches <- bind_rows(
  mutate(match1_unique, no = "1. Match"),
  mutate(match2_unique, no = "2. Match"),
  mutate(match3_unique, no = "3. Match")
)
```

``` r
tab_comparison <- left_join(tab_matches, .matches, by = c("id_s", "id_t")) %>%
  replace_na(list(match = 0)) %>%
  group_by(no) %>%
  summarise(p = sum(match) / n())
```

<img src="man/figures/README-unnamed-chunk-22-1.png" width="100%" />
