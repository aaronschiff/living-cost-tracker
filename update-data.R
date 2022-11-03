# Update household living costs data from Stats NZ


# *****************************************************************************
# Setup ----

library(tidyverse)
library(here)
library(rvest)
library(jsonlite)
library(httr)
library(janitor)
library(lubridate)

round_digits <- 2L

download_data <- TRUE
# *****************************************************************************


# *****************************************************************************
# Download data ----

download_price_data <- function() {
  # Get URLs of all CVS files on Stats NZ's datasets page
  # Thanks to Chris Knox for this code
  all_csvs <-
    read_html("https://www.stats.govt.nz/large-datasets/csv-files-for-download/") |>
    html_element(css = "#pageViewData") |>
    html_attr(name = "data-value") |>
    parse_json() |>
    pluck("PageBlocks") |>
    discard(.p = ~ !has_name(., "BlockDocuments")) |>
    map_dfr(.f = function(l) {
      tibble(
        section = l$Title,
        title = l$BlockDocuments |> map_chr(pluck("Title")),
        path = l$BlockDocuments |> map_chr(pluck("DocumentLink"))
      )
    })

  # Identify CSV files to download
  dl_csv <- bind_rows(
    # HLPI
    all_csvs |>
      filter(str_detect(string = title, pattern = "Household living cost price indexes")) |>
      filter(str_detect(string = title, pattern = "time series indexes")) |>
      mutate(filename = "hlpi"),
  ) |>
    mutate(is_zip = str_detect(string = path, pattern = "\\.zip"))

  # Download and save latest CSV files
  for (i in 1:nrow(dl_csv)) {
    g <- GET(url = paste0("https://www.stats.govt.nz", dl_csv[[i, "path"]]))
    if (dl_csv[[i, "is_zip"]]) {
      writeBin(
        object = content(x = g, as = "raw"),
        con = here(paste0("data/", dl_csv[[i, "filename"]], ".zip"))
      )
      uz <- unzip(
        zipfile = here(paste0("data/", dl_csv[[i, "filename"]], ".zip")),
        exdir = "data"
      )
      file.rename(
        from = here(uz[1]),
        to = here(paste0("data/", dl_csv[[i, "filename"]], ".csv"))
      )
      file.remove(here(paste0("data/", dl_csv[[i, "filename"]], ".zip")))
    } else {
      writeBin(
        object = content(x = g, as = "raw"),
        con = here(paste0("data/", dl_csv[[i, "filename"]], ".csv"))
      )
    }
  }

}

# *****************************************************************************


# *****************************************************************************
# Process data ----

# Download data if necessary
if (download_data) {
  download_price_data()
}

# Clean HLPI data
dat_hlpi <- read_csv(
  file = here("data/hlpi.csv"),
  col_types = "ccccccccinn"
) |>
  clean_names() |>
  # Tidy periods
  mutate(frequency = "Q") |>
  separate(col = quarter, into = c("year", "quarter"), sep = "Q", convert = TRUE) |>
  mutate(date = ymd(paste(year, 3L * quarter, "1", sep = "-"))) |>
  # Select relevant data
  select(
    hlpi_name,
    series_ref,
    nzhec_name, level,
    year, quarter, date,
    index
  ) |>
  # Calculate year-on-year changes
  arrange(series_ref, date) |>
  group_by(series_ref) |>
  mutate(yoy_pct_change = index / dplyr::lag(x = index, n = 4L) - 1) |>
  ungroup() |>
  filter(!is.na(yoy_pct_change)) |>
  # Sort out ordering
  mutate(series_order = str_sub(
    string = series_ref,
    start = 10L, end = -1L
  )) |>
  mutate(series_order = ifelse(series_order == "A", "00", series_order)) |>
  mutate(level = factor(
    x = level,
    levels = c("All groups", "group", "subgroup", "class"),
    ordered = TRUE
  )) |>
  mutate(hlpi_name = factor(
    x = hlpi_name,
    levels = c(
      "All households",
      "Beneficiary",
      "Maori",
      "Superannuitant",
      "Income quintile 1 (low)",
      "Income quintile 2",
      "Income quintile 3",
      "Income quintile 4",
      "Income quintile 5 (high)",
      "Expenditure quintile 1 (low)",
      "Expenditure quintile 2",
      "Expenditure quintile 3",
      "Expenditure quintile 4",
      "Expenditure quintile 5 (high)"
    ),
    ordered = TRUE
  )) |>
  # Remove subgroups and classes that are duplicates of their parents
  filter(!(series_order %in% c("04101", "131"))) |>
  # Arrange
  arrange(hlpi_name, series_order, date)

# *****************************************************************************
