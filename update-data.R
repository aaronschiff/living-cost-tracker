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
