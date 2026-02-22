library(jsonlite)
library(dplyr)
library(here)

message("Starting SteamSpy download...")

# -----------------------------
# Download data
# -----------------------------
url <- "https://steamspy.com/api.php?request=all"

raw_data <- fromJSON(url)

steam_df <- bind_rows(raw_data)

# -----------------------------
# Add snapshot metadata
# -----------------------------
steam_df <- steam_df |>
  mutate(
    snapshot_date = Sys.Date(),
    snapshot_time = Sys.time()
  )

# -----------------------------
# Create filename
# -----------------------------
file_name <- paste0(
  "steamspy_",
  Sys.Date(),
  ".csv.gz"
)

file_path <- here("data", "raw", file_name)

# -----------------------------
# Save compressed snapshot
# -----------------------------
write.csv(
  steam_df,
  gzfile(file_path),
  row.names = FALSE
)

message("Saved snapshot to:")
message(file_path)
