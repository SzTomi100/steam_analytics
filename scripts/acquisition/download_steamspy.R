library(jsonlite)
library(dplyr)
library(purrr)
library(here)
library(DBI)
library(RSQLite)

# -----------------------------
# SETTINGS
# -----------------------------
PAGES_PER_RUN <- 40
SLEEP_TIME <- 3

progress_file <- here("data","temp","progress.txt")

# -----------------------------
# DATABASE
# -----------------------------
con <- dbConnect(
  SQLite(),
  here("data","raw","steam.sqlite")
)

# -----------------------------
# TABLE + CONSTRAINTS
# -----------------------------
dbExecute(con,"
CREATE TABLE IF NOT EXISTS steamspy_history (
  appid TEXT,
  snapshot_date TEXT,
  name TEXT,
  developer TEXT,
  publisher TEXT,
  score_rank TEXT,
  positive TEXT,
  negative TEXT,
  userscore TEXT,
  owners TEXT,
  average_forever TEXT,
  average_2weeks TEXT,
  median_forever TEXT,
  median_2weeks TEXT,
  price TEXT,
  initialprice TEXT,
  discount TEXT,
  languages TEXT,
  genre TEXT,
  ccu TEXT,
  tags TEXT,
  UNIQUE(appid, snapshot_date)
)
")

# Indexes (critical for long-term speed)
dbExecute(con,
          "CREATE INDEX IF NOT EXISTS idx_appid
 ON steamspy_history(appid)")

dbExecute(con,
          "CREATE INDEX IF NOT EXISTS idx_date
 ON steamspy_history(snapshot_date)")

# -----------------------------
# LOAD PROGRESS
# -----------------------------
if (file.exists(progress_file)) {
  start_page <- as.integer(readLines(progress_file))
} else {
  start_page <- 0
}

message(paste("Starting from page", start_page))

# -----------------------------
# DOWNLOADER
# -----------------------------
download_page <- function(page){
  
  url <- paste0(
    "https://steamspy.com/api.php?request=all&page=",
    page
  )
  
  txt <- tryCatch(
    paste(readLines(url, warn = FALSE), collapse=""),
    error = function(e) NULL
  )
  
  if (is.null(txt) || !startsWith(txt,"{"))
    return(NULL)
  
  fromJSON(txt, simplifyDataFrame = FALSE)
}

# -----------------------------
# INSERT ONLY IF CHANGED
# -----------------------------
insert_if_changed <- function(df){
  
  today <- as.character(Sys.Date())
  
  for(i in seq_len(nrow(df))){
    
    row <- df[i,]
    
    existing <- dbGetQuery(con, paste0("
      SELECT *
      FROM steamspy_history
      WHERE appid = '", row$appid, "'
      ORDER BY snapshot_date DESC
      LIMIT 1
    "))
    
    # first observation
    if(nrow(existing) == 0){
      
      row$snapshot_date <- today
      
      dbWriteTable(
        con,
        "steamspy_history",
        row,
        append = TRUE,
        row.names = FALSE
      )
      
    } else {
      
      compare_cols <- setdiff(
        names(row),
        "snapshot_date"
      )
      
      old_vals <- existing[1, compare_cols]
      new_vals <- row[, compare_cols]
      
      comparison <- mapply(function(a, b) {
        
        if(is.na(a) && is.na(b)) {
          FALSE
        } else {
          a != b
        }
        
      }, old_vals, new_vals)
      
      changed <- any(comparison, na.rm = TRUE)
      
      if(changed){
        
        row$snapshot_date <- today
        
        dbWriteTable(
          con,
          "steamspy_history",
          row,
          append = TRUE,
          row.names = FALSE
        )
      }
    }
  }
}

# -----------------------------
# MAIN LOOP
# -----------------------------
crawl_finished <- FALSE
last_success <- start_page

for(page in start_page:(start_page + PAGES_PER_RUN - 1)){
  
  message(paste("Downloading page", page))
  
  batch <- download_page(page)
  
  # -------- FINAL PAGE ----------
  if(is.null(batch)){
    
    message("Final page reached.")
    message("Resetting crawler for next run.")
    
    writeLines("0", progress_file)
    
    crawl_finished <- TRUE
    break
  }
  
  if(length(batch) == 0){
    
    message("All pages completed.")
    message("Resetting crawler for next run.")
    
    writeLines("0", progress_file)
    
    crawl_finished <- TRUE
    break
  }
  
  batch_df <- map_dfr(batch,function(x){
    
    x <- lapply(x,function(v)
      if(is.null(v) || length(v)==0)
        NA_character_
      else
        as.character(v[[1]])
    )
    
    as.data.frame(x)
  })
  
  insert_if_changed(batch_df)
  
  last_success <- page + 1
  
  Sys.sleep(SLEEP_TIME)
}

# -----------------------------
# SAVE PROGRESS (ONLY IF NOT FINISHED)
# -----------------------------
if(!crawl_finished){
  writeLines(as.character(last_success), progress_file)
}

dbDisconnect(con)

message(paste("Next start page:",
              ifelse(crawl_finished,0,last_success)))