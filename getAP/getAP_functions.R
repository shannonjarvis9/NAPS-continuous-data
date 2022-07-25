library(lubridate)
library(janitor)
library(dplyr)
library(WriteXLS)
library(pracma)
library(readxl)
library(gdata)
library(dplyr)
library(tidyr)
library(foreach)
library(doParallel)
registerDoParallel(detectCores() - 1) 


# Function: setup_directories
#-----------------------
# Sets up files within the specified directory 
# Returns the locations of directories if returnWorkingDir =  TRUE
setup_directories <- function(dir = getwd(), returnWorkingDir =  TRUE){
  dir <- gsub("[//]$","", dir) # remove forward slash if at end 
  if(!dir.exists(dir)){stop("You did not specify a valid directory")}
  
  if(!dir.exists(paste0(dir, "/OutputData"))){
    dir.create(paste0(dir, "/OutputData"))
  }
  if(!dir.exists(paste0(dir, "/RawData"))){
    dir.create(paste0(dir, "/RawData"))
  }

  
  if(returnWorkingDir){
    wd <- list()
    wd$home   <- dir
    wd$RawData   <- paste0(dir,"/RawData/")
    wd$OutputData   <- paste0(dir,"/OutputData/")
    return(wd)
  }
}


# Function: download_NAPS
#-----------------------
# Runs bash script to download all desired NAPS file   
download_NAPS <- function(start_year = 1980, end_year = year(Sys.Date()), 
                          pollutants = c("PM10", "O3", "NO2", "NO", "SO2", "PM25", "CO", "NOX"), 
                          scriptDir = getwd(), dataDir = getwd()){
  scriptDir <- gsub("[//]$","", scriptDir) # remove forward slash if at end 
  dataDir <- gsub("[//]$","", dataDir) # remove forward slash if at end 
  
  if(!is.numeric(start_year) | !is.numeric(end_year)){stop("Start and end years must be numeric")}
  if(!dir.exists(dataDir)){stop("You did not specify a valid data directory")}
  if(!dir.exists(scriptDir)){stop("You did not specify a valid script directory")}
  if(start_year > end_year){stop("The start year must be after the end year")}
  if(any(! pollutants %in% c("PM10", "O3", "NO2", "NO", "SO2", "PM25", "CO", "NOX"))){stop("Specified invalid pollutants")}
  
  generate_NAPSdownload_script(start_year, end_year, pollutants, scriptDir)
  
  
  setwd(dataDir)
  system(paste0("bash ", scriptDir, "/download_NAPS_script.sh"))
  
  setwd(scriptDir)
}



# Function: generate_NAPSdownload_script
#-----------------------
# Greates the file_address txt file (location of all url's in the data)
# Generates the bash script which downloads all files 3 times and saves into 3 
#   different directories (Download1,2,3)
generate_NAPSdownload_script <- function(start_year, end_year, pollutants, 
                                         scriptDir = getwd(), dataDir = getwd()){
  
  if(!dir.exists(dataDir)){stop("You did not specify a valid data directory")}
  if(!dir.exists(scriptDir)){stop("You did not specify a valid script directory")}
  
  # create the file address txt file (url of all files to be downloaded) 
  url_wo_year <- c("http://data.ec.gc.ca/data/air/monitor/national-air-pollution-surveillance-naps-program/Data-Donnees/", 
                   "/ContinuousData-DonneesContinu/HourlyData-DonneesHoraires/")
  
  urls <- paste0(url_wo_year[1], start_year:end_year, url_wo_year[2], 
                 paste(rep(pollutants, each = length(start_year:end_year)), start_year:end_year, sep = "_"),".csv")
  
  write.table(urls, file = paste0(scriptDir, "/file_address"), sep = "\n",row.names = FALSE, 
              col.names = FALSE, quote = FALSE)
  

  # create the bash script 
  bash_lines <- data.frame( c1 = c("#!/bin/bash",
                                   paste0("wget -i ", scriptDir, "/file_address")))
  
  write.table(bash_lines, file = paste0(scriptDir, "/download_NAPS_script.sh"), sep = "\n",row.names = FALSE, 
              col.names = FALSE, quote = FALSE)
}


get_NAPS_metadata <- function(dir = getwd(), download = FALSE){
  dir <- gsub("[//]$","", dir) # remove forward slash if at end 
  if(!dir.exists(dir)){stop("You did not specify a valid directory (dir)")}
  
  # download the data 
  if(download){
    system(paste("wget -b -nd https://data.ec.gc.ca/data/air/monitor/national-air-pollution-surveillance-naps-program/ProgramInformation-InformationProgramme/StationsNAPS-StationsSNPA.xlsx"))
  }
  
  # Lets load the data
  NAPS_raw_header <- colnames(read_excel(paste0(dir,"/StationsNAPS-StationsSNPA.xlsx")))
  NAPS_raw_metadata <- read_excel(paste0(dir,"/StationsNAPS-StationsSNPA.xlsx"), 
                                  col_types = c("numeric", "text", "numeric", 
                                                rep("text", 4), rep("numeric", 4),
                                                rep("date",2), rep("text",27), "numeric"),
                                  skip = 1) 
  names(NAPS_raw_metadata) <- make_clean_names(NAPS_raw_header)
  NAPS_raw_metadata
}





# Function: readMyCsv
#-----------------------
# Reads the csv to data frame and checks that it is the correct format/num col
# Input: file_path - path of file to be read to a data frame 
readMyCsv <- function(file_path, pollutant){
  ncol <- min(max(count.fields(file_path, sep = ","), na.rm = TRUE), 32L)
  df <- read.csv(file_path, header = FALSE, na.strings = c("NA","", " ", "-", "-999","-999.000", "-99"), 
                 fileEncoding="latin1" , check.names = FALSE, col.names=paste0('V', seq_len(ncol)),
                 fill=TRUE, quote = "", sep = ",", skipNul = TRUE ) 
  df <- as.data.frame(sapply(df, function(x){gsub("\"", "", x, fixed = TRUE)}))
  # Try to find the header row 
  headerRow <- max(grep("Pollut", df$V1)) 
  
  # Expect there to be 31 rows - some files have an additional 
  # 'Method" column - this needs to be removed 
  if(grepl("Method", df[headerRow,2])){ 
    if(pollutant == "PM25" & length(unique(df[(headerRow + 1):nrow(df),2])) > 1 ){df <- df %>% filter(.[[2]] == 706)}
      df <- select(df, -2)} 
  if(ncol(df) > 31){df <- select(df, -c(32:ncol(df)))}
  
  return(df)
}


## Function: getFile_csv
## -----------------------------------------------------
## Inputs: file_path: path of file to be read
## Output: a data frame (however data frame column NOT appropriately formatted)  
getFile_csv <- function(file_path, pollutant){
  df <- readMyCsv(file_path, pollutant)

  
  # Later years have english name // french name  --> I just want the english part 
  names(df) <- c("Pollutant", "NAPSID", "City", "P/T", "Latitude", "Longitude", 
                 "Date", "H01", "H02", "H03", "H04", "H05", "H06", "H07", "H08", 
                 "H09", "H10", "H11", "H12", "H13", "H14", "H15", "H16", "H17", 
                 "H18", "H19", "H20", "H21", "H22", "H23", "H24")
  
  
  # keep only rows with valid pollutants 
  df <- df[which(df$Pollutant %in% c(pollutant, "PM2.5")), ]
  df$Date <- gsub("-", "", df$Date) #different date formats 
  
  df[, c(8:31)] <- sapply(df[, c(8:31)], as.numeric)
  
  df %>% 
    clean_names()
}


## Function: check_df
## -----------------------------------------------------
# Prev had an issue with the EC website for downloading where file would time 
# out, required that I downloaded each file 3 times, merged and removed the bad lines
# this function was used to check, still in use as its good sanity check 
## Required all these checks to remove the bad obsevations caused by downloading
## issues - if website is fixed, won't need these checks! (or as many) 
## Inputs: df: data frame to be checked 
##         stations: list of valid station numbers 
##         i: index of file being read from the files vector 
##         year: year of the file being read 
## Output: data frame passed checks & a message if checks fail 
check_df <- function(file_path, df, year, printWarning = FALSE){
  
  NAPS_raw_metadata <- get_NAPS_metadata(wdAP$home)
  
  # Check the dates are valid then convert 
  invalid <- which(is.na(as.Date(as.character(df$date),format = "%Y%m%d")) & 
                     is.na(as.Date(as.character(df$date),format = "%m/%d/%Y")))
  if(length(invalid) != 0){
    if(printWarning){
      print(sprintf("Warning: %i Invalid dates, file %s",length(invalid), file_path))
      print(df[invalid,1:7])
    }
    df <- df[-invalid, ]
  }
  
  df <- df  %>%
    dplyr::mutate(date = as.Date(as.character(date),tryFormats = c( "%Y%m%d", "%m/%d/%Y"))) %>% 
    dplyr::mutate(year = year(date), month = month(date), day = day(date)) %>%
    dplyr::mutate(napsid = as.integer(napsid), latitude = as.numeric(latitude),
           longitude = as.numeric(longitude))
  
  # Remove observations where year doesn't match the file name's year
  if(any(! unique(df$year) %in% year)){
    if(printWarning){
      print(sprintf("Warning: Invalid years removed, file %s", file_path))
    }
    df <- df[which(df$year == year),]
  }
  
  # Check the stations and info are valid
  df <- merge(df, NAPS_raw_metadata %>% 
                rename("napsid" = "naps_id", "lat" = "latitude", "lon" = "longitude") %>% 
                select(c("napsid", "lat", "lon")), by = "napsid", all.x = TRUE)
  invalidStn <- df$napsid[! df$napsid %in% NAPS_raw_metadata$naps_id]
  if(length(invalidStn) != 0){
    if(printWarning){
      print(sprintf("Warning: Invalid station id, file %s, invalid station %s",file_path, unique(invalidStn)))
    }
    df <- df[- which(df$napsid %in% invalidStn),]
  }
  
  # Check the lat and long info are valid - sometines lat/long are misisng decimal (hence /10^5)
  invalidLat <- which(abs(df$latitude - df$lat) <= 0.1 & abs(df$latitude/100000 - df$lat) <= 0.1 )  
  invalidLon <- which(abs(df$longitude - df$lon) <= 0.1 & abs(df$longitude/100000 - df$lon) <= 0.1 ) 
  if(length(invalidLat) != 0 | length(invalidLon) != 0){
    if(printWarning){
      print(sprintf("Warning: Invalid station coordinates removed, file %s, coordinates; %i",
                    file_path, unique(c(df$latitude[invalidLat], df$longitude[invalidLon]))))
    }
    df <- df[-unique(c(invalidLat, invalidLon)),]
  }
  df <- df %>% select(-c("lat", "lon"))
  
  # check the hourly measurements are reasonable - lots of issues with file 
  # should have less than 5 digit value (nchar < 5), max(c(0,x)) in case row is all NA 
  hourly_char <- apply((apply(df[,8:31], MARGIN = 2, nchar)),1, function(x) max(c(0,x), na.rm = TRUE))
  if(any(hourly_char > 5)){
    invalidChar <- which(hourly_char > 5)
    val <- hourly_char[which(hourly_char > 5)]
    if(printWarning){
      print(sprintf("Warning: Invalid mass on row %i removed, file %s,", invalidChar, file_path))
      print(df[invalidChar,8:31])
    }
    df <- df[-invalidChar,]
  }
  
  # Remove duplicate rows 
  if(any(duplicated(df))){
    if(printWarning){
      print(sprintf("Removed duplicated rows, file %s,", file_path))
    }
    df <- df[!duplicated(df), ]
  }
  
  # Remove duplicate days/napsid combination  (of site and date)
  # Remove rows that have the most NA values (or just remove the second observation)
  duplicates <- df[which(duplicated(df[c("date", "napsid")]) | 
                           duplicated(df[c("date", "napsid")], fromLast = TRUE)), ]
  if(nrow(duplicates) != 0){
    toSave <- duplicates %>% dplyr::mutate(numNa = rowSums(is.na(duplicates))) %>%
      group_by(date, napsid) %>% slice(which.min(numNa)) %>% select(-numNa)
    if(printWarning){
      print(sprintf("Warning: %i duplicate observations were removed,", nrow(duplicates) - nrow(toSave)))
    }
    # remove all duplicates, the jsut add the ones to save 
    df <- rbind(anti_join(df, duplicates, by = names(df)), toSave) 
  }
  
  # Check there are 365 or 366 observations per year 
  check_dates <- df %>% group_by(napsid) %>%
    dplyr::summarise(count = n())
  num_days_year <- length(seq.Date(min(df$date), max(df$date), by="day"))
  if(any(check_dates$count != num_days_year)){
    wrong_stn <- which(check_dates$count != num_days_year)
    if(printWarning){
      print(sprintf("Warning: Station %s does not have the expected number of observations, has %i, expect %i, file %s,", 
                    check_dates$napsid[wrong_stn], check_dates$count[wrong_stn], num_days_year, file_path))
    }
  }
  
  
  return(df %>% dplyr::mutate(across(starts_with("h"), as.numeric)))
}




# Function from create_db.R with small edits
#----------------------------------------------------------
# Reads in files in dir and outputs them in a data frame by census 
# division for each station 
# ap   <- get_ap(time_span, assign_CD_ap, dir)
"get_ap" <- function(time_span, assign_CD_ap, dir, sub_dir = "getAPInterp", prefix, file_pattern,
                     replace_neg = FALSE) {
  
  
  cur_dir <- getwd()
  if(cur_dir != dir) {
    old_dir <- cur_dir
  } # otherwise we're currently in dir, so we'll move back to it at the end
  # of this function
  setwd(paste0(dir, "/", sub_dir))

  cd_num <- unique(assign_CD_ap[["cd_code"]])
  
  # get file listing
  allFilesInDir <- grep(file_pattern, list.files(path = ".", pattern = "csv"), value = TRUE)
  N <- length(allFilesInDir)
  
  #ymd <- get_yr_m_d(time_span)
  time_seq <- seq()
  ymd <- data.frame(Yr = as.integer(format(as.Date(time_span), "%Y")),
                    M = as.integer(format(as.Date(time_span), "%m")),
                    D = as.integer(format(as.Date(time_span), "%d"))) #chron::year(time_span)))
  J <- length(time_span)
  

  poll_names <- unlist(lapply(strsplit(allFilesInDir, "_|.csv"), FUN = function(x) { x[[1]] }))
  CD_level <- vector("list", N)
  CD_level <- lapply(CD_level, FUN = function(x) { 
    x <- vector("list", length(cd_num)) 
    y <- lapply(x, FUN = function(z) { 
      z <- as.data.frame(matrix(NA, nrow = J, ncol = 50))
      z[, 1:4] <- cbind(time_span, ymd$Yr, ymd$M, ymd$D)
      names(z)[1:4] <- c("Date", "Yr", "M", "D")
      z$Date <- as.Date(z$Date, origin = "1970-01-01")
      z
    } ) 
  })
  # names are assigned as cd_num, in whatever order that is; everything works off this
  CD_level <- lapply(CD_level, FUN = function(x) { names(x) <- cd_num; x })
  CD_level_8hmx <- CD_level
  
  # Cycle through the pollutants; N in total
  for (j in 1:N) {
    cat(paste0("Processing ", allFilesInDir[j], "... \n"))
    raw_in <- read.csv(file = allFilesInDir[j], header = TRUE, stringsAsFactors = FALSE, 
                       sep = ",") 
    
    pollutant_name <- strsplit(allFilesInDir[j], split = "_|.csv")[[1]][[1]]
    stations <- unique(raw_in[["naps_id"]])
    M <- length(stations)
    raw_stat <- vector("list", M)
    
    cat("Splitting stations, replacing -999s:\n")
    pb = txtProgressBar(min = 0, max = 100, initial = 0, style = 3)
    
    for(k in 1:M) {
      tmp <- raw_in[raw_in[, "naps_id"] == stations[k], ] 
      tmp[tmp <= -999] <- NA
      if(replace_neg){
        tmp[tmp < 0] <- 0
      }
      raw_stat[[k]] <- tmp
      setTxtProgressBar(pb, k / M * 100)
    }
    
    cat("\nCompressing hourly records into daily records:\n")
    pb = txtProgressBar(min = 0, max = 100, initial = 0, style = 3)
    
    stat_hr <- stat_8hr <- vector("list", M)
    if(tolower(poll_names[j]) != "o3") {
      for(k in 1:M) {
        stat_hr[[k]] <- compress_station(raw_stat[[k]])[, c("year", "month", "day", "dly")]  
        setTxtProgressBar(pb, k / M * 100)
      } 
    } else {
      for(k in 1:M) {
        stat_hr[[k]] <- compress_station(raw_stat[[k]])[, c("year", "month", "day", "dly")] 
        stat_8hr[[k]] <- compress_station_8hmx(raw_stat[[k]])[, c("year", "month", "day", "dly")]
        setTxtProgressBar(pb, k / M * 100)
      } 
    }
    names(stat_hr) <- names(stat_8hr) <- stations
    
    # possibly an empty record (or more than one) at the end of the individual stations
    if(tolower(poll_names[j]) == "o3") {
      stat_8hr <- lapply(stat_8hr, FUN = function(x) { x[which(!is.na(x[, "year"])), ] })
    }
    
    #
    #  Combine stations into CD-level, stat_hr only
    #
    assign_CD_ap_filter <- assign_CD_ap %>% filter(napsid %in% stations)
    
    cat("\nCombining stations into CD-level metrics:\n")
    pb = txtProgressBar(min = 0, max = 100, initial = 0, style = 3)
    
    for(k in 1:length(cd_num)) {
      ## grab station IDs
      stat_in_cd <- pull(assign_CD_ap_filter[assign_CD_ap_filter$cd_code == cd_num[k], ], "napsid")
      
      if(length(stat_in_cd) > 0) {
        
        # first empty column; CD_level[[j]][[k]] is empty of data at this point, so start there
        idx <- min(which( apply(CD_level[[j]][[k]], MAR = 2, FUN = function(x) { all(is.na(x)) } ) ))
        
        for(m in 1:length(stat_in_cd)) {
          sub_dat <- stat_hr[[as.character(stat_in_cd[m])]]
          sub_dat <- sub_dat %>% group_by(year, month, day) %>% slice(which.max(!is.na(dly)))
          # convert the dates to chron in order to merge
          chron_dates <- chron(paste(sub_dat[["month"]], sub_dat[["day"]], sub_dat[["year"]], sep = "/"))
          CD_level[[j]][[k]][which(CD_level[[j]][[k]][, "Date"] %in% chron_dates), idx] <- sub_dat[which(chron_dates %in% CD_level[[j]][[k]][, "Date"]), "dly"]
          names(CD_level[[j]][[k]])[idx] <- as.character(stat_in_cd[m])
          idx <- idx + 1
        }
      } # only do this if there are actual stations
      setTxtProgressBar(pb, k / length(cd_num) * 100)
    }
    cat("\n")
    # CD_level[[j]][[*]] now has data
    
    #
    #  Drop the NA columns, add the mean for CD-level
    #
    CD_level[[j]] <- lapply(CD_level[[j]], FUN = function(x) { 
      # drop NA columns and year/etc before applying mean
      idx <- which( apply(x, MAR = 2, FUN = function(y) { all(is.na(y)) } ) )
      sub_dat <- x[, -c(idx, which(names(x) %in% c("Date", "Yr", "M", "D")))]
      
      if(length(dim(sub_dat)) > 1) {
        CD.24hm <- apply(sub_dat, MAR = 1, FUN = mean, na.rm = TRUE)
      } else if(is.vector(sub_dat)) {
        CD.24hm <- sub_dat 
      } else {
        CD.24hm <- rep(NA, dim(x)[1])
      }
      # can get NaNs here if there are no non-NA entries
      CD.24hm[is.nan(CD.24hm)] <- NA
      cbind(x, CD.24hm)
    })
    
    
    if(tolower(poll_names[j]) == "o3") {
      #
      #  Combine stations into CD-level, stat_8hr only
      #
      cat("Combining stations into CD-level metrics (8-hour):\n")
      pb = txtProgressBar(min = 0, max = 100, initial = 0, style = 3)
      
      for(k in 1:length(cd_num)) {
        # grab station IDs
        stat_in_cd <- pull(assign_CD_ap_filter[assign_CD_ap_filter$cd_code == cd_num[k], ], "napsid")
        
        if(length(stat_in_cd) > 0) {
          
          # first empty column
          idx <- min(which( apply(CD_level_8hmx[[j]][[k]], MAR = 2, FUN = function(x) { all(is.na(x)) } ) ))
          
          for(m in 1:length(stat_in_cd)) {
            sub_dat <- stat_8hr[[as.character(stat_in_cd[m])]]
            sub_dat <- sub_dat %>% group_by(year, month, day) %>% slice(which.max(!is.na(dly)))
            # convert the dates to chron in order to merge
            chron_dates <- chron(paste(sub_dat[["month"]], sub_dat[["day"]], sub_dat[["year"]], sep = "/"))
            CD_level_8hmx[[j]][[k]][which(CD_level_8hmx[[j]][[k]][, "Date"] %in% chron_dates), idx] <- sub_dat[which(chron_dates %in% CD_level_8hmx[[j]][[k]][, "Date"]), "dly"]
            names(CD_level_8hmx[[j]][[k]])[idx] <- as.character(stat_in_cd[m])
            idx <- idx + 1
          }
        } # only do this if there are actual stations
        setTxtProgressBar(pb, k / length(cd_num) * 100)
      }
      
      #
      #  Drop the NA columns, add the mean for CD-level
      #
      CD_level_8hmx[[j]] <- lapply(CD_level_8hmx[[j]], FUN = function(x) { 
        idx <- which( apply(x, MAR = 2, FUN = function(y) { all(is.na(y)) } ) )
        sub_dat <- x[, -c(idx, which(names(x) %in% c("Date", "Yr", "M", "D")))]
        if(length(dim(sub_dat)) > 1) {
          CD.24hm <- apply(sub_dat, MAR = 1, FUN = mean, na.rm = TRUE)
        } else if(is.vector(sub_dat)) {
          CD.24hm <- sub_dat 
        } else {
          CD.24hm <- rep(NA, dim(x)[1])
        }
        # can get NaNs here if there are no non-NA entries
        CD.24hm[is.nan(CD.24hm)] <- NA
        cbind(x, CD.24hm)
      })
      cat("\n")
    }
  }  # end of pollutant loop, all raw data read in 
  
  #
  #  Now have CD_level, and maybe CD_level_8hr
  #  * iterate through the N pollutants, and combine them; all data.frames are
  #    the same dimension, so the combination is easy; just need some renames
  ap <- vector("list", length(cd_num))
  
  # iterate through the CDs, combining the metrics into a single data frame
  for(j in 1:length(cd_num)) {
    z <- as.data.frame(matrix(NA, nrow = J, ncol = 100))
    z[, 1:4] <- cbind(time_span, ymd$Yr, ymd$M, ymd$D)
    names(z)[1:4] <- c("Date", "Yr", "M", "D")
    idx <- 5 
    for(k in 1:N) {
      if(tolower(poll_names[k]) == "o3") {
        x1 <- lag_one_two(CD_level[[k]][[j]][, "CD.24hm"])
        x2 <- lag_one_two(CD_level_8hmx[[k]][[j]][, "CD.24hm"])
        z[, idx:(idx+2)] <- x1
        z[, (idx+3):(idx+5)] <- x2
        names(z)[idx:(idx+5)] <- c(paste0(prefix, "O3.24hm.lag0"), paste0(prefix, "O3.24hm.lag1"), paste0(prefix, "O3.24hm.lag2"),
                                   paste0(prefix, "O3.8hmx.lag0"), paste0(prefix, "O3.8hmx.lag1"), paste0(prefix, "O3.8hmx.lag2"))
        idx <- idx + 6
      } else {
        z[, idx:(idx+2)] <- lag_one_two(CD_level[[k]][[j]][, "CD.24hm"])
        names(z)[idx:(idx+2)] <- paste(paste0(prefix, poll_names[k]), 
                                       "24hm", c("lag0", "lag1", "lag2"), sep = ".")
        idx <- idx + 3
      }
    }
    idx <- which( apply(z, MAR = 2, FUN = function(x) { all(is.na(x)) } ) )
    if(!is.null(idx)) {
      ap[[j]] <- z[, -idx]
    } else {
      ap[[j]] <- z[, ] 
    }
  }
  names(ap) <- cd_num
  
  # convert the date columns 
  ap <- lapply(ap, FUN =  function(z) { 
    z$Date <- as.Date(z$Date, origin = "1970-01-01")
    z
  }) 
  
  # final return
  setwd(cur_dir)
  ap
}




# From create_db 
#  chron infuriatingly saves all date objects as character or numeric factors. 
#  This converts them to 100% numeric vectors and returns the results in a 
#  data.frame
#
"get_yr_m_d" <- function(time_span) {
  stopifnot("dates" %in% class(time_span))
  
  # object handed in is a generic date object, but most likely from chron package
  year_conv <- years(time_span)
  Yr <- as.numeric(levels(year_conv)[as.numeric(year_conv)])
  month_conv <- months(time_span); levels(month_conv) <- 1:12
  M <- as.numeric(month_conv)
  Day <- as.numeric(days(time_span))
  date_split <- list(Yr = Yr, M = M, D = Day)
  date_split
}


#
#  Function which takes a time series x and returns x plus its first and second lag
#
"lag_one_two" <- function(x) {
  stopifnot(is.vector(x))
  N <- length(x)
  x.lag1 <- c(NA, x[1:(N-1)])
  x.lag2 <- c(NA, NA, x[1:(N-2)])
  cbind(x, x.lag1, x.lag2)
}

#
#  Take a 24-hour level station, and compress to daily mean
#
"compress_station" <- function(stat) {
  stopifnot(dim(stat)[2] == 29)
  dat <- stat[, 6:29] %>% mutate_if(is.character, as.numeric)
  dly <- rep(NA, dim(stat)[1])
  dat_mn <- cbind(stat[, 1:5], dly)
  is_bad <- apply(dat, MAR = 1, FUN = function(x) { length(which(is.na(x))) > 6 })
  means <- apply(dat, MAR = 1, FUN = function(x) { mean(x, na.rm = TRUE) })
  means[is_bad] <- NA
  dat_mn[, "dly"] <- means
  dat_mn[which(is.nan(dat_mn[, "dly"])), "dly"] <- NA
  dat_mn
}

#
#  Take a 24-hour level station, and compress to daily
#  maximum 8-hour mean. 
#
"compress_station_8hmx" <- function(stat) {
  stopifnot(dim(stat)[2] == 29)
  dat <- stat[, 6:29] %>% mutate_if(is.character, as.numeric)
  dly <- rep(NA, dim(stat)[1])
  dat_mn <- cbind(stat[, 1:5], dly)
  is_bad <- apply(dat, MAR = 1, FUN = function(x) { length(which(is.na(x))) > 6 })
  means <- apply(dat, MAR = 1, FUN = function(x) { 
    mn8 <- rep(NA, 17)
    for(l in 1:17) {
      subDat <- x[l:(l+7)]
      mn8[l] <- if(length(which(is.na(subDat))) <= 2) {
        mean(subDat, na.rm = TRUE) 
      } else { NA }
    }
    if(length(which(is.na(mn8))) < 5) {
      max(mn8, na.rm = TRUE) 
    } else { NA }
  })
  means[is_bad] <- NA
  dat_mn[, "dly"] <- means
  dat_mn[which(is.nan(dat_mn[, "dly"])), "dly"] <- NA
  dat_mn
}
