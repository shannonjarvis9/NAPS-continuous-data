#
#  Take hourly dumped files from catnaps screen+interpolate,
#  and form the large "Branka-style" files needed for the AHI
#  input
#
library("chron")
library(tidyr)
library(dplyr)
library(foreach)
library(doParallel)
registerDoParallel(detectCores() - 1) 


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

setwd(paste0(wd$getCatnaps,"Hourly_Out/"))
allFilesInDir <- list.files(path = ".")
fileTerm <- unlist(lapply(strsplit(allFilesInDir, "\\."), FUN = function(x) { tail(x, n = 1) } ))
allFilesInDir <- allFilesInDir[which(fileTerm == "csv" & substr(allFilesInDir, 1, 3) == "hr_")]
N <- length(allFilesInDir)
polls <- unique(unlist(lapply(strsplit(allFilesInDir, "_"), "[[", 2)))
station_list <- unlist(lapply(strsplit(unique(unlist(lapply(strsplit(allFilesInDir, "_"), "[[", 3))), "\\."), "[[", 1))

dat_out <- vector("list", length(station_list))


pollcode <- c("006", "007", "008", "013", "050", "004", "014" , "005")
names(pollcode) <- c("NO2", "O3", "NO", "NOX", "PM25U", "SO2", "PM10", "CO")


foreach(p = polls) %dopar% {
  dat_out_raw <- vector("list", length(station_list))
  dat_out_catnaps <- vector("list", length(station_list))
  cat(paste0("Pollutant: ", p, " ...\n"))
  idx <- 1
  for(s in station_list) {
    cat(paste0("Station: ", s, " ... \n")) 
    if(file.exists(paste0("hr_", p, "_", s, ".csv"))) { 
      raw_in <- read.csv(file = paste0("hr_", p, "_", s, ".csv"), header = TRUE, 
                         stringsAsFactors = FALSE, sep = ",") %>% 
        mutate(poldata = replace(poldata, which(poldata<0), 0)) %>% 
        mutate(poldata_interp = replace(poldata_interp, which(poldata_interp<0), 0))
      
      pollut_dat_raw <- raw_in %>% 
        pivot_wider(id_cols =  c("year", "month", "day"), names_from = "hour", values_from = "poldata") %>% 
        mutate(pollutant = pollcode[p],
               naps_id = as.numeric(s)) %>% 
        relocate(c(pollutant, naps_id), .before = 1) %>% 
        relocate(c("0"), .after = "day")
      
      pollut_dat_catnaps <- raw_in %>% 
        pivot_wider(id_cols =  c("year", "month", "day"), names_from = "hour", values_from = "poldata_interp") %>% 
        mutate(pollutant = pollcode[p],
               naps_id = as.numeric(s)) %>% 
        relocate(c(pollutant, naps_id), .before = 1) %>% 
        relocate(c("0"), .after = "day")
      
      
      dat_out_raw[[idx]] <- pollut_dat_raw
      dat_out_catnaps[[idx]] <- pollut_dat_catnaps
      idx <- idx + 1
    }
  }
  dat_final_raw <- do.call(rbind, dat_out_raw)
  dat_final_catnaps <- do.call(rbind, dat_out_catnaps)
  
  names(dat_final_raw) <- c("pollutant", "naps_id", "year", "month", "day", paste0("hour0", 1:9),
                            paste0("hr", 10:24))
  write.csv(x = dat_final_raw, file = paste0(wd$getAPInterp, p, "_original.csv"),
            row.names = FALSE)
  
  
  names(dat_final_catnaps) <- c("pollutant", "naps_id", "year", "month", "day", paste0("hour0", 1:9),
                                paste0("hr", 10:24))
  write.csv(x = dat_final_catnaps, file = paste0(wd$getAPInterp, p, "_screen_10hr_int.csv"),
            row.names = FALSE)
}

