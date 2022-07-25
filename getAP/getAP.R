# This script reads and organizes all of the NAPS data from Environment 
# Canada's data portal 

# Each NAPS file is downloaded 3 times (had issue with downloading of files where data
# was cut off, downloading 3 times should help ensure there is overlap in the files
# and minimal data is missing - so far in pratice it has worked)

# After downloading, the files are read into a data frame, organized and output 
# into pollutant specific csv files 

# Recomended to submit this as a job as file downloading takes a LONG time 

source(paste0(wd$getAP, "getAP_functions.R"))


#--------------------------------------------------------------------------------
# Setup our parameters
#--------------------------------------------------------------------------------
# parameters specified in createAHIDat.R (start year, end year, pollutants)


wdAP <- setup_directories(wd$getAP)




#--------------------------------------------------------------------------------
# Generate the bash script and download NAPS files 
#--------------------------------------------------------------------------------
NAPS_raw_metadata <- get_NAPS_metadata(wdAP$home)


# will prompt user if they want the files downloaded 
download_NAPS(start_year, end_year, pollutants = types, 
              scriptDir = wdAP$home, dataDir = wdAP$RawData)



#--------------------------------------------------------------------------------
# Read and organize the files 
#--------------------------------------------------------------------------------

# Get valid stations/lat and long values from the NAPS metadata 
stations <- unique(NAPS_raw_metadata$naps_id)
latitude <- c(min(unique(NAPS_raw_metadata$latitude), na.rm = TRUE), 
              max(unique(NAPS_raw_metadata$latitude), na.rm = TRUE))
longitude <- c(min(unique(NAPS_raw_metadata$longitude), na.rm = TRUE), 
               max(unique(NAPS_raw_metadata$longitude), na.rm = TRUE))



files <- list.files(paste0(wdAP$RawData), full.names = FALSE, pattern = ".csv")
file_years <- unlist(lapply(strsplit(files, "_|\\."), "[[", 2)) 
file_pollut <- unlist(lapply(strsplit(files, "_|\\.|/"), "[[", 1)) 


# Initialize the data frame 
tmp <- getFile_csv(paste0(wdAP$RawData, files[1]),
                   file_pollut[1])
hourly_df <- check_df(files[1], tmp, file_years[1]) 


cat("Reading raw data files\n")
pb <- txtProgressBar(min = 0, max =  length(files), initial = 0, style = 3)

# Iterate through the files, adding to the data frame 
for(i in 2:length(files)){
  
  tmp <- getFile_csv(paste0(wdAP$RawData, files[i]),
                     file_pollut[i])
  valid <- check_df(files[i], tmp, file_years[i], printWarning = TRUE)
  hourly_df <- rbind(hourly_df,valid)
  
  setTxtProgressBar(pb, i)
  
}

close(pb)


hourly_df[which(hourly_df$pollutant == "PM2.5"),"pollutant"] <- "PM25"
save(file = paste0(wdAP$OutputData, "raw_hourly_dat_with_neg.rda"), hourly_df)

hourly_df <- hourly_df %>% 
  dplyr::mutate_at(vars(starts_with("h")), ~ifelse(. <0, NA, .))

save(file = paste0(wdAP$OutputData, "raw_hourly_dat.rda"), hourly_df)




#--------------------------------------------------------------------------------
# Create the data frames/ csv files 
#--------------------------------------------------------------------------------

for(i in unique(file_pollut)){
  write.csv(hourly_df %>% filter(pollutant == i)  %>% 
              select(-c('p_t', 'latitude', 'longitude', 'city', 'date')) %>% 
              relocate(all_of(c("year", "month", "day")), .before = h01),
            row.names = FALSE,
            file = paste0(wdAP$OutputData, i, ".csv"),
            quote = FALSE)
}





