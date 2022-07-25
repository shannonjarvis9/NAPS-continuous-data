library(gdata)
library(dplyr)
# This file prepares the NAPS data in the format to be used in the catnaps
# script 

# Reads the raw hourly rda file and outputs a file per pollutant per year 
# to getCatnaps/NAPS/pollutant/year_pollutant.csv 

# Required format for catnaps 


#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
# Format the hourly data 
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
load(paste0(wd$getAP, "OutputData/raw_hourly_dat.rda"))
dirs  <- c("CO", "NO","NO2", "O3", "SO2", "PM10", "PM25", "TEOM", "NOX")

poll_code <- data.frame(pollutant = dirs,
                        pcode = c(005, 008, 006, 007, 004, 014, 050, 015, 013))

hourly_dat <- hourly_df %>% 
  select(starts_with("h"))

hourly_dat[hourly_dat==-999] <- NA
 

hourly_desc <- merge(hourly_df, poll_code, by = "pollutant") %>% 
  select(! starts_with("h")) %>%
  select(-c("city", "p_t", "latitude", "longitude", "date")) %>% 
  dplyr::rename(naps_id = napsid) %>% 
  relocate(pcode, naps_id, year, month, day)  

hourly_df <- cbind(hourly_desc, hourly_dat)


# Need to add summary columns for catnaps 
pm25min <- apply(hourly_df[, 7:dim(hourly_df)[2]], MAR = 1, min, na.rm = TRUE)
pm25min[!is.finite(pm25min)] <- NA
pm25max <- apply(hourly_df[, 7:dim(hourly_df)[2]], MAR = 1, max, na.rm = TRUE)
pm25max[!is.finite(pm25max)] <- NA
pm25mean <- round(apply(hourly_df[, 7:dim(hourly_df)[2]], MAR = 1, mean, na.rm = TRUE))
pm25mean[!is.finite(pm25max)] <- NA

hourly_df$avgD <- pm25mean
hourly_df$minD <- pm25min
hourly_df$maxD <- pm25max

# Reorder columns 
hourly_df <- hourly_df[,c(1:6, 31:33, 7:30)]


setwd(wd$getCatnaps)

if(!dir.exists("NAPS")) { dir.create("NAPS") }
setwd(paste0(wd$getCatnaps, "NAPS"))



cat(paste0("Creating hourly files ...\n "))
for(d in dirs){
  setwd(paste0(wd$getCatnaps, "NAPS"))
  
  if(!dir.exists(paste0(d))) { dir.create(paste0(d))}
  cat(paste0(d, " ...\n "))
  
  if(d %in% unique(hourly_df$pollutant)){
    setwd(paste0(wd$getCatnaps, "NAPS/",d))
    
    poll <- hourly_df %>% filter(pollutant == d) %>% select(-"pollutant")
    df <- split(poll, f = poll$year)
    

    for(i in names(df)){
      write.csv(df[[i]], file = paste0(i,"_", d, ".csv"),col.names=FALSE,  sep="",
                row.names = FALSE)
    }
  }
}

