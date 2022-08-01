
# This is the main script to go from NAPS, temp, morb and mort raw data to a 
# a list of CD with all variables required to run air pollutant exposure and 
# health models

# Scripts used are a combination of those obtained from Wesley Burr (catnaps, 
# old_working_ahi) and those written myself. I tried my best to indicate where 
# scripts/functions were obtained from and what (if any) modifications were made
library(tidyr)
library(dplyr)
library(chron) 
library(AHItools)
library(readxl)

#--------------------------------------------------------------------------------
# Setup our parameters - may need to modify these! 
      # may also need to update metadata bae on desired stations 
#--------------------------------------------------------------------------------
# Date range 
start_year <- 1980
end_year <- 2019 # 2020 data not available as of April 2022

# All possible pollutants: c("PM10", "O3", "NO2", "NO", "SO2", "PM25", "CO", "NOX") 
types <- c("O3", "NO2", "SO2", "PM25", "PM10")

# location of the InpterPaper directory
dir <- "~/InterpPaper/"


#-----------------------------------------------
# setup our working directories 
#-----------------------------------------------
wd <- list()
wd$home   <- dir
wd$metadata <- paste0(dir,"metadata/")
wd$getAP   <- paste0(dir,"getAP/")
wd$getAPInterp   <- paste0(dir,"getAPInterp/")
wd$getCatnaps   <- paste0(dir,"getCatnaps/")
wd$Outputs   <- paste0(dir,"Outputs/")


all_cd <- read_excel(paste0(wd$metadata, "List of 53 selected  CDs_2021.xlsx"))$cduid

all_dates <- seq(chron(paste0("01/01/", start_year)), 
                 chron(paste0("12/31/", end_year)))

#--------------------------------------------------------------------------------
# Start by downloading and interpolating the NAPS data 
#--------------------------------------------------------------------------------


# Download and organize the NAPS data into pollutant specific csv files 
# takes ~ 3 hrs 
#--------------------------------------------------------------------------------
# will prompt user to confirm the files should be downloaded
source(paste0(wd$getAP, "getAP.R"))



# Run catnaps 
# takes ~ 2 days to run without parallelization (yikes!) - has been updated! 
#--------------------------------------------------------------------------------
# note: metadata files located in wd$getCatnaps/Meta and may need to be updated
# if new stations are added. Last update: November 2021

# organizes NAPS from wd$getAP/OutputData to be in a format for catnaps (wd$getCatnaps/NAPS)
source(paste0(wd$getCatnaps ,"0-getNAPS.R"))

# creates the catnaps db from the wd$getCatnaps/NAPS pollutant directories 
source(paste0(wd$getCatnaps ,"1-initialize.R"))

# transform the catnaps output (wd$getCatnaps/Hourly_Out) to csv files in same dir
# replaces negative interpolate values with zeros 
source(paste0(wd$getCatnaps ,"2-buildRDa.R"))



# Create the .rda object of data for each census division 
# Interpolate daily data
# takes ~ 1 day
# modified for parallel computation! 
#--------------------------------------------------------------------------------
source(paste0(wd$getAPInterp, "0-organizeCatnaps.R"))

source(paste0(wd$getAPInterp, "1-interpolate.R"))

# now we have the NAPS data organized in 2 nice df 





# Now we can put everything together 


#--------------------------------------------------------------------------------
# Combine the interp and non-interp to create the databse by CD 
#--------------------------------------------------------------------------------

# first need to combine the interpolated and non-interpolated ap data 
load(paste0(wd$getAPInterp, "ap.rda"))
load(paste0(wd$getAPInterp, "interpCheckGap/interpolated_all.rda"))


non_interp_df <- ldply(ap, data.frame) %>%
  dplyr::rename(cd = .id) %>%
  dplyr::mutate(cd = as.double(cd)) %>%
  dplyr::filter(cd %in% all_cd)

interpolated <- ldply(interpolated, data.frame) %>%
  dplyr::rename(cd = .id) %>%
  dplyr::mutate(cd = as.double(cd)) %>%
  dplyr::filter(cd %in% all_cd)  %>% 
  mutate_each(funs(replace(., .<0, NA))) %>% 
  dplyr::mutate_at(vars(starts_with("k")), ~ifelse(. <0, 0, .))

interpolated[,3:20] <-as.data.frame(sapply(interpolated[,3:20], function(x) ifelse (x< 0, 0,x)))

completed_ap <- merge(interpolated, non_interp_df, 
                      by = intersect(names(interpolated), names(non_interp_df))) %>% 
  complete(cd = all_cd) %>% 
  dplyr::group_by(cd) %>% 
  complete(Date = seq.Date(as.Date(min(all_dates)), as.Date(max(all_dates)), by="day")) %>% 
  dplyr::filter(!is.na(Date) & Date >= min(all_dates)) %>%
  dplyr::mutate(Year = year(Date), Month = month(Date), Day = day(Date),
                dow = day.of.week(Month, Day, Year), 
                season = ifelse( Month %in% 4:9, "warm", "cold")) %>% 
  dplyr::select(-c(Yr, M, D)) %>% 
  dplyr::relocate(all_of(c("Year", "Month", "Day", "dow", "season")), .after = Date)


AHIdat_ap <- lapply(split(completed_ap, completed_ap$cd), function(df) { df$cd <- NULL; df })
save(AHIdat_ap, file = paste0(wd$Outputs, "AHIdat_ap.rda"))








