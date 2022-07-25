#
#  Script to read in raw data, and run the usual CATNAPS
#  screening and interpolation procedure on it.
#
#  Author: Wesley S. Burr <wesley.burr@gmail.com>
#  Last Modified: Jun 16, 2022
#  Based on code written by Dave Riegert and Aaron Springford
#
#

###############################################################################
library(catnaps)
library(lubridate)
library(AHItools)
library(readxl)
library(dplyr)
library(foreach)
library(doParallel)
registerDoParallel(detectCores() - 1) 
 
# Get the naps_id to cd assignment 
AHTI_CD_napsid <- read_excel(paste0(wd$metadata,"AHTI CD napsid.xlsx"), col_types = rep("numeric",6))
unique(AHTI_CD_napsid$CDUID)
assign_to_cd <- vector("list", length = ncol(AHTI_CD_napsid)-1)
names(assign_to_cd) <- c("O3", "NO2", "SO2", "PM25", "PM10")

for(i in 1:length(assign_to_cd)){
  assign_to_cd[[i]] <- AHTI_CD_napsid %>% select(c(names(AHTI_CD_napsid)[1], names(AHTI_CD_napsid)[i+1]))
  names(assign_to_cd[[i]] ) <- c("cd_code", "napsid")
  assign_to_cd[[i]] <- assign_to_cd[[i]]  %>% filter(!is.na(napsid))
}

assign_to_cd[["PM25U"]] <- assign_to_cd[["PM25"]]





# source the modified functions 
source("~/catnaps_package/R/manageDB_shannonEdit.R")
setwd(wd$getCatnaps)



#
#  Initialize database, read in raw data
#

# types, start_year and end_year were initialized in 1-createAHIDat.R

dbFile <- "catnaps_v1.db"
if(file.exists(dbFile)) { file.remove(dbFile) }
initializeTable_date_mod(dbFile = dbFile, startYear = start_year, endYear = end_year)
initializeTable_naps_meta_mod(NAPSMetaFile = paste0(wd$metadata, "naps_metatdata_updated_2021.xls"), 
                          dbFile = dbFile, cdAssign = do.call("rbind", assign_to_cd))
initializeTable_pollutant_meta_mod(paste0(wd$metadata, "NAPS-pollutant-codesV2014.xlsx"), dbFile)
initialize_all_helper(dbFile)
initializeDB("./NAPS", dbFile = dbFile, startYear = start_year, endYear = end_year)


CDs <- read_excel(paste0(wd$metadata, "List of 53 selected  CDs_2021.xlsx"))$cduid

startDate <- as.POSIXct(paste0(start_year - 1, "-12-31 01:00"), tz = "UTC", origin = "1970-01-01 00:00 UTC")
endDate   <- as.POSIXct(paste0(end_year + 1, "-01-01 05:00"), tz = "UTC", origin = "1970-01-01 00:00 UTC")

napsListFNs <- paste("napsList-",CDs,".RData",sep="")

################################################################################
#
#  Identify ("Missing Flag") missing values; then create the 
#  NA-sequence that corresponds to it, using the actual data together
#  with the flags. 
#
#  Loop over Census Divisions and the pollutants O3, NO2, PM25_adj, PM25_unadj
#
foreach(cd = CDs) %dopar% {
  cat(paste0(cd, " ...\n "))
  # List all ozone and NO2 stations applicable
  myNapsList <- napsListDB(unlist( unique(subset(do.call("rbind", assign_to_cd), cd_code %in% cd, napsid))),
                              startDate, endDate, dbFile, pollutants = types)
  # Loop over the various stations, by name 
  for(i in names(myNapsList)) {
    # falls through to the default; flags missing values
    myNapsList[[i]] <- flagValue(myNapsList[[i]])
   # 
    for(p in names(myNapsList[[i]])) { 
      #  take the 'Missing Flag' created by the default flagValue(), combine
      #  it with the original time series, and create a new time series with
      #  inserted NAs, which we call 'valuesNA'
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       flagToNA(myNapsList[[i]]$data[[p]]$value,
                                       myNapsList[[i]]$data[[p]]$stats$'Missing Flag',
                                       newName = "valuesNA")
                                      )
    }
  }
  save(myNapsList, file = napsListFNs[match(cd, CDs)])
}




################################################################################
#
#  now have a set of 24 RData files, each napsList-XXXX.RData, where 
#  XXXX is the Census Division code. Each file will load as object
#  'myNapsList' in memory.
#
#  Loop on the CDs again, then the stations, then the 4 pollutants, and 
#  compute a standard set of pollutantStat's (including 10+ zeroes, etc.)
#

foreach(cd = CDs) %dopar% {
  rm("myNapsList")
  load(paste0("napsList-", cd, ".RData"))
  cat(paste0(cd, " ... \n"))
  # Loop on the stations
  for(i in names(myNapsList)) {
    cat(paste0(i, " - "))
    # and then loop on the pollutants
    for(p in names(myNapsList[[i]])) {
      cat(paste0(p, " .... "))
      myNapsList[[i]]$data[[p]] <- flagStat(myNapsList[[i]]$data[[p]], 24, 0.5, minNoInf, "valuesNA", "Daily Min")
      myNapsList[[i]]$data[[p]] <- flagStat(myNapsList[[i]]$data[[p]], 24, 0,   maxNoInf, "valuesNA", "Daily Max")
      myNapsList[[i]]$data[[p]] <- flagStat(myNapsList[[i]]$data[[p]], 24, 0.5, mean,     "valuesNA", "Daily Mean", na.rm = TRUE)
      myNapsList[[i]]$data[[p]] <- flagStat(myNapsList[[i]]$data[[p]], 24, 0,   res,      "valuesNA", "Resolution")
    }
    cat("\n")
  }
  # now each pollutant+station has daily min/max/mean/resolution 

  # Loop on the stations again
  for(i in names(myNapsList)) {
    cat(paste0(i, " -"))
    for(p in names(myNapsList[[i]])) {
      cat(paste0(" ", p, " "))

      ################################################################################ 
      # 
      #  Strings of zeroes detection
      #  * checks resolution (e.g., earlier NO2 is 10-ppb resolution, so a string
      #    of zeroes could be due to rounding
      #
      valsNAts <- getStatTS(myNapsList[[i]], p, "valuesNA")
      valsNA <- valsNAts[, 2]
      dRes <- getStatValue(myNapsList[[i]]$data[[p]]$stats, "Resolution")

      # Expand (fill in) the daily resolution values
      dResShort <- dRes[seq( from = 13, by = 24, to = length(dRes))]
      dResExpand <- c(rep(dResShort, each = 24), dResShort[length(dResShort)])

      # Condition: if the resolution > 10, then look for 10 zeroes in a row
      valsMW <- makeWalk(valsNA, na.rm = TRUE)
      # Will need to grab the corresponding resolution for each row of valsMW
      if(nrow(valsMW) > 0) {
        dResSub <- dResExpand[!is.na(valsNA)]
        cs <- cumsum(valsMW$n)
        runs <- cbind(c(1, cs[-length(cs)] + 1), cs)
        dResMW <- numeric(nrow(runs))
        # Over each run in valsMW, take the median resolution as the resolution for the entire run.
        for(j in 1:nrow(runs)){
          dResMW[j] <- median(dResSub[runs[j, 1]:runs[j, 2]], na.rm = TRUE)
          if(is.na(dResMW[j])) {
            dResMW[j] <- max(dResMW[j-1], 0)
          }
        }
      } else {
        # This is the case where there is no data at all, so dRes should be NAs
        dResMW <- dRes
        valsMW <- makeWalk(rep(NA,nrow(valsNAts)), na.rm = TRUE)
      }

      #  Now, this statistic wants strings of 10+ zeroes in a row, for 
      #  the resolution being < 10 (older data); or strings of 100+ zeroes in 
      #  a row when the resolution is 10+. 
      zeroStrings <- makeSeries(valsMW, !((valsMW$v == 0 & valsMW$n > 10 & dResMW < 10) |(valsMW$v == 0 & valsMW$n > 100 & dResMW >= 10)))
      zeroStrings <- (zeroStrings == 0)
      zeroStrings[is.na(zeroStrings)] <- FALSE
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       pollutantStat(zeroStrings, "Zeroes > 10",
                                                     call("makeSeries", valsMW, 
                                                          !(valsMW$v==0 & valsMW$n > 10))
                                                    )
                                      )
      cat(".")

      ################################################################################
      #
      # Baseline shift detection based on looking at shifts in daily minima
      #
      dm <- getStatValue(myNapsList[[i]]$data[[p]]$stats, "Daily Min")
      maxT <- max(which(!is.na(dm)))
      minT <- min(which(!is.na(dm)))
      pTarg <- as.numeric(difftime(myNapsList[[i]]$time[maxT], myNapsList[[i]]$time[minT] , units = "weeks")) / 8
      dmSS <- spotShift(dm, dt = 12, offset = 0, k = 10, delta = 10, enp.target = pTarg)
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       pollutantStat(dmSS, "Daily Min Shift", 
                                                     attr(dmSS, "theCall")))
      cat(".")
    
      ################################################################################  
      #
      # Truncated daily max values
      #
      dM <- getStatTS(myNapsList[[i]], p, "Daily Max") 
      # Tabulate the daily maxima
      dMtab <- rev(table(dM$'Daily Max'))
      # If there are more daily max values than expected, flag
      # Here, we will take the average number in the next top 10 bins as comparison
      nbins <- length(dMtab)
      dM$'Truncated Daily' <- rep( FALSE, nrow(dM) )
      if(nbins > 1) {
        if(dMtab[1] > sum(dMtab[2:ceiling(nbins / 4)]) / floor(nbins/4)) {
          dM$'Truncated Daily' <- valsNA == as.numeric(names(dMtab)[1])
        }
      }
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       pollutantStat(dM$'Truncated Daily', "Truncated Daily",
                                                     call("if(dMtab[1] > sum( dMtab[2:ceiling(nbins / 4)]) / floor(nbins / 4)) {
                                                             dM$'Truncated Daily' <- valsNA == as.numeric(names(dMtab)[1])
                                                           } else {
                                                             dM$'Truncated Daily' <- rep(FALSE, nrow(dM))
                                                           }") 
                                                    )
                                      )
      cat(".")

      # A different criterion: strings of the same maximum value
      maxStrings <- makeSeries(valsMW, !(valsMW$v == max(valsMW$v,na.rm = TRUE) & valsMW$n>3))
      maxStrings <- maxStrings == max(valsMW$v, na.rm = TRUE)
      maxStrings[is.na(maxStrings)] <- FALSE
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       pollutantStat(maxStrings, "Max 3 in a row",
                                                     call("makeSeries", valsMW, !( valsMW$v == max(valsMW$v, na.rm = TRUE) & valsMW$n > 3))
                                                    )
                                      )
      cat(".")

      ################################################################################     
      #
      # Outlier zero values
      #
      pollVals <- getStatTS(myNapsList[[i]], p, "valuesNA")
      # 12 hour blocks
      minT <- trunc(min(pollVals$time), "day") + lubridate::hours(0)
      maxT <- trunc(max(pollVals$time), "day") + lubridate::hours(12)
      pollVals$daynight <- cut(pollVals$time, seq(from = minT, to = maxT, by = "12 hour"),
                               labels = FALSE)
     
      # Retrieve daily resolution information
      pollVals$res <- getStatTS(myNapsList[[i]], p, "Resolution")$Resolution
      flagging <- flagStat(pollVals$res, 24, 0, mean, na.rm = TRUE)$stat

      # cut to the second day and second-last day
      second_day_idx <- min(which(trunc(pollVals$time, "day") == trunc(min(pollVals$time), "day") + lubridate::days(1)))
      second_last_idx <- max(which(trunc(pollVals$time, "day") == trunc(max(pollVals$time), "day") - lubridate::days(1)))
      inner_flag <- flagStat(pollVals$res[second_day_idx:second_last_idx], 24, 0, mean, na.rm = TRUE)$stat
      first_flag <- mean(pollVals$res[1:(second_day_idx - 1)], na.rm = TRUE)
      last_flag <- mean(pollVals$res[(second_last_idx + 1):(dim(pollVals)[1])], na.rm = TRUE)

      pollVals$res <- c(rep(first_flag, second_day_idx - 1), rep(inner_flag, each = 24), rep(last_flag, dim(pollVals)[1] - second_last_idx))

      # Compute values divided by resolution, which will be used for outlier zero detection.
      pollVals$valsByRes <- pollVals$valuesNA / pollVals$res
      # Detection of outlier zeroes. Depends on daily resolution information.
      pollVals$outlier0 <- unlist(tapply(pollVals$valsByRes, pollVals$daynight, FUN = outlierValue, delta = 20))

      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       pollutantStat(pollVals$outlier0, "Outlier Zero",
                                                     call("tapply", pollVals$valsByRes, pollVals$daynight, FUN = outlierValue, delta = 20) 
                                                    ) 
                                      )
      cat(".")

      ################################################################################ 
      #
      #  All of the above flags, continuously added to 'valuesNA'. That is, each
      #  flag deletes more and more values from the original series, which became
      #  'valuesNA' by removing the "true missing" values, above.
      #

      # Strings of zero values
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       flagToNA(myNapsList[[i]]$data[[p]]$stats$'valuesNA',
                                                myNapsList[[i]]$data[[p]]$stats$'Zeroes > 10',
                                                newName = "valuesNA")
                                      )
      # Daily Min Shift
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]], 
                                       flagToNA(myNapsList[[i]]$data[[p]]$stats$'valuesNA',
                                                myNapsList[[i]]$data[[p]]$stats$'Daily Min Shift',
                                                newName = "valuesNA") 
                                      )

      # Truncated values (saturation)
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       flagToNA(myNapsList[[i]]$data[[p]]$stats$'valuesNA',
                                                myNapsList[[i]]$data[[p]]$stats$'Truncated Daily',
                                                newName = "valuesNA")
                                      )

      # Strings of daily max values
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       flagToNA(myNapsList[[i]]$data[[p]]$stats$'valuesNA',
                                                myNapsList[[i]]$data[[p]]$stats$'Max 3 in a row',
                                                newName = "valuesNA")
                                      )
      # Outlier zero values
      myNapsList[[i]]$data[[p]] <- add(myNapsList[[i]]$data[[p]],
                                       flagToNA(myNapsList[[i]]$data[[p]]$stats$'valuesNA',
                                                myNapsList[[i]]$data[[p]]$stats$'Outlier Zero',
                                                newName = "valuesNA")
                                      )
    }
    cat("\n")
  }
  cat("\n")
  save(myNapsList, file = paste("flagged", napsListFNs[match(cd, CDs)], sep = "-"))
}
 
################################################################################
#
#  now have flaggednapsList-XXXX.RData files (24 in total), which contain
#  all of the above flagged work. This includes a 'valuesNA' series for each
#  CD+station+pollutant, which we can interpolate.
#

#  Interpolate as 10-hour, via catnaps
foreach (cd = CDs) %dopar%{
  cat(paste0("Working on CD ", cd, ".\n"))
  load(paste0("flagged-napsList-", cd, ".RData"))
  myNapsListInterped <- interpStat(myNapsList, statName = "tsinterp Univar",
                                     maxGap = 10, sideRatio = 5, maxMissingRatio = 0.4, 
                                     missingSig = NA, cores = 1)
  save(myNapsListInterped, file = paste0("interp-napsList-", cd, ".RData"))
}

# Screening the interpolations
pdf("screen_10hr_interp.pdf", width = 9, height = 6)
par(mar = c(4,4,4,1))
for(cd in CDs) {
  cat(paste0("Working on CD ", cd, ".\n"))
  load(paste0("interp-napsList-", cd, ".RData"))
  for(j in names(myNapsListInterped)) {
    for(p in names(myNapsListInterped[[j]])) {
      ts_raw <- getStatTS(myNapsListInterped[[j]], p, "valuesNA") 
      ts_int <- getStatTS(myNapsListInterped[[j]], p, "tsinterp Univar")
      if(length(which(!is.na(ts_int[, 2]))) > 2) {
      pl_min <- min(which(!is.na(ts_int[, 2])))
      pl_max <- max(which(!is.na(ts_int[, 2])))
      plot(ts_int$time[pl_min:pl_max], ts_int$'tsinterp Univar'[pl_min:pl_max], type = "l", col = "red",
           xlab = "Time", ylab = "Ambient Concentration", 
           main = paste0(cd, " - ", j, " - ", p))
      lines(ts_raw$time[pl_min:pl_max], ts_raw$'valuesNA'[pl_min:pl_max], col = "black")
      }
      cat(paste0(cd, " - ", j, " - ", p, ": ", length(which(is.na(ts_raw[, 2]))) - length(which(is.na(ts_int[, 2]))), "\n"))
    }
  }
}
dev.off()

################################################################################
#
#  We now have flagged data, and interpolated data. Each CD is a separate
#  RData file, stored in the current directory. Let's cleanup our work.
#

dbFile <- "catnaps_v2.db"
if(file.exists(dbFile)) { file.remove(dbFile) }
initializeTable_date_mod(dbFile = dbFile, startYear = start_year, endYear = end_year)
initializeTable_naps_meta_mod(NAPSMetaFile = paste0(wd$metadata, "naps_metatdata_updated_2021.xls"), 
                              dbFile = dbFile, cdAssign = do.call("rbind", assign_to_cd))
initializeTable_pollutant_meta_mod(paste0(wd$metadata, "NAPS-pollutant-codesV2014.xlsx"), dbFile)
initialize_all_helper(dbFile)



# Loop on CDs
for (cd in CDs) { 
  load(paste0("interp-napsList-", cd, ".RData"))
  for(n in names(myNapsListInterped)) {
    for(p in names(myNapsListInterped[[n]])) {
      # this is a custom version of initializeDB which is designed
      # to work on already-constructed naps and napsList objects
      initializeDB_interped(myNapsListInterped[[n]], dbFile = dbFile, p)
    }
  }
}

################################################################################
#
#  Vlad-style one-file-per-station write-out
#  * Claire requests flags indicating "interpolated or not"
#  * also, may be interesting to get raw vs gaps-inserted vs interpolated
#  * so 3 series per pollutant per station + one flag series for "interpolated"
#
for (cd in CDs){ 
  cat(paste("CD ", cd, " ... \n")) 
  #load(file.path(paste0("interp-napsList-", cd, ".RData")))
  load(paste0(wd$getCatnaps, "interp-napsList-", cd, ".RData"))
  # Loop on the stations of this CD
  for(n in names(myNapsListInterped)) {
    cat(paste("Station ", n, " ... \n")) 
    # and on the pollutants contained inside 
    for(p in names(myNapsListInterped[[n]])) {
      # the actual series are: 
      # value, time
      # 'Missing Flag', 'Daily Min', 'Daily Max', 'Daily Mean'
      # 'Zeroes > 10', Daily Min Shift', Truncated Daily', Max 3 in a row', Outlier Zero'
      # 'valuesNA', 'tsinterpUnivar'
      time_arr <- getPollutant(myNapsListInterped[[n]], p)$time

      # one problem with time_arr ... it's in UTC!
      utc_offset <- myNapsListInterped[[n]]$meta$utc_offset
      time_arr <- time_arr + chron::hours(utc_offset)
      #time_arr <- time_arr + hours(utc_offset)
      
      dat_orig <- getPollutant(myNapsListInterped[[n]], p)$value
      pre_interp <- getStatTS(myNapsListInterped[[n]], p, "valuesNA")
      post_interp <- getStatTS(myNapsListInterped[[n]], p, "tsinterp Univar")

      output_df <- data.frame(date = time_arr, year = year(time_arr), month = month(time_arr),
                              day = day(time_arr), hour = hour(time_arr),
                              poldata = dat_orig, poldata_screen = pre_interp[, 2],
                              poldata_interp = post_interp[, 2], 
                              interpolated = !is.na(post_interp[, 2]) & is.na(pre_interp[, 2]))
      if(!dir.exists("Hourly_Out")) { dir.create("Hourly_Out") }
      if(!(p %in% c("PM25_adj", "PM25_unadj"))) {
        write.csv(x = output_df, file = paste0(wd$getCatnaps,file.path("Hourly_Out", paste0("hr_", p, "_", n, ".csv"))), 
                  row.names = FALSE)
      } else if(p == "PM25_adj") {
        write.csv(x = output_df, file =  paste0(wd$getCatnaps,file.path("Hourly_Out", paste0("hr_PM25a", "_", n, ".csv"))), 
                  row.names = FALSE)
      } else {
        write.csv(x = output_df, file =  paste0(wd$getCatnaps,file.path("Hourly_Out", paste0("hr_PM25u", "_", n, ".csv"))), 
                  row.names = FALSE)
      }
    }
  }
}





