#
#  Takes AHIdat.rda from ../ (created by running create_db.R
#
#  * interpolates up to ~ 3 month daily gaps
#  * doesn't worry too much about phase and structure and things, just applies
#    tsinterp to the data
#

library("tsinterp")
library("chron")
library(lubridate)
library(dplyr)
library(foreach)
library(readxl)
library(doParallel)
registerDoParallel(detectCores() - 1) 

setwd(wd$getAPInterp)
all_cd <- read_excel(paste0(wd$metadata, "List of 53 selected  CDs_2021.xlsx"))$cduid


# removes gaps to interpolate if more than 24 weeks (6 months) 
# finds the indcies where there is a gap of more than 182 days between non-
# missing data 
"check_gaps" <- function(in_dat, in_date){
  gaps <- which(is.na(in_dat))
  if(length(gaps) != 0){
    date_nonmissing <- in_date[which(!is.na(in_dat))]
    gap_time <- c(NA, date_nonmissing[-1] - date_nonmissing[-length(date_nonmissing)])
    gap_idx <- which(gap_time >= 182) # 182 days ~ 6 months
    
    if(length(gap_idx) == 0){
      return(gaps)
    } else {
      if(1 %in% gap_idx){gap_idx <- gap_idx[- which(gap_idx == 1)]}
      
      # minus one because we were looking at the nonmissing - but want to remove misisng 
      to_remove <- as_date(unlist(lapply(gap_idx, 
                                 function(x){seq.Date(date_nonmissing[x -1] +1 , date_nonmissing[x] -1, by = "day")})),
                           origin = lubridate::origin)
       
      return(gaps[ - which(in_date[gaps] %in% to_remove)]) 
    }
  } else {
    return(gaps)
  }
}


#  Takes a list of dates, returns split into Year, Month, Day data frame
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


#  Function which takes a time series x and returns x plus its first and second lag
"lag_one_two" <- function(x) {
  stopifnot(is.vector(x))
  N <- length(x)
  x.lag1 <- c(NA, x[1:(N-1)])
  x.lag2 <- c(NA, NA, x[1:(N-2)])
  cbind(x, x.lag1, x.lag2)
}


load(paste0(wd$getAPInterp, "ap.rda"))
AHIdat_cut <- lapply(ap, FUN = function(x) { x[x$Yr >= start_year & x$Yr <= end_year, ] })
AHIdat_cut <- AHIdat_cut[names(AHIdat_cut) %in% all_cd]

names_series <- unique(unlist(lapply(ap, colnames)))
n_series <- names_series[substr(names_series, 1, 1) == "c"]
n_series <- n_series[unlist(lapply(strsplit(n_series, "\\."), "[[", 3)) == "lag0"]
k_series <- n_series
for(j in 1:length(k_series)) {
  k_series[j] <- paste0("k", substr(k_series[j], 2, nchar(k_series[j])))
}

# Setup interpolated arrays
dat <- AHIdat_cut[[1]]
sample_series <- dat$Date

interpolated <- vector("list", length(AHIdat_cut))
interpolated <- lapply(interpolated, FUN = function(x) {
                           y <- as.data.frame(matrix(data = NA, nrow = length(sample_series), ncol = length(n_series)));
                           names(y) <- k_series;
                           y })
names(interpolated) <- names(AHIdat_cut)




foreach (j = 1:length(AHIdat_cut)) %dopar% { 
  cat(paste0("Interpolating CD ", j, "\n"))
  dat <- AHIdat_cut[[j]]
  time_date <- dat$Date

  for(k in 1:length(n_series)) {
      if(n_series[[k]] %in% names(dat)){
        cat(paste0("   ... pollutant ", k, "\n"))
        
        spec_series <- dat[[n_series[k]]]
        first_non_miss <- min(which(!is.na(spec_series)))
        final_non_miss <- max(which(!is.na(spec_series)))
        if((is.finite(first_non_miss) & is.finite(final_non_miss)) &
           !(first_non_miss == 1L & final_non_miss == length(spec_series))){
          in_dat <- spec_series[first_non_miss:final_non_miss]
          in_date <- time_date[first_non_miss:final_non_miss]
          
          if(length(in_dat) >= 366 & any(is.na(in_dat))){ # need missing vals 
            # initialize year-offset arrays
            in_dat2 <- cbind(c(rep(NA, 366), in_dat[1:(length(in_dat) - 366)]), in_dat,
                             spec_series[(first_non_miss + 365):(first_non_miss + 365 + length(in_dat) - 1)])
            
            gap_idx <- check_gaps(in_dat, in_date)
            interp_idx <- which(is.na(in_dat2[, 2]))
            set_to_na <- interp_idx[! interp_idx %in% gap_idx] # index we didnt want to interpolate
            # replace missing data with the average of the year before and year after, first pass
            in_dat2[which(is.na(in_dat2[, 2])), 2] <- rowSums(in_dat2, na.rm = TRUE)[which(is.na(in_dat2[, 2]))]
            
            # do univariate interpolation on the data
            z <- interpolate(in_dat2[, 2], gap = gap_idx, sigClip = 0.95, delT = 86400)
            z[[1]][set_to_na] <- NA  # bacuase we can't have NA values in the interpolate, but we didnt want to interp those values 

            #z <- interpolate(in_dat2[, 2], gap = which(is.na(in_dat)), sigClip = 0.95, delT = 86400)
            interpolated[[j]][[k_series[k]]] <- spec_series
            interpolated[[j]][[k_series[k]]][first_non_miss:final_non_miss] <- z[[1]]
          } else {
            interpolated[[j]][[k_series[k]]] <- spec_series
          }
      } else {
          interpolated[[j]][[k_series[k]]] <- spec_series
      }
    }
  }
  cat(paste0("Finished CD ", j, "\n"))

  # Save the current progress, so if it crashes, we can resume
  save(file = paste0(wd$getAPInterp, "interpCheckGap/interpolated_", j, ".rda"), interpolated)
}



# combine all the variab of the parallel computation 
interpolated_to_fill <- interpolated
for(i in 1:length(AHIdat_cut)){
  load(paste0(wd$getAPInterp, "/interpCheckGap/interpolated_", i, ".rda"))
  interpolated_to_fill[[i]] <- interpolated[[i]]
  interpolated_to_fill[[i]]$Date <- sample_series
}

remove_locigal <- lapply(interpolated_to_fill, function(x){x %>% select(- where(is.logical))})




# add lag cols to interpolate 
for(i in 1:length(remove_locigal)){
  z <- data.frame("Date" = remove_locigal[[i]][, c("Date")])
  idx <- ncol(z) + 1 
  col_names <- grep("lag0", names(remove_locigal[[i]]), value = TRUE)
  poll_names <- sub("k", "", unlist(lapply(strsplit(col_names, "\\."), "[[", 1)))
  time_names <- sub("k", "", unlist(lapply(strsplit(col_names, "\\."), "[[", 2)))
  
  if(length(col_names) != 0){
    for(k in 1:length(col_names)) {
      z[, idx:(idx+2)] <- lag_one_two(remove_locigal[[i]][, col_names[k]])
      names(z)[idx:(idx+2)] <- paste(paste0("k", poll_names[k]), 
                                     time_names[k], c("lag0", "lag1", "lag2"), sep = ".")
      idx <- idx + 3
    }
  }
  
  remove_locigal[[i]] <- z 
}


interpolated <- remove_locigal
#---------------------------------------------------------------------------

save(file = paste0(wd$getAPInterp, "interpCheckGap/interpolated_all.rda"), interpolated)

