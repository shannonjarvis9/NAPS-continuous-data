# This script transforms the csv files produced from running catnaps into a list 
# of census divisions 

# The resulting .rda object contains a list of CD where each CD data frame 
# contains the daily AP observations 
# Columns are named with a prefix: n: original AP data
#                                  k: 10-hr interpolated AP from catnaps 

# The get_ap functions were written by Wesley Burr and obtained from Old_Working_AHIdat/create_db.R  


library(chron)
library(foreach)
library(doParallel)
library(readr)
registerDoParallel(detectCores() - 1) 


#--------------------------------------------------------------------------------
# Create the .rda object of data for each census division 
#--------------------------------------------------------------------------------
source(paste0(wd$getAP, "getAP_functions.R"))


# setup our files
#--------------------------------------------------------
assign_CD_ap <- read_csv(paste0(wd$metadata, "assign_to_cd.csv"))




# read the original AP data into a list 
#--------------------------------------------------------
ap_orig   <- get_ap(time_span = seq(chron(paste0("01/01/", start_year)), 
                                    chron(paste0("12/31/", end_year))),
                    assign_CD_ap, dir = wd$home, prefix = "n",
                    file_pattern = "orig")

save(file = paste0(wd$getAPInterp,"ap_orig.rda"), ap_orig)



# read the 10-hour gap-filled AP data into a list 
#--------------------------------------------------------
ap_interp   <- get_ap(time_span = seq(chron(paste0("01/01/", start_year)), 
                                      chron(paste0("12/31/", end_year))),
                      assign_CD_ap, dir = wd$home , prefix = "c",
                      file_pattern = "screen_10hr_int", replace_neg = TRUE)


save(file = paste0(wd$getAPInterp,"ap_interp.rda"), ap_interp)
 

# merge the original and interpolated into one data frame 
#--------------------------------------------------------
ap <- vector("list", length = length(intersect(names(ap_interp), names(ap_orig))))
names(ap) <- intersect(names(ap_interp), names(ap_orig))

for(i in names(ap)){
  ap[[i]] <- merge(ap_interp[[i]], ap_orig[[i]], by = intersect(names(ap_interp[[i]]), names(ap_orig[[i]])))
}


save(file = paste0(wd$getAPInterp,"ap_vals.rda"), ap)

