#Biodiversity DATA ORIGAMI to be processed on weekly basis:

#Roy Rich  orginate 11/6/test
# The following codes are functions and procedures for parsing loggernet data streams into researcher tables for BDT sesnor arrays.
# functions are set up in order with annotation along with diretory structure needed to implement on scheduled basis.

## Libraries needed
##plyr, reshape2, lubridate, data.table libraries

## BDT data processing outline and flow
## 1. Copy data and structure from scheduled files.
## 2. Process into *.csv raw files
## 3. Flat file normalization for storage



### CHECK FILES FOR DATA AND LOGGERNET FORMAT
### Function DATACHECK
### - removes short files less that 5 rows (header is 4 lines in TOA5, and 2 lines in TOACI1)
### - raw_dir is location of files and should be a copy of raw files; raw_dir_copy is temporary directory, raw_storage is final storage directory for files additional files should be included
### - removes non-dat/backup file extensions as well

library(reshape2)
library(data.table)
library(lubridate)
library(zoo)
library(gtools)
library(tools)
library(ggplot2)
library(plyr)
library(gridExtra)


mean_rm<-function(x){mean(x, na.rm=TRUE)} ### used for aggregate function, allows mean data to deal with NA within apply functions



DATACHECK<-function(raw_dir, raw_dir_copy, raw_storage) {

  a<-list.files(raw_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)
  file.copy(a, raw_dir_copy) ### archinve manually

  i<-list.files(raw_dir_copy, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

  j<-file_ext(i[1:length(i)])

  p<-c(1:length(i))
  for (n in p) {

    if(j[n]!= "dat" && j[n]!= "bak") file.remove(i[n])}


  i<-list.files(raw_dir_copy, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

  p<-c(1:length(i))
  for (n in p) {
    p2<-read.table(i[n], sep = ",", quote = "\"'",
                   dec = ".",
                   na.strings = "NA", colClasses = "character", nrows = 6,
                   skip = 0, check.names = FALSE,fill=TRUE,
                   strip.white = FALSE,blank.lines.skip = TRUE)
    type<-p2[1,1]
    p3<-nrow(p2)
    if(p3<3 & type =="TOACI1") file.remove(i[n])
    if(p3<5 & type =="TOA5") file.remove(i[n])

    i<-list.files(raw_dir_copy, pattern = NULL, all.files = FALSE,
                  full.names = TRUE, recursive = TRUE,
                  ignore.case = FALSE)
    file.copy(i, raw_storage)
    ###file.remove(i) ### changed for scheduled function
  }
}

##########################################################################

###PROCESS
###Convert TOA5 or TOACI1 to .csv files
###header change and rename file based on data characterististics
#Incoming directory= raw_storage
#Outgoing directory=  proc_storage
#meta_file = record of processed files


PROCESS<-function(proc_storage,raw_dir_copy,meta_file) {

  i<-list.files(raw_dir_copy, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)


  p<-c(1:length(i))
  for (n in p) {
    currentfile<-i[n]
    dat<-read.table(i[n],sep = ",", quote = "\"'",
                    dec = ".",
                    na.strings = "NA", colClasses = "character", nrows = 6,
                    skip = 0, check.names = FALSE,fill=TRUE,
                    strip.white = FALSE,blank.lines.skip = TRUE)

    type<-dat[1,1]


    if(type =="TOACI1") dat3<-read.table(i[n],, sep = ",", quote = "\"'",
                                         dec = ".",,,,
                                         na.strings = "NA", colClasses = "character", nrows = -1,
                                         skip = 2,,fill=TRUE,
                                         strip.white = FALSE,blank.lines.skip = TRUE,
                                         ,,,,,)

    if(type =="TOA5") dat3<-read.table(i[n],, sep = ",", quote = "\"'",
                                       dec = ".",,,,
                                       na.strings = "NA", colClasses = "character", nrows = -1,
                                       skip = 4,,fill=TRUE,
                                       strip.white = FALSE,blank.lines.skip = TRUE,
                                       ,,,,,)
    d<-ncol(dat3)
    dat1<-dat[1,]
    dat2<-dat[2,1:d]
    datnames<-c(dat2,"Logger", "Program", "Table")
    newnames<-datnames
    rm(dat)
    if(type =="TOA5")dat4<-cbind(dat3,dat1[2],dat1[6],dat1[8])
    if(type =="TOACI1")dat4<-cbind(dat3,dat1[2],type,dat1[3])
    colnames(dat4)<-newnames

    dat5<-substr(dat4[1,1],1,10)
    dat6<-substr(dat4[1,1],12,20)
    dat7<-chartr(":","-",dat6)
    ###create unique file name "d" by pulling data from process file in this order
    ###table type, date, time, block, nrows in dataframe, type of orginal file
    ###any file name that could be overwritten would be duplicate file
    if(type =="TOA5")
      d<-c(paste(dat1[1,8],dat5,dat7,dat1[1,2],nrow(dat4),type,".csv",sep="_"))
    if(type =="TOACI1")d<-c(paste(dat1[1,3],dat5,dat7,dat1[1,2],nrow(dat4),type,".csv",sep="_"))

    d<-c(paste(proc_storage,"/", d ,sep=""))

    dat8<-c(d,datnames,i[n])
    write.table(dat8,meta_file, append = TRUE, quote = FALSE, sep = ",",
                ,na = "NA", dec = ".", row.names = FALSE,
                col.names = FALSE, qmethod = c("escape", "double"))
    write.table(dat4,d,append = FALSE, quote = FALSE, sep = ",",
                ,na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))

    file.remove(i[n])

    rm(dat1)
    rm(dat2)
    rm(dat3)
    rm(datnames)
    rm(dat4)
    rm(d)
    rm(dat6)
    rm(dat7)
    rm(dat8)
  }
}

# Function HEADER_CHECK
#added as a temporary fix for a group of files that have incorrect headers. mostly on node 107. Correct structure: "VWC_j1_Avg(1)", old structure being corrected "VWC_b1(2)". There is probbaly a better way to do this...
header<-function(proc_storage) {

  i<-list.files(proc_storage, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)


  p<-c(1:length(i))
  for (n in p){
    dt<-fread(i[n])

    ###condition names
    newnames<-names(dt)
    newnames<-gsub("_Avg","", newnames ) #added to fix header issues in some files
    newnames<-sub("(*_)(.*[[:digit:]])(.{3})", "\\1\\2_Avg\\3", newnames) #added to fix header issues in some files
    #newnames<-gsub("[(]","",newnames)
    #newnames<-gsub("[)]","",newnames)
    #newnames<-gsub("timestamp","tmstamp",newnames)

    setnames(dt, newnames)


    d<-i[n]

    write.table(dt,d,append = FALSE, quote = FALSE, sep = ",",
                ,na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
  }
}
###NORMAL_BDT FUNCTION
##### Re-Formats raw data files from loggernet processing, and stacks (melts) the data.
##### Creates a logger file with all logger level data, and creates a plot level file with all plot level data
##### Creates flat form renormalized files for data file with experimental data integrated.

###The two key files needed for this are:
##1 var_role: a list of variables in the CR1000 processed data,their common variable name, and whether they need to be stacked.
##2 design_table: this connects the experimental design to the cr1000 variable name and the scale link for data. It has every variable used in datalogger exept raw t_temp data which are store separatetly, and 60 minute data.
##  An excel file called BDT-processing_keys.xlsx has sheets needed to add or change the structure of the data. The original source for this is the configuration file and the Sauron Program. After the 2012 switchover of to water removal, the program structure became ver complex to accomdate the change in experimental design. Variables may be coded by original (standard coding or by PPT manipulation coding). Caution must be made in properly ading new varables to check code and determine experimental structure of channels.

#var_role = linkage between Loggernet varaibles and scale of data
#design_table = experimental design and link
#source_dir = directory of processed files
#output_dir = directory of renormalized files
#store_dir= stored files moved after renormalizing


###new
NormBDT<-function(node1, sensors, plot_design,proc_storage,source_dir,output_dir,store_dir,increment){

  node<-fread(node1)
  var<-fread(sensors)
  plotname<-fread(plot_design)
  increment= 15

  key1=c("logger", "plot","SDI12address")

  plotname2=subset(plotname, select=-SDI12address)
  plotname2<-lapply(plotname2,tolower)
  plotname2<-as.data.table(plotname2)
  plotname<-cbind(plotname$SDI12address, plotname2)
  plotname<-rename(plotname, c("V1" = "SDI12address"))

  node<-lapply(node,tolower)
  node<-as.data.table(node)

  var2=subset(var,select=-SDI12address)
  var2<-lapply(var2,tolower)
  var2<-as.data.table(var2)
  var<-cbind(var$SDI12address, var2)
  var<-rename(var, c("V1" = "SDI12address"))

  var2<-merge(var,plotname,allow.cartesian=TRUE,by=key1)

  key2<-c("logger")
  var3<-merge(node,plotname,allow.cartesian=TRUE,by=key2)

  var4<-rbind.fill (var3, var2)
  var4<- as.data.table(var4)
  var1<-var4[role=="key",]
  key<-as.vector(var1$CR300_variable)
  var1<-var4[role=="n",]
  nnn<-as.vector(var1$CR300_variable)
  var1<-var4[role=="y",]
  ddd<-as.vector(var1$CR300_variable)

  i<-list.files(proc_storage, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)

  file.copy(i,source_dir)

  i<-list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)
  j<-list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = FALSE, recursive = TRUE,
                ignore.case = FALSE)
  ##n= 1 #for testing

  p<-c(1:length(i))
  for (n in p)

  {dt<-fread(i[n])

  ###condition names
  setnames(dt, tolower(names(dt)))
  newnames<-names(dt)
  newnames<-gsub("[(]","",newnames)
  newnames<-gsub("[)]","",newnames)
  newnames<-gsub("timestamp","tmstamp",newnames)

  setnames(dt, newnames)

  ### lowercase data
  dt<-lapply(dt,tolower)
  dt<-as.data.table(dt)
    ##if(type =="TOA5")
    ##if(type =="TOACI1")dt$tmstamp<-dt$timestamp

  #if (is.na(dt$logger) == TRUE) dt[,logger:=statname1]

  ## create time2 and row_id
  Sys.setenv(TZ="America/Cancun") ### set for EST all year long

  dt<-subset(dt,grepl("20..-..-.. ..:..:..", dt$tmstamp))
  dt$tmstamp<-as.POSIXct(dt$tmstamp)
  dt2<-round(minute(dt$tmstamp)/increment)*increment
  dt$time2<-update(dt$tmstamp, min=dt2)
 # dt$row_id<-paste(dt$tmstamp,dt$logger,sep="_")
 # dt[,row_id:=paste("bdt", row_id, sep="_")] # create rowid

  plot_names<-c(key,ddd)
  site_names<-c(key,nnn)

  dtbase<-subset(dt,select=names(dt)%in%site_names) ##site data for merge
  dts<-subset(dt,select=names(dt)%in%plot_names) ##plot level to denormalize

  ###normalize data
  rm(dt2)
  rm(dt)

  ###stack block data
  # key5<-c("logger","time2", "tmstamp", "statname")
  # dtb2<-melt(dtbase, id.vars = key5, measure.vars = , variable.name="cr300_variable",, na.rm = FALSE,)
  # dtb2<-apply(dtb2,2,tolower)
  # dtb3<-as.data.table(dtb2)
  # rm(dtb2)

  ###stack plotdata
  key<-c("logger","time2","tmstamp","statname" )
  dts2<-melt(dts, id.vars = key, measure.vars = , variable.name="CR300_variable",, na.rm = FALSE,)
  dts2<-apply(dts2,2,tolower)
  dts3<-as.data.table(dts2)
  rm(dts2)
  rm(dts)

  key2=c("logger","CR300_variable")
  setkeyv(dts3,key2)
  setkeyv(var4,key2)
  key3<-c("time2", "logger", "plot","subplot","value", "height", "Research_variable", "CR300_variable", "role")
  key4<-c("time2", "logger", "plot")
  dts4<-merge(dts3,var4,allow.cartesian=TRUE,by=key2)

  ###merge and recast data
  dts5<-subset(dts4, select=key3)
  dts5A<-dts5[role=="y",]
  dts5B<-dts5[role!="y",]

  rm(dts5)

  setkey(dts5A,NULL)
  dts5A<-unique(dts5A)
  dts5A$value<-as.double(dts5A$value)
  is.na(dts5A)<-is.na(dts5A)
  dts6A<-dcast(dts5A,plot+subplot+logger+time2+height~ Research_variable, fun.aggregate = mean_rm ,subset = NULL, drop = TRUE, value.var = "value") # note that fill should be allowed to default
  dts6<-as.data.table(dts6A)

  rm(dts5A)
  rm(dts6A)

  ### fix for text data
  if(nrow(dts5B)> 0){
    setkey(dts5B,NULL)
    dts5B<-unique(dts5B)
    is.na(dts5B)<-is.na(dts5B)
    dts6B<-dcast(dts5B,plot+subplot+logger+time2~ Research_variable, fun.aggregate = NULL ,subset = NULL, drop = TRUE, value.var = "value") # note that fill should be allowed to default
    dts6B<-as.data.table(dts6B)
    dts6<-dts6B[dts6]}

  rm(dts5B)
  rm(dts6B)

  key5<-c("logger", "time2")

  dts6$time2<-as.POSIXct(dts6$time2)
  dtbase$time2<-as.POSIXct(dtbase$time2)
  setkeyv(dts6,key5)
  setkeyv(dtbase,key5)
  dtbase<-unique(dtbase)
  ### merge non normalized variables with data
  dts7<-dts6[dtbase]

  rm(dts6)
  rm(dtbase)

  ###add back in plot design variables
  add<- c("logger", "plot","subplot","div")
  plotname2<-as.data.table(unique(subset(plotname, select=add)))

  add2<- c("tmstamp", "statname", "batt_check","logger")

  key6<-c("plot", "subplot", "logger")
  setkeyv(dts7,key6)
  setkeyv(plotname2,key6)
  dts8<-plotname2[dts7]
  rm(dts7)
  ###
  dts8<-subset(dts8,select=!(names(dts8)%in%add2))

  d<-c(paste("norm_",j[n]))
  d<-c(paste(output_dir,d,sep="/"))

  write.table(dts8,d, append = FALSE, quote = FALSE, sep = ",",
              na = "NA", dec = ".", row.names = FALSE,
              col.names = TRUE, qmethod = c("escape", "double"))

  b<-c(paste(store_dir,j[n],sep="/"))
  file.copy(i[n],b)
  file.remove(i[n])
  rm(dts8)
  }}



#FUNCTION MONTHLY MANAGE
#- checks for duplicates and stores and sorts data by logger month

MONTHLYMANAGE<-function(norm_dir,soil_dir,air_dir,tab1,tab2){

  i<-list.files(norm_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)
  j<-list.files(norm_dir, pattern = NULL, all.files = FALSE,
                full.names = FALSE, recursive = TRUE,
                ignore.case = FALSE)

  dt1<-fread(i[1])

  p<-c(2:length(i))
  for (n in p)

  {dt<-fread(i[n])

  dt1<-rbind.fill(dt,dt1)
  }

  dt<-unique(dt1)
  dt<-as.data.table(dt)

  dt$time2<-as.POSIXct(dt$time2)

  MO= unique(week(dt$time2))
  YR<-unique(year(dt$time2))
  #BK<-unique(tolower(dt$logger))
  YB<-merge(YR,MO)
  names(YB)<-c("YR","MO")
  #YB<-merge(Y1,BK)
  #names(YB)<-c("YR","MO", "BK")
  data.frame(YB)

  CS215<- c("plot", "subplot", "div", "time2", "height", "airtemp","cleanrh","rawrh","svp","vp", "vpd","batt_volt")
  Sentek<-c("plot", "subplot", "div" ,"time2" ,  "height","sal",  "temp","vwc" ,"batt_volt")


  p<-c(1:nrow(YB))
  for (m in p) {

    dtplot<- subset(dt, week(dt$time2)==YB$MO[m] & year(dt$time2)== YB$YR[m] )
    dtsoil<-subset(dtplot,select=names(dtplot)%in%Sentek) ##site data for merge
    dtsoil<-subset(dtsoil,dtsoil$height<0)

    dtair<-subset(dtplot,select=names(dtplot)%in%CS215)##plot level to denormalize
    dtair<-subset(dtair,dtair$height==120)
    d<-c(paste(tab1,YB$MO[m],YB$YR[m],sep="_"))
    d<-c(paste(d,"csv",sep="."))
    d<-c(paste(soil_dir,d,sep="/"))

    if (file.exists(d)==TRUE) {

      dt2<-fread(d)
      dt2<-rbind.fill(dtsoil,dt2)
      dt2<-unique(dt2)
      dt2<-as.data.table(dt2)

      write.table(dt2,d, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
      rm(dt2)}

    if (file.exists(d)==FALSE) {

      write.table(dtsoil,d, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))}



      e<-c(paste(tab2,YB$MO[m],YB$YR[m],sep="_"))
      e<-c(paste(e,"csv",sep="."))
      e<-c(paste(air_dir,e,sep="/"))

      if (file.exists(e)==TRUE) {

        dt2<-fread(e)
        dt2<-rbind.fill(dtair,dt2)
        dt2<-unique(dt2)
        dt2<-as.data.table(dt2)

        write.table(dt2,e, append = FALSE, quote = FALSE, sep = ",",
                    , na = "NA", dec = ".", row.names = FALSE,
                    col.names = TRUE, qmethod = c("escape", "double"))
        rm(dt2)}

      if (file.exists(e)==FALSE) {

        write.table(dtair,e, append = FALSE, quote = FALSE, sep = ",",
                    , na = "NA", dec = ".", row.names = FALSE,
                    col.names = TRUE, qmethod = c("escape", "double"))
      }

  }}


##For low memory systems
MONTHLYMANAGE2<-function(norm_dir,soil_dir,air_dir,tab1,tab2){

  i<-list.files(norm_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)
  j<-list.files(norm_dir, pattern = NULL, all.files = FALSE,
                full.names = FALSE, recursive = TRUE,
                ignore.case = FALSE)

  #dt1<-fread(i[1])

  p<-c(1:length(i))
  for (n in p)

  {dt<-fread(i[n])

  #dt1<-rbind.fill(dt,dt1)


  #dt<-unique(dt1)
  #dt<-as.data.table(dt)


  dt$time2<-as.POSIXct(dt$time2)

  MO= unique(week(dt$time2))
  YR<-unique(year(dt$time2))
  #BK<-unique(tolower(dt$logger))
  YB<-merge(YR,MO)
  names(YB)<-c("YR","MO")
  #YB<-merge(Y1,BK)
  #names(YB)<-c("YR","MO", "BK")
  data.frame(YB)

  CS215<- c("plot", "subplot", "div", "time2", "height", "airtemp","cleanrh","rawrh","svp","vp", "vpd","batt_volt")
  Sentek<-c("plot", "subplot", "div" ,"time2" ,  "height","sal",  "temp","vwc" ,"batt_volt")


  p<-c(1:nrow(YB))
  for (m in p) {

    dtplot<- subset(dt, week(dt$time2)==YB$MO[m] & year(dt$time2)== YB$YR[m] )
    dtsoil<-subset(dtplot,select=names(dtplot)%in%Sentek) ##site data for merge
    dtsoil<-subset(dtsoil,dtsoil$height<0)

    dtair<-subset(dtplot,select=names(dtplot)%in%CS215)##plot level to denormalize
    dtair<-subset(dtair,dtair$height==120)
    d<-c(paste(tab1,YB$MO[m],YB$YR[m],sep="_"))
    d<-c(paste(d,"csv",sep="."))
    d<-c(paste(soil_dir,d,sep="/"))

    if (file.exists(d)==TRUE) {

      dt2<-fread(d)
      dt2<-rbind.fill(dtsoil,dt2)
      dt2<-unique(dt2)
      dt2<-as.data.table(dt2)

      write.table(dt2,d, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
      rm(dt2)}

    if (file.exists(d)==FALSE) {

      write.table(dtsoil,d, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))}



    e<-c(paste(tab2,YB$MO[m],YB$YR[m],sep="_"))
    e<-c(paste(e,"csv",sep="."))
    e<-c(paste(air_dir,e,sep="/"))

    if (file.exists(e)==TRUE) {

      dt2<-fread(e)
      dt2<-rbind.fill(dtair,dt2)
      dt2<-unique(dt2)
      dt2<-as.data.table(dt2)

      write.table(dt2,e, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
      rm(dt2)}

    if (file.exists(e)==FALSE) {

      write.table(dtair,e, append = FALSE, quote = FALSE, sep = ",",
                  , na = "NA", dec = ".", row.names = FALSE,
                  col.names = TRUE, qmethod = c("escape", "double"))
    }

  }}}



###duplicate removal hunting
dup_removal<-function(source_dir, output_dir){
  k<-list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = TRUE, recursive = TRUE,
                ignore.case = FALSE)
  l<-list.files(source_dir, pattern = NULL, all.files = FALSE,
                full.names = FALSE, recursive = TRUE,
                ignore.case = FALSE)
  p<-c(1:length(k))
  for (n in p) {
    dt3<-fread(k[n])
    dt3<-unique(dt3)
    c<-c(paste("nd", l[n], sep="_"))
    c<-c(paste(output_dir,c, sep="/"))
    write.table(dt3,c, append = FALSE, quote = FALSE, sep = ",",
                , na = "NA", dec = ".", row.names = FALSE,
                col.names = TRUE, qmethod = c("escape", "double"))
  }
}


#

####commands for 2017_2018 functions on SERVER. Files pathes changed to pullenj to run on Jamie's computer.Sucessfuly ran on Feb 25th 2019. Unless changes are made to the data normalization process this data should not need to be run again. The exception will be the files moved into a seperate folder (D:\Dropbox (Smithsonian)\BDT_Sensor_Data\Processed_data\files with issues) which had problems that prevented them from being run through the "NormBDT" function, see word document "Troubleshooting sensor data issues" fpr details on issues. data from the export folder will be cleaned up and a final version will be saved for publishing(?) or sharing with people####


raw_dir<-"D:/sample_biodiversitree/data/raw_data/raw_dir_21"
raw_dir_copy<-"D:/sample_biodiversitree/data/raw_data/raw_dir_temp"
raw_storage<-"D:/sample_biodiversitree/data/raw_data/raw_total_storage"

proc_storage<-"D:/sample_biodiversitree/data/processed_data/proc_storage_21"
meta_file<-"D:/sample_biodiversitree/data/processed_data/proc_storage_21/metafile_2021.txt"

node1<-"D:/sample_biodiversitree/table_design/node.csv"
sensors<-"D:/sample_biodiversitree/table_design/sensors.csv"
plot_design<-"D:/sample_biodiversitree/table_design/plot.csv"

source_dir<-"D:/sample_biodiversitree/data/processed_data/proc_storage_21"
output_dir<-"D:/sample_biodiversitree/data/normalized_data/normalized_21"
store_dir<-"D:/sample_biodiversitree/data/normalized_data"

###function cleans duplicates and parses data files by month. Uses rbind.fill
norm_dir<-"D:/sample_biodiversitree/data/normalized_data/normalized_21"
soil_dir<-"D:/sample_biodiversitree/data/export_data/export_20/soil_21"
air_dir<-"D:/sample_biodiversitree/data/export_data/export_20/air_21"
tab1= "BDT_soil"
tab2= "BDT_air"



DATACHECK(raw_dir, raw_dir_copy,raw_storage)

PROCESS(proc_storage,raw_dir_copy,meta_file)

header(proc_storage)

NormBDT(node1, sensors, plot_design, proc_storage, source_dir, output_dir,store_dir,increment)

MONTHLYMANAGE2(norm_dir,soil_dir,air_dir,tab1,tab2)


