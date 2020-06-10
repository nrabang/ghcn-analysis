# ***Nico Rabang ID 91369399***
# ***Q1***

# **** Bash commands used to get tree structure of files  ****


# fs -lsr /data/ghcnd/ | awk '{print $8}' | \sed -e 's/[^-][^\/]*\//--/g' -e 's/^/ /' -e 's/-/|/'


# **** Bash commands end here ****


# ***Q2A***

import sys

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from pyspark.sql import functions as F

# Create Dataframes from text files

spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

raw_countries = spark.read.text("hdfs:///data/ghcnd/countries")
countries = raw_countries.select(
raw_countries.value.substr(1,2).cast('string').alias('country_code'),
raw_countries.value.substr(4,50).cast('string').alias('country_name')
)

#countries.show()

raw_stations = spark.read.text('hdfs:///data/ghcnd/stations')
stations = raw_stations.select(
raw_stations.value.substr(1,11).cast('string').alias('station_id'),
raw_stations.value.substr(13,8).cast('double').alias('latitude'),
raw_stations.value.substr(22,9).cast('double').alias('longitude'),
raw_stations.value.substr(32,5).cast('double').alias('elevation'),
raw_stations.value.substr(39,2).cast('string').alias('state_code'),
raw_stations.value.substr(42,29).cast('string').alias('name'),
raw_stations.value.substr(73,3).cast('string').alias('gsn_flag'),
raw_stations.value.substr(77,3).cast('string').alias('hcn_crn_flag'),
raw_stations.value.substr(81,5).cast('string').alias('wmo_id')
)

#stations.show()

raw_states = spark.read.text('hdfs:///data/ghcnd/states')
states = raw_states.select(
raw_states.value.substr(1, 2).cast('string').alias('state_code'),
raw_states.value.substr(4, 47).cast('string').alias('state_name')
)

#states.show()

raw_inventory = spark.read.text('hdfs:///data/ghcnd/inventory')
inventory = raw_inventory.select(
raw_inventory.value.substr(1,11).cast('string').alias('inventory_id'),
raw_inventory.value.substr(13,8).cast('double').alias('latitude'),
raw_inventory.value.substr(22,9).cast('double').alias('longitude'),
raw_inventory.value.substr(32,4).cast('string').alias('element'),
raw_inventory.value.substr(37,4).cast('integer').alias('first_year'),
raw_inventory.value.substr(42,4).cast('integer').alias('last_year')

)

#inventory.show()

daily_schema = StructType([
StructField("station_id", StringType(), True),
StructField("date", StringType(), True),
StructField("element", StringType(), True),
StructField("element_value", DoubleType(), True),
StructField("measurement_flag", StringType(), True),
StructField("quality_flag", StringType(), True),
StructField("source_flag", StringType(), True),
StructField("observation_time", StringType(), True)

])

# Create Dataframe from csv format

daily = (
spark.read.format('com.databricks.spark.csv')
.option('dateFormat', 'YYYYMMDD')
.option('timestampFormat', 'HHMM')
.load('hdfs:///data/ghcnd/daily', schema=daily_schema)
.limit(500)
#.cache()


)

#daily.show(1000)

# ***Q2C***	

#code to get rows and columns in stations
#print((stations.count(), len(stations.columns)))
(103656, 9) 
#The stations table has 103656 rows

#code to get rows and columns in states
#print((states.count(), len(states.columns)))
(74, 2)
#The states table has 74 rows

#code to get rows and columns in countries
#print((countries.count(), len(countries.columns)))
(218, 2)
#The countries table has 218 rows

#code to get rows and columns in inventory
#print((inventory.count(), len(inventory.columns)))
(595699, 6) 
#The inventory table has 595699 rows


no_wmoid = (
stations
# Group by source and count destinations
.groupBy('wmo_id')
.agg({'wmo_id': 'count'})
.orderBy('count(wmo_id)', ascending=False)
.select(['wmo_id', F.col('count(wmo_id)').alias('countsx')
]))

#alternate way of getting stations with no wmo_id values

stationsnowmo= stations.filter((stations["wmo_id"] == "     ") | stations["wmo_id"].isNull() | F.isnan(stations["wmo_id"]))

#Q3a

stations2 = stations.withColumn("country_code", stations['station_id'].substr(1,2))

#Q3b
stations2 = stations2.join(countries, on='country_code', how='left')

#Q3c

stationsUSonly= stations2.filter(stations2['country_code']=='US')
stationsUSonly = stationsUSonly.join(states, on='state_code', how='left')
stationsstates = stations.join(stationsUSonly, on='station_id', how='left')


stations2 = stations2.join(states, on='state_code', how='left')

US_stations = stations2.where(F.col("country_code") == 'US')

#Q3d

inv2= inventory.filter(F.col('element').isNotNull()).groupBy('inventory_id').agg(F.min('first_year'),F.max('last_year'))
inv2 = inv2.withColumnRenamed('inventory_id', 'station_id')

inv3= inventory.groupBy("inventory_id").agg(F.countDistinct("element"))
tab_all_elems =inv3.withColumnRenamed('count(DISTINCT element)', 'all_elem_count')

#core elements listed:
#    PRCP = Precipitation (tenths of mm)
#    SNOW = Snowfall (mm)
#    SNWD = Snow depth (mm)
#    TMAX = Maximum temperature (tenths of degrees C)
#    TMIN = Minimum temperature (tenths of degrees C)

core = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']

#inventory table with only core elements shown
core_elems_only = inventory.where(F.col('element').isin(core))

#sum of stations for core elem count
core_elems_only_sum = core_elems_only.groupBy('element').agg(F.count('element'))

#number of core elements each station collected
tab_core_elems_only = core_elems_only.groupBy('inventory_id').agg(F.countDistinct('element'))
tab_core_elems_only.select(F.col('count(DISTINCT element)'))

#inventory table with only non-core elements shown
no_core_elems = inventory.where(F.col('element').isin(*core)== False)

#number of non-core elements each station collected
tab_no_core_elems = no_core_elems.groupBy('inventory_id').agg(F.countDistinct('element'))

#renaming columns
tab_core_elems_only = tab_core_elems_only.withColumnRenamed('count(DISTINCT element)', 'core_elem_count')
tab_no_core_elems = tab_no_core_elems.withColumnRenamed('count(DISTINCT element)', 'other_elem_count')

#table for stations with all 5 core elems 
fivecountshow = tab_core_elems_only.filter(tab_core_elems_only['core_elem_count']==5)

#rename columns to prep for joining with stations
tab_core_elems_only = tab_core_elems_only.withColumnRenamed('inventory_id', 'station_id')
tab_no_core_elems = tab_no_core_elems.withColumnRenamed('inventory_id', 'station_id')
tab_all_elems = tab_all_elems.withColumnRenamed('inventory_id', 'station_id')

#Q3E

stations3 = stations2.join(tab_core_elems_only, on='station_id', how='left')
stations3 = stations3.join(tab_no_core_elems, on='station_id', how='left')
stations3 = stations3.join(tab_all_elems, on='station_id', how='left')
stations3 = stations3.join(inv2, on='station_id', how='left')
stations4 = daily.join(stations3, on='station_id', how='left')

#use enriched stations table and join with inventory to get elem columm for PRCP count
stations3x = stations3.withColumnRenamed('station_id', 'inventory_id')
stations3x = stations3x.join(inventory.select(F.col('inventory_id'), F.col('element')), on='inventory_id', how='left')
#filter this table where element count is 1 (collect only 1 element) and this element is specifically PRCP
prcpcount = stations3x.filter((stations3x['all_elem_count']==1) & (stations3x['element']=='PRCP'))
#rename enriched stations table to prep_stations
prep_stations = stations3

#save enriched_table as 1 file
#prep_stations.repartition(1).write.format('com.databricks.spark.csv').save('/user/nar78/enrichedstations')



#to find if there are stations in daily 1000 subset not in stations
stations4 = daily.join(stations3, how='left', on='station_id')  
notinstations = stations4.filter(F.col('country_code').isNull())
#alternative way using collect_set
s= daily.select(F.collect_set('station_id').alias('station_id')).first()['station_id']
q= prep_stations.select(F.collect_set('station_id').alias('station_id')).first()['station_id']
#set(s).issubset(set(q)) 


# *** Analysis ***

#Q1A
#prep_stations.count()
til2017 = prep_stations.filter((prep_stations['min(first_year)']==2017) | (prep_stations['max(last_year)']==2017))
GSNcount = prep_stations.groupBy("gsn_flag").agg(F.countDistinct("station_id"))
HCcount = prep_stations.groupBy("hcn_crn_flag").agg(F.countDistinct("station_id"))
hcn_crn = ['HCN', 'CRN']
GSN_HCcount = prep_stations.filter(prep_stations['hcn_crn_flag'].isin(hcn_crn)&prep_stations['gsn_flag'].isin('GSN'))

#Q1B
countries_prep_stations=prep_stations.join(countries, how='left', on='country_code')
countries_prep_stations=countries_prep_stations.groupBy('country_code').agg(F.countDistinct('station_id'))
countries_prep_stations = countries_prep_stations.withColumnRenamed('count(DISTINCT station_id)', 'num_stations')
countries = countries.join(countries_prep_stations, how='left', on='country_code')
countries_an_q2b = countries.alias('countries_an_q2b')
#countries_an_q2b.write.csv('countries_an_q2b.csv')

states_prep_stations=prep_stations.join(states, how='left', on='state_code')
states_prep_stations=states_prep_stations.groupBy('state_code').agg(F.countDistinct('station_id'))
states_prep_stations = states_prep_stations.withColumnRenamed('count(DISTINCT station_id)', 'num_stations')
states = states.join(states_prep_stations, how='left', on='state_code')
states_an_q2b = states.alias('states_an_q2b')
#states_an_q2b.write.csv('states_an_q2b.csv')

#Q1C
southern_stations=prep_stations.filter(prep_stations['latitude']<0)
southern_stations.count()

total_us_stations = prep_stations.filter(prep_stations['country_name'].contains('United States'))
us_main_stations = prep_stations.filter(prep_stations['country_code'] == 'US')
us_non_main = total_us_stations.join(us_main_stations, on='station_id', how='left_anti')

#Q2A

#fxn source: https://medium.com/@petehouston/calculate-distance-of-two-locations-on-earth-using-python-1501b1944d97

from math import radians, degrees, sin, cos, asin, acos, sqrt
def great_circle(lon1, lat1, lon2, lat2):
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    return 6371 * (
        acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon1 - lon2))
    )

from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import DoubleType

gcircle_udf = UserDefinedFunction(great_circle, DoubleType())

#Q2B

nz_stations = prep_stations.filter(prep_stations['country_code'] == 'NZ')
nz_dists = nz_stations.select(F.col('station_id'), F.col('latitude'), F.col('longitude'), F.col('name'))
new_names = ['station_id1', 'lat1', 'lon1', 'name1']
nz_dists = nz_dists.toDF(*new_names)
new_names2 = ['station_id2', 'lat2', 'lon2', 'name2']
nz_dists2= nz_dists.toDF(*new_names2)
nz_dists = nz_dists.crossJoin(nz_dists2)



nz_dists = nz_dists.withColumn('distance', gcircle_udf('lon1', 'lat1', 'lon2', 'lat2'))
nz_dists= nz_dists.filter(nz_dists['distance'] > 1.0)
nz_dists = nz_dists.dropDuplicates(['distance'])
#save
#nz_dists.repartition(1).write.format('com.databricks.spark.csv').save('/user/nar78/nz_dists')

orderednz = nz_dists.orderBy('distance')

# **** Bash commands used to get tree structure of files  ****

#Q3A

hdfs getconf -confKey dfs.blocksize

hdfs fsck /data/ghcnd/daily/2017.csv.gz -blocks -files

hdfs fsck /data/ghcnd/daily/2010.csv.gz -blocks -files


# **** Bash commands end here ****


#Q3B
#2010 load
daily2010 = (
spark.read.format('com.databricks.spark.csv')
.option('dateFormat', 'YYYYMMDD')
.option('timestampFormat', 'HHMM')
.load('hdfs:///data/ghcnd/daily/2010.csv.gz', schema=daily_schema)

)

# daily2010.count()

#2 stages

#2017 load
daily2017 = (
spark.read.format('com.databricks.spark.csv')
.option('dateFormat', 'YYYYMMDD')
.option('timestampFormat', 'HHMM')
.load('hdfs:///data/ghcnd/daily/2017.csv.gz', schema=daily_schema)

)

# daily2017.count()

#Q3C

#2010-2015 load
daily201015 = (
spark.read.format('com.databricks.spark.csv')
.option('dateFormat', 'YYYYMMDD')
.option('timestampFormat', 'HHMM')
.load('hdfs:///data/ghcnd/daily/20{10,11,12,13,14,15}*', 
schema=daily_schema)

)

# daily201015.count()




#Q4a


daily_schema = StructType([
StructField("station_id", StringType(), True),
StructField("date", StringType(), True),
StructField("element", StringType(), True),
StructField("element_value", DoubleType(), True),
StructField("measurement_flag", StringType(), True),
StructField("quality_flag", StringType(), True),
StructField("source_flag", StringType(), True),
StructField("observation_time", StringType(), True)

])

daily = (
spark.read.format('com.databricks.spark.csv')
.option('dateFormat', 'YYYYMMDD')
.option('timestampFormat', 'HHMM')
.load('hdfs:///data/ghcnd/daily', schema=daily_schema)


)


#Q4b
#num obs for whole daily
#daily.count()

#daily filtered on core elems
daily2=daily.filter(daily['element'].isin(core))
#counts for each core elem in whole daily
# daily2.groupBy('element').agg(F.count('element')).orderBy('count(element)').show()

#Q4c

#filter so that only tmin and tmax show
daily3=daily.filter((daily['element']=='TMIN') | (daily['element']=='TMAX'))
#do collect_set to get distinct elems as array col
daily4=daily3.groupBy('station_id', 'date').agg(F.collect_set('element')).withColumnRenamed('collect_set(element)', 'elemset')
#create tmin and tmax flags and populate by checking if tmin and tmax exist for each array
daily5=daily4.withColumn("TMIN_flag", F.array_contains(F.col('elemset'),"TMIN")).withColumn("TMAX_flag", F.array_contains(F.col('elemset'),"TMAX"))
#filter where tmin flag is true but tmax flag is false: faulty daily
daily6 = daily5.filter((daily5['TMIN_flag'] == 'true')&(daily5['TMAX_flag'] == 'false'))

#select subset of stations to join with faulty obs daily table
stationsfornetwork = stations.select(F.col('station_id'), F.col('gsn_flag'), F.col('hcn_crn_flag'))
#create GSN and HCN/CRN flags
stationsfornetwork2 = stationsfornetwork.filter((stationsfornetwork['gsn_flag']=='GSN') | ((stationsfornetwork['hcn_crn_flag']=='CRN')| (stationsfornetwork['hcn_crn_flag']=='HCN')))
daily7 = daily6.dropDuplicates(['station_id'])
#join with faulty daily (daily6)
networkdaily = daily7.join(stationsfornetwork2, on='station_id', how='inner')

#count faulty obs for each station
# networkdaily.groupBy('gsn_flag').count().show()
# networkdaily.groupBy('hcn_crn_flag').count().show()

#Q4d

#join daily with broadcasted stations to get country column
countrytm= daily3.join(F.broadcast(prep_stations.select('station_id','country_code', 'name')),on='station_id', how='left')
#filter on NZ
nztm=countrytm.filter(countrytm['country_code']=='NZ')
#create year column
nztm=nztm.withColumn('date2', nztm.date.substr(1, 4).cast(IntegerType()))
#save nztminmax
nztm.write.format('com.databricks.spark.csv').save('/user/nar78/nztminmax')


# **** Bash commands used to get row count for all files ****


#ls | xargs -wc


# **** Bash commands end here ****


#create yearly NZ avgs and count
nztm2 = nztm.groupBy('date2').agg({'element_value':'avg'})
# nztm2.count()
#save for plotting in R
# nztm.repartition(1).write.format('com.databricks.spark.csv').save('/user/nar78/nztman')


# **** R CODE FOR PLOTTING NZ TIME SERIES ****

#Import packages
# library(tidyverse)
# library(lubridate)
# library(scales)
# library(ggplot2)
# dfn <- read.csv('/Users/nicorabang/Desktop/nztman.csv', header = FALSE)

# #Use tidyverse for data manipulation for time series and column renaming
# str(dfn)
# dfn <- dfn %>% as.tibble()
# dfn<- dfn %>% rename(station_id=V1, date=V2, minmax=V3, value=V4, name=V10) 
# dfn <- dfn %>% mutate(date = ymd(date))

# dfn2 <- dfn %>% mutate(year = year(dfn$date))
# avefn2 <- dfn2 %>% group_by(date,minmax) %>%  summarise(mean_rain= mean(value))

# #plot with GGPlot2, use facet wrap to plot 15 stations
# ggplot(data = dfn, aes(x = date, y = value))+
#   geom_line(aes(color=dfn$minmax)) + (scale_x_date(breaks=date_breaks("15 years"),
#                                                    labels=date_format("%b %y"))) + xlab("Time of Observation") +
#   ylab("Y") +facet_wrap(~name)

# ggplot(data = avefn2, aes(x = date, y = mean_rain))+
#   geom_line(aes(color=avefn2$minmax)) + (scale_x_date(breaks=date_breaks("10 years"),
#                                                    labels=date_format("%b %y"))) + xlab("Time of Observation") +
#   ylab("Y") 

# **** RC CODE ENDS HERE****


#Q4e

#filter daily on PRCP
prcpdaily = daily.select(F.col('station_id'), F.col('date'), F.col('element'), F.col('element_value')).filter(daily['element']=='PRCP')
#create year column as string
prcpdaily2 = prcpdaily.withColumn('date2', prcpdaily.date.substr(1, 4))
#convert string year to int
prcpdaily2 = prcpdaily2.select(F.col('station_id'), F.col('date2').cast(IntegerType()), F.col('element'), F.col('element_value'))
#join daily to broadcasted station to get country code, country name, min of first_year and station_id
prcpdaily3 = prcpdaily2.join(F.broadcast(prep_stations.select('station_id', 'country_name', 'country_code', 'min(first_year)')), on='station_id', how='left')
#group by year and country name and aggregate as avg, then order
prcpdaily4 = prcpdaily3.groupBy('date2', 'country_name', 'country_code').agg({'element_value':'avg'}).orderBy(F.desc('avg(element_value)'))

#pure cumulative sums for rainfall
prcpdaily5 = prcpdaily3.groupBy('country_name','country_code').agg({'element_value':'sum'})
prcpdaily5 = inv2.withColumnRenamed('sum(element_value)', 'cumu_rain')
#save for exporting cumulative sums
prcpdaily5.repartition(1).write.format('com.databricks.spark.csv').save('/user/nar78/xPRCP')

# **** Python code used to plot Choropleth ****

# import geopandas as gpd
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# shapefile = '/Users/nicorabang/Desktop/TM_WORLD_BORDERS-0.3/TM_WORLD_BORDERS-0.3.shp'
# #Read shapefile using Geopandas into gpd dataframe
# gdf = gpd.read_file(shapefile)
    
# #sort gpd dataframe
# gdf= gdf.iloc[gdf['NAME'].sort_values().index.values]
# gdf['num'] = np.arange(len(gdf))
# gdf

# #load csv from Spark
# df = pd.read_csv('/Users/nicorabang/Desktop/xPRCP.csv')
# df

# #left join gpd dataframe and results from prcpdaily4 and sort
# merged = pd.merge(gdf, df, on='FIPS')
# merged = merged.sort_values(by=['cumu_rain'], ascending=False)
# merged.head(10)

# #plot choropleth
# variable = 'cumu_rain'
# fig, ax = plt.subplots(1, figsize=(10, 6))
# vmin, vmax = 1, 100
# merged.plot(column=variable, cmap='Blues', linewidth=0.8, edgecolor='0.8',ax=ax)
# ax.set_title('Average Precipitation', fontdict={'fontsize': '20', 'fontweight' : '3'})
# plt.axis('off')

# # Create colorbar as a legend
# sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=vmin, vmax=vmax))
# # empty array for the data range
# sm._A = []
# cbar = plt.colorbar(sm)
# fig1 = plt.gcf()
# fig1.savefig('prcpmap.png', dpi=1800)


# **** Python code used  for first try to plot Choropleth ends here ****

#investigation of choropleth imbalance
prcpdaily4.describe('avgforyear').show()

#export csv for yearly averages for countries for processing in R and plotting in Python

prcpdaily4.repartition(1).write.format('com.databricks.spark.csv').save('/user/nar78/prcpdaily4')


# # ***** R re-processing for outliers *****
# #load R packages
# library(tidyverse)
# library(ggplot2)
# df=read.csv('/Users/nicorabang/Desktop/prcpdaily4.csv', header=FALSE)
# df = as_tibble(df)
# df<- df %>% rename(year=V1, country=V2, FIPS=V3, avgrain=V4) 

# #look at data
# df = df %>% arrange(desc(avgrain))
# df

# #initial plot
# p = ggplot(df, aes(x=avgrain)) + geom_histogram(bins=15)
# p+ geom_vline(aes(xintercept=mean(avgrain)),
#                 color="blue", linetype="dashed", size=1)

# #check desciptive stats
# dfsummary=summary(df$avgrain)
# dfsummary

# #calculate upper bound
# iqrdf=IQR(df$avgrain)
# meandf=mean(df$avgrain)
# upper = dfsummary[5] + iqrdf
# upper #this is the upper bound as defined by 1.5*IQR + 3rd Quartile. Equal to 77

# df2 = df %>% filter(avgrain < 77) %>% filter(avgrain > 0) # filter negative values and above upper

# #create new dataframe without outliers
# df3 = df2 %>%
#       group_by(FIPS, country) %>%
#       summarize(avgavgrain = mean(avgrain, na.rm = TRUE)) %>%
#       arrange(desc(avgavgrain))
      
# #replot
# p = ggplot(df2, aes(x=avgrain)) + geom_histogram(bins=20)
# p
# p+ geom_vline(aes(xintercept=mean(df2$avgrain),
#               color="blue", linetype="dashed", size=1))

# #check summaries again
# summary(df3$avgavgrain)
# #save
# write.csv(df3, file = "procdaily.csv")

# # ***** R code ends here *****

# import geopandas as gpd
# import pandas as pd
# import numpy as np
# import matplotlib.pyplot as plt
# shapefile = '/Users/nicorabang/Desktop/TM_WORLD_BORDERS-0.3/TM_WORLD_BORDERS-0.3.shp'
# #Read shapefile using Geopandas into gpd dataframe
# gdf = gpd.read_file(shapefile)
    
# #sort gpd dataframe
# gdf= gdf.iloc[gdf['NAME'].sort_values().index.values]
# gdf['num'] = np.arange(len(gdf))
# gdf

# #load csv from Spark
# df = pd.read_csv('/Users/nicorabang/Desktop/procdaily.csv')
# df

# #left join gpd dataframe and results from prcpdaily4 and sort
# merged = pd.merge(gdf, df, on='FIPS')
# merged = merged.sort_values(by=['avgavgrain'], ascending=False)
# merged.head(10)

# #plot choropleth
# variable = 'avgavgrain'
# fig, ax = plt.subplots(1, figsize=(10, 6))
# vmin, vmax = 1, 100
# merged.plot(column=variable, cmap='Blues', linewidth=0.8, edgecolor='0.8',ax=ax)
# ax.set_title('Average Precipitation', fontdict={'fontsize': '20', 'fontweight' : '3'})
# plt.axis('off')

# # Create colorbar as a legend
# sm = plt.cm.ScalarMappable(cmap='Blues', norm=plt.Normalize(vmin=vmin, vmax=vmax))
# # empty array for the data range
# sm._A = []
# cbar = plt.colorbar(sm)
# fig1 = plt.gcf()
# fig1.savefig('prcpmap.png', dpi=1800)



