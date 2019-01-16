# Spark Homework

This application uses spark to get needed data from the hotels dataset

## Prerequisites

Please install Java 8 JDK, Scala and SBT. More info: https://www.scala-lang.org/download/

## Getting Started

The recommend way of use is to build jar using SBT plugin and run it with parameter using scala command

* Build project with SBT assembly plugin: (run command from the project root)
```
sbt assembly
```
* Find generated jar and run it with 1 parameter using scala:
```
scala pathToJarFile.jar parameter

pathToJarFile.jar: Path to the generated jar file
parameter: Path to the csv file
```

## HotelUtils methods

```
createHotelRDD
```
Method to create dataset of Hotels from the csv file
@param context Spark Context
@param csvPath Path to the csv file
@throws InvalidInputException if path to the csv file is not correct
@return Resilient Distributed Dataset (RDD) of Hotels

```
get3MostPopularCoupleHotels
```
Method to find top 3 most popular hotels between couples
@param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
@return Array[((continent, country, market), number)]
        Array of hotels and their number in the dataset.
        Hotels are treated as composite keys of continent, country and market.

```
getMostPopularCountryHotelsBookedAndSearchedFromSameCountry
```
Method to find the most popular country where hotels are booked and searched from the same country
@param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
@return Array[(country, number)]
        Array of countries and their number in the dataset.

```
get3TopHotelsPeopleWithChildrenInterestedButNotBooked
```
Method to find top 3 hotels where people with children are interested but not booked in the end
@param hotelRDD Resilient Distributed Dataset (RDD) of Hotels to proceed
@return Array[((continent, country, market), number)]
        Array of hotels and their number in the dataset.
        Hotels are treated as composite keys of continent, country and market.
    
## Running the tests

* Please open the following class file:
```
src/test/scala/app/HotelUtilsTest.scala
```
* Run `createHotelRDD()` method to test the hotels dataset creating
* Run `get3TopHotelsPeopleWithChildrenInterestedButNotBooked()` method to test finding top 3 hotels where people with children are interested but not booked
* Run `get3MostPopularCoupleHotels()` method to test finding top 3 most popular hotels between couples
* Run `get1MostPopularCountryHotelsBookedAndSearchedFromSameCountry()` method to test finding the most popular country where hotels are booked and searched from the same country