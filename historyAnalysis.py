#!/usr/bin/env python3
"""Analyse historical data of financial assets

Usage:

    Make the python script executable (chmod +x historyAnalysis.py) and call it as follows
    ./historyAnalysis index percentage function
    where 
        index: spx, nasdaq
        percentage: the percentage
        function: the function (declines by default, currently the only one available)
"""


import csv
import sys
import datetime


def readFile(fileName):
    """Reads a csv file in an inverted order and stores it in a collection

      Args:
          fileName: The fully qualified name of the file

      Returns:
          A collection with the inverted content of the csv file
    """
    with open(fileName, 'r') as csvfile:
        data = csv.reader(reversed(list(csvfile)), delimiter = ',')
        return data


def printRows(data):
    """Prints all the rows in a collection.

      Args:
        data: The collection
    """
    for row in data:
        print(row)


def humanReadableDate(date):
    """Coverts a date string with monkey format(%m/%d/%Y) into a human readable one (%d.%m.%Y)

      Args:
          date: string containing a date in a monkey format (%m/%d/%Y)

      Returns:
          A date in a human readable format (%d.%m.%Y)
    """
    dateVar = datetime.datetime.strptime(date, '%m/%d/%Y').date()
    formatedDate = datetime.datetime.strftime(dateVar, '%d.%m.%Y')
    return formatedDate


def calculateDuration(date1, date2):
    """Calculates duration in days between to dates

      Args:
          date1: string containing the start date in a monkey format (%m/%d/%Y)
          date1: string containing the end date in a monkey format (%m/%d/%Y)

      Returns:
          Duration in days between start and end dates
    """
    dateVar1 = datetime.datetime.strptime(date1, '%m/%d/%Y').date()
    dateVar2 = datetime.datetime.strptime(date2, '%m/%d/%Y').date()
    timeDelta = dateVar2 - dateVar1
    return timeDelta.days

def findLastDate():
    pass

def updateHistoricalData():
    pass

def findDeclines(index, percentage, path='data/HistoricalData_'):
    """Finds the number of declines given a percentage

      Args:
          fileName: The name of the file
          percentage: Decline percentage to search in the historic data
          path: The path where the file is located (data/ folder if not specified)

      Returns:
          Nothing at the moment
    """
    fileExtension = ".csv"
    qualifiedName = path + index + fileExtension
    data = readFile(qualifiedName)
    inDecline = False
    declineFound = False
    i = 0
    numberOfDeclines = 0
    startDate = ""
    endDate = ""
    declinesDuration = []

    for row in data:
        
        try :

            currentValue = float(row[1])
            currentDate = row[0]

            # initialize data
            if i == 0 :
                allTimeHigh = currentValue
                maximumValue = currentValue
                allTimeMinimum = currentValue
                minimumValue = currentValue
                declineValue = maximumValue * (1.0 - percentage/100.0)
                print("**************************************************")
                print("Data initialized")
                print("Searching declines greater or equal to " + str(percentage) + "%")
                startDate = currentDate
                print("Starting Date: " + humanReadableDate(startDate))
                print("**************************************************")

            i+=1

            if maximumValue < currentValue :
                maximumValue = currentValue
                    
                # check for all time high
                if maximumValue >= allTimeHigh :

                    if inDecline :
                        print("Minimum: " + str(minimumValue) + " at " + humanReadableDate(minimumDate))
                        maximumDecline = 100 * (1 - minimumValue/allTimeHigh)
                        print("Decline of: " + str(round(maximumDecline, 2)) + "%")
                        print("Decline end at " + humanReadableDate(currentDate))
                        declineDuration = calculateDuration(allTimeHighDate, currentDate)
                        declinesDuration.append(declineDuration)
                        print("Decline duration: " + str(declineDuration) + " days")
                        print("**************************************************")
                        
                    declineValue = maximumValue * (1.0 - percentage/100.0)
                    allTimeHigh = maximumValue
                    allTimeHighDate = currentDate
                    inDecline = False

            elif not inDecline:
                if declineValue >= currentValue :
                    inDecline = True
                    declineFound = True
                    declineDate = currentDate
                    print("**************************************************")
                    print("Maximum: " + str(allTimeHigh) + " at " + humanReadableDate(allTimeHighDate))
                    print("Decline found: " + str(currentValue) + " at " + humanReadableDate(declineDate))
        
            endDate = currentDate
            
            # check for all time minimum
            if allTimeMinimum > currentValue :
                allTimeMinimum = currentValue
                allTimeMinimumDate = currentDate
        
            if declineFound :
                declineFound = False
                numberOfDeclines+=1
                maximumValue = currentValue
                minimumValue = currentValue
                minimumDate = currentDate

            if inDecline :
                if minimumValue > currentValue :
                    minimumValue = currentValue
                    minimumDate = currentDate

        # skip titles
        except :
            #print("Exception found: " + str(i))
            #print(row[0] + "    " + row[1])
            if inDecline :
                print("Minimum: " + str(minimumValue) + " at " + humanReadableDate(minimumDate))
                maximumDecline = 100 * (1 - minimumValue/allTimeHigh)
                print("Maximum decline of: " + str(round(maximumDecline, 2)) + "% until now")
                currentDecline = 100 * (1 - currentValue/allTimeHigh)
                print("Current decline of " + str(round(currentDecline, 2)) + "% with " + str(currentValue) + " at the " + humanReadableDate(endDate))
                declineDuration = calculateDuration(allTimeHighDate, endDate)
                print("Elapsed days: " + str(declineDuration) + " days")
            continue

    print("**************************************************")
    print("Start date: " + humanReadableDate(startDate))
    print("End date: " + humanReadableDate(endDate))
    print("All time high: " + str(allTimeHigh) + " at " + humanReadableDate(allTimeHighDate))
    print("All time minimum: " + str(allTimeMinimum) + " at " + humanReadableDate(allTimeMinimumDate))
    print("Total number of declines: " + str(numberOfDeclines))
    print("Average decline duration: " + str(int(getAverage(declinesDuration))) + " days")
    print("**************************************************")

def findPeriods():
    """Creates a list of all the periods from a given time frame

      Args:
          initialDate: initial date
          endDate: end date
          percentage: the minimum decline percentage after a peak

      Returns:
          A list with all the periods found
    """
    return

def getAverage(list):
    return sum(list)/len(list)


def displayHelp():
    pass

def printArguments():
    print(sys.argv[0])
    print(sys.argv[1])
    print(sys.argv[2])


def main(index, percentage, function = "decline"):
    printArguments()
    if function == "decline":
        findDeclines(index, percentage)


if __name__ == '__main__' :
    index = sys.argv[1]
    percentage = float(sys.argv[2])
    function = "decline"
    main(index, percentage, function)
