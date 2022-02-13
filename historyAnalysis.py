#!/usr/bin/env python3
"""Analyse historical data of financial assets

Usage:

    python3 historyAnalysis
"""


import csv
import sys


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
    endDate = ""

    for row in data:
        
        try :

            # initialize data
            if i == 0 :
                allTimeHigh = float(row[1])
                maximumValue = float(row[1])
                allTimeMinimum = float(row[1])
                minimumValue = float(row[1])
                declineValue = maximumValue * (1.0 - percentage/100.0)
                print("**************************************************")
                print("Data initialized")
                print("Searching declines greater or equal to " + str(percentage) + "%")
                print("Starting Date: " + row[0])
                print("**************************************************")

            i+=1

            if maximumValue < float(row[1]) :
                maximumValue = float(row[1])
                    
                # check for all time high
                if maximumValue >= allTimeHigh :

                    if inDecline :
                        print("Minimum: " + str(minimumValue) + " at " + minimumDate )
                        maximumDecline = 100 * (1 - minimumValue/allTimeHigh)
                        print("Decline of: " + str(round(maximumDecline, 2)) + "%")
                        print("Decline end at " + row[0])
                        print("**************************************************")
                        
                    declineValue = maximumValue * (1.0 - percentage/100.0)
                    allTimeHigh = maximumValue
                    allTimeHighDate = row[0]
                    inDecline = False

            elif not inDecline:
                if declineValue >= float(row[1]) :
                    inDecline = True
                    declineFound = True
                    declineDate = row[0]
                    print("**************************************************")
                    print("Maximum: " + str(allTimeHigh) + " at " + allTimeHighDate )
                    print("Decline found: " + row[1] + " at " + declineDate)
        
            endDate = row[0]
            
            # check for all time minimum
            if allTimeMinimum > float(row[1]) :
                allTimeMinimum = float(row[1])
                allTimeMinimumDate = row[0]
        
            if declineFound :
                declineFound = False
                numberOfDeclines+=1
                maximumValue = float(row[1])
                minimumValue = float(row[1])
                minimumDate = row[0]

            if inDecline :
                if minimumValue > float(row[1]) :
                    minimumValue = float(row[1])
                    minimumDate = row[0]

        # skip titles
        except :
            #print("Exception found: " + str(i))
            #print(row[0] + "    " + row[1])
            continue

    print("**************************************************")
    print("End date: " + endDate)
    print("All time high: " + str(allTimeHigh) + " at " +allTimeHighDate)
    print("All time minimum: " + str(allTimeMinimum) + " at " + allTimeMinimumDate)
    print("Total number of declines: " + str(numberOfDeclines))
    print("**************************************************")


#def displayHelp():


def printArguments():
    print(sys.argv[0])
    print(sys.argv[1])
    print(sys.argv[2])


def main(index, percentage, function = "decline"):
#    help(readFile)
    printArguments()
    if function == "decline":
        findDeclines(index, percentage)


if __name__ == '__main__' :

    index = sys.argv[1]
    percentage = float(sys.argv[2])
    function = "decline"
    main(index, percentage, function)
