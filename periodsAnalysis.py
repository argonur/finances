#!/usr/bin/env python3
"""
"""


import csv
import sys
import datetime


class Instant:
    """A date and value"""

    def __init__(self, date, value):
        self.date = date
        self.value = value

    def __str__(self) -> str:
        return "%s at %s" % (self.value, self.date) 


class Period:
    """Class to store and analyse relevant information from maximum to maximum"""

    def __init__(self, maximum1 = 0, decline = 0, minimum = 0, declineEnd = 0, maximum2 = 0):
        self.maximum1 = maximum1
        self.decline = decline
        self.minimum = minimum
        self.declineEnd = declineEnd
        self.maximum2 = maximum2

    def __str__(self) -> str:
        info = "Initial maximum: " + str(self.maximum1) + "\n" \
            "Decline: " + str(self.decline) + "\n" \
            "Minimum: " + str(self.minimum) + "\n" \
            "Decline end: " + str(self.declineEnd) + "\n" \
            "Final maximum: " + str(self.maximum2)
        return info

    def calculateDuration(self, date1, date2):
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


def getAverage(list):
    return sum(list)/len(list)


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
    periodsList = []
    period1 = None
    period2 = None

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
                        minimum = Instant(humanReadableDate(minimumDate), minimumValue)
                        maximumDecline = 100 * (1 - minimumValue/allTimeHigh)
                        print("Decline of: " + str(round(maximumDecline, 2)) + "%")
                        print("Decline end at " + humanReadableDate(currentDate))
                        declineEnd = Instant(humanReadableDate(currentDate), currentValue)
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
                    
                    if period1 == None:
                        pass
                    else:
                        pass


                    print("Maximum: " + str(allTimeHigh) + " at " + humanReadableDate(allTimeHighDate))
                    maximum1 = Instant(humanReadableDate(allTimeHighDate), allTimeHigh)
                    print("Decline found: " + str(currentValue) + " at " + humanReadableDate(declineDate))
                    decline = Instant(humanReadableDate(declineDate), currentValue)
        
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


def main():
    pass


def test():
    instant = Instant("12.10.1985", 10000)
    period = Period(instant, instant, instant, instant, instant)
    print(period)


if __name__ == '__main__' :
    test()