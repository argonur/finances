"""
This script intends to detect the x% or more declines 
"""

import csv

declinePercentage = 7.0

with open('HistoricalData_spx.csv', 'r') as csvfile:

    data = csv.reader(reversed(list(csvfile)), delimiter = ',')

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
                declineValue = maximumValue * (1.0 - declinePercentage/100.0)
                print("**************************************************")
                print("Data initialized")
                print("Searching declines greater or equal to " + str(declinePercentage) + "%")
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
                        
                    declineValue = maximumValue * (1.0 - declinePercentage/100.0)
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