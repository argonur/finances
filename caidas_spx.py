"""
This script intends to detect the x% or more declines 
"""

import csv

declinePercentage = 10.0

with open('HistoricalData_spx.csv', 'r') as csvfile:

    data = csv.reader(reversed(list(csvfile)), delimiter = ',')

    declineFound = False
    i = 0

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
                print(row[0])
                print("**************************************************")

            i+=1

            if maximumValue < float(row[1]) :
                maximumValue = float(row[1])
                maximumDate = row[0]
                declineValue = maximumValue * (1.0 - declinePercentage/100.0)
                # check for all time high
                if maximumValue > allTimeHigh :
                    allTimeHigh = maximumValue

            elif declineValue >= float(row[1]) :
                inDecline = True
                declineDate = row[0]
           

           ##### !!!!! Baustelle
            # check for all time minimum
            if minimumValue > float(row[1]) :
                minimumValue = float(row[1])
                allTimeMinimumDate = row[0]
        
            if inDecline :
                minimumValue = float(row[1])

            if declineFound :
                declineFound = False
                print("**************************************************")
                print("Decline found!")
                print("Maximum")
                print(maximumDate)
                print(maximumValue)
                print("Decline")
                print(declineDate)
                print(declineValue)
                maximumValue = float(row[1])
                declineValue = maximumValue * (1.0 - declinePercentage/100.0)
                print("**************************************************")
                

        # skip titles
        except :
            #print("Exception found: " + str(i))
            #print(row[0] + "    " + row[1])
            #print("maximumValue:" + str(maximumValue))
            #print("minimumValue:" + str(minimumValue))
            #print("declineValue:" + str(declineValue))
            continue

