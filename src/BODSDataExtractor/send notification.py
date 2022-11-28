# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 09:05:00 2022

@author: aakram7
"""

import os



import pandas as pd

from selenium import webdriver

from selenium.webdriver.common.by import By

from selenium.webdriver.edge.options import Options

import time


from datetime import date
from datetime import datetime

from datetime import timedelta

from BODSDataExtractor.otc_db_download import fetch_otc_db

otc=fetch_otc_db()



#bring in an extract

test_xml_table = pd.DataFrame(columns = ['URL', 'FileName', 'NOC', 'TradingName', 'LicenceNumber', 'OperatorShortName', 'OperatorCode', 'ServiceCode', 'LineName', 'PublicUse', 'Origin', 'Destination', 'OperatingPeriodStartDate', 'OperatingPeriodEndDate', 'SchemaVersion', 'RevisionNumber','journey_pattern_json'] )

test_xml_table['URL']=['https://data.test.bus-data.dft.gov.uk/timetable/dataset/102/download/','https://data.test.bus-data.dft.gov.uk/timetable/dataset/102/download/', 'https://data.test.bus-data.dft.gov.uk/timetable/dataset/324/download/', 'https://data.test.bus-data.dft.gov.uk/timetable/dataset/1014/download/','https://data.test.bus-data.dft.gov.uk/timetable/dataset/1006/download/', 'https://data.test.bus-data.dft.gov.uk/timetable/dataset/913/download/', 'https://data.test.bus-data.dft.gov.uk/timetable/dataset/950/download/']

test_xml_table['Expired_Operator']=[True,True,False,False, "No End Date", True, True]

test_xml_table["Origin"]=["Eton","Bilton Circular", "Harrogate", "Harrow", "Iran", "England", "Pateley Bridge"]

test_xml_table['ServiceCode']=["PBBBBBB","PB0001748:116","PB0001748:116","PB0001748:116","PB0001748:140","PB0001748:350","PH0000132:3"]

test_xml_table['LineName']=[300, 400,500,600,700,600,900]



#USING A SERVICE LINE EXTRACT LOCALLY




#username = 

# enter username below after "=" as string ('')

Username=os.environ.get("Username")

#password =

# enter password below after "=" as string ('')

password = os.environ.get("password")


feedbackDataFrame= pd.DataFrame(columns=["URL","Route Origin",'ServiceCode', 'LineName',"Time Acessed","Message"])


# add some data to feedback dataframe

URLsentfeedback=['https://data.test.bus-data.dft.gov.uk/timetable/dataset/950/feedback']

TIMESENDFEEDBACK=["2022-11-05 14:03:12.829719"]

MESSAGESENDFEEDBACK=["Message"]

ROUTEORIGIN=["Pateley Bridge"]

LINENAME=[600]

SERVICECODEDF=["PB0001748:116"]



feedbackDataFrame["URL"]=URLsentfeedback
feedbackDataFrame["Time Acessed"]=TIMESENDFEEDBACK
feedbackDataFrame["Message"]=MESSAGESENDFEEDBACK
feedbackDataFrame["Route Origin"]=ROUTEORIGIN
feedbackDataFrame["LineName"]=LINENAME
feedbackDataFrame["ServiceCode"]=SERVICECODEDF




#index= list(feedbackDataFrame["URL"].values).index('https://data.test.bus-data.dft.gov.uk/timetable/dataset/950/feedback')


from BODSDataExtractor.otc_db_download import fetch_otc_db

otc=fetch_otc_db()

#current line name and current service code are wrong way round!!!!


def feedbackLog(current_Url,current_Origin, CurrentLineName, CurrentServiceCode, username, password, data_for_url,data_added):
    
    '''Using the data from the expired operator as taken from the checkExpiredFlag function, we make sure if we need to send a message based on the below conditions'''
    

    #if the service code is in the OTC Database and it hasn't expired, we don't do anything    

    if CurrentServiceCode in otc["service_code"].values:
        

        index_of_service_code_in_otc= list(otc["service_code"].values).index(CurrentServiceCode)

        expiry=otc["exp_date"].iloc[index_of_service_code_in_otc]
        
        
        
        
        expiry_date=datetime.strptime(expiry,"%d/%m/%y")

        
        otc_expired=(datetime.now()>expiry_date)
        
        if (otc_expired==True):
            print("Dont send anything, service code has not expired in OTC database")
            return
    
    
        
    #If the url (dataset id) has already been added to our feedback log..
    
    if current_Url in feedbackDataFrame["URL"].values:
        
        indexd= list(feedbackDataFrame["URL"].values).index(current_Url)
        
        dateTimeforURL = feedbackDataFrame["Time Acessed"].iloc[indexd]
        
        dateTimeforURL=str(dateTimeforURL)
        
        dateTimeforURL= datetime.strptime(dateTimeforURL,'%Y-%m-%d %H:%M:%S.%f')
        
        
        #Checking if feedback has been sent more than 6 days ago and this route origin is already in our feedback dataframe  
        

        if ((datetime.now()-dateTimeforURL).days)>6 and (CurrentServiceCode in feedbackDataFrame["ServiceCode"].values and CurrentLineName in feedbackDataFrame["LineName"]) :

            
            #Send this information to the sendnotification function
            
            sendNotification(current_Url, current_Origin, CurrentServiceCode, CurrentLineName, username, password, data_for_url,data_added)
            
            print("need to send feedback because feedback has expired for this service code")
            
           
         #Checking if feedback has been sent more than 6 days ago and this route origin is not already in our feedback dataframe
         
        elif ((datetime.now()-dateTimeforURL).days)>6 and CurrentServiceCode not in feedbackDataFrame["ServiceCode"].values :
            
            
            #Send this information to the sendnotification function
            
            sendNotification(current_Url, current_Origin, CurrentServiceCode, CurrentLineName, username, password, data_for_url,data_added)
            
            print("Feedback needed because we haven't sent feedback for this service code yet")
        
        else:
            print("Don't send anything feedback has been sent recently")
            
    else:
        
        #Send this information to the sendnotification function because we have not collected any of this information
        
        sendNotification(current_Url, current_Origin, CurrentServiceCode, CurrentLineName, username, password, data_for_url,data_added)
        print("need to send feedback because no existing feedback")
        

    
    




def sendNotification(current_Url, current_Origin, CurrentLineName, CurrentServiceCode, username, password, data_for_url,data_added):
    
    
    '''Using the data passed from the feedbacklog function, we are appending to a list that will be used to send feedback to operators'''


# Combining all data associated to a specific URL
      
    
    count=0
    #loops_for_rows=0
    for urls in data_for_url:

          
        
        
        
        
        for item in urls:

            
            index=data_for_url.index(urls)
            if item==current_Url and count<1:
                data_for_url[index].append(CurrentServiceCode)
                data_for_url[index].append(CurrentLineName)
                data_for_url[index].append(current_Origin)
                count+=1
                
                
                
                #sendsomething(data_for_url)
                
                continue
            
            
     # add on to url data row that is already in data_for_url list           
            
            elif item==current_Url and count>1:
                
                print("doing this")
                
                index=index-1
                data_for_url[index].append(CurrentServiceCode)
                data_for_url[index].append(CurrentLineName)
                data_for_url[index].append(current_Origin)
                
                continue


    
    #we only send this list to the next function once we have looked at all rows in the dataframe
    
    
    if len(data_for_url)==len(data_added):
        sendsomething(data_for_url)   
        
    return data_for_url




def sendsomething(data_for_url):
    
    '''First this function is creating a unique message to be sent to operators, then using selenium we send this message'''


    # creating unique feedback message after looking at all of the smaller lists within the data_for_url_list
    
    for result in data_for_url:
        if len(result)>1:
            
            
            message="Hi there,\nWe have noticed that your data has expired for the following:" 
            
            count=1
            
            for value in range(1, (len(result))):
                

                
                if count==1:
                    message= message + "\n-Line Number: " + str(result[value])
                    count=count+1
                    
                elif count==2:
                    message= message + "\n-Service Code: " + str(result[value])
                    count=count+1
                
                elif count==3:
                    message= message + "\n-Originating at: " + str(result[value])
                    count=count+1
                  
                    
            #if the length of our smaller list is greater than 3, we need to add the additonal data to our message
                elif count==4:   
                    message= message+ "\nand "
                    message= message + "\n-Line Number: " + str(result[value])
                    count=2
                    continue
            
            #if the length of our smaller list is greater than 3, we need to add the additonal data to our feedback log
            
            if value>3:
                
                #these are added to once we ru
                subtractor=2
                subtractorb=3
                subtractorc=4
                
                check=int(value/3)
                
                for index in range(check):
                    
                    #add details to feedback datframe to keep track of messages
                    
                    feedbackDataFrame.loc[len(feedbackDataFrame.index)]=[result[0], result[value-subtractorb], result[value-subtractorc], result[value-subtractor], datetime.now(), message]
                    
                    subtractor=subtractor+3
                    subtractorb=subtractorb+4
                    subtractorc=subtractorc+4
                    
                    
                    
                
            else:
                #add details to feedback datframe to keep track of messages
                
                feedbackDataFrame.loc[len(feedbackDataFrame.index)]=[result[0], result[value], result[value-1], result[value-2], datetime.now(), message]
                
            
            print("---------------------------------------------")
            print("send it to-------", result[0])
            
            print(message)
            print("________________________________________________")
            
            

            #send information to this url
            
            
            urltosend=result[0]
            
            print("sending......")
            
            print(urltosend)
            
            
            # location of selenium web driver- install based on your browser and add your own installation location here...
            
            driver= webdriver.Edge(r"C:\Users\aakram7\OneDrive - KPMG\Documents\My Office Files\BODS Python\bods-dasta-extractor-operating period expired\edge")


            driver.get(urltosend)

            #login details entered and various page interactions....
            
            login= driver.find_element( By.NAME, "login")

            login.send_keys(Username)

            loginPassword= driver.find_element( By.NAME, "password")

            loginPassword.send_keys(password)

            driver.find_element(By.NAME,"submit" ).click()
               
            time.sleep(1)



            feedback= driver.find_element( By.NAME,'feedback')

            feedback.send_keys(message)
               
            time.sleep(1)



def checkExpiredFlag():
    
    '''Takes key information from the service extract dataframe and iterates it to appropriately named lists'''

    count=0
    listofURLS=[]
    listofOrigins=[]
    listofServiceCodes=[]
    listofLineNames=[]
    
    #creating list to group dataset infomation together
    data_for_url=[]
    #Makes a note of all the datasets looked at in the code
    data_added=[]
    
    #Changing the url from the service extract database so we can go straight to the feedback page
    for value in test_xml_table['URL']:
        stripped_value=value.strip('/download')
        stripped_value=stripped_value+"/feedback"
   
        listofURLS.append(stripped_value)
    
    
    #Converting service extract collumns to lists....
    

    #list of lists created to keep dataset information together
    for url in listofURLS:
            data_for_url.append([url]) 
        
    for origin in test_xml_table['Origin']:
        listofOrigins.append(origin)
        
    for service_code in test_xml_table['ServiceCode']:
        listofServiceCodes.append(service_code)
        
    for line_name in test_xml_table["LineName"]:
        listofLineNames.append(line_name)
        
    
    
    #Checking through all rows where the Expired operator status
    #for every row in dataframe
    
    
    for value in test_xml_table['Expired_Operator']:
        
        data_added.append("operator")
        
        if value== True:
            
            
            #If the expired operator status is true, we make a note of the below information associated with it's index
            
            current_Url= listofURLS[count]
            current_Origin=listofOrigins[count]
            CurrentServiceCode=listofServiceCodes[count]
            CurrentLineName=listofLineNames[count]

            #Passing all this information to the feedback log function
            
            feedbackLog(current_Url,current_Origin, CurrentLineName, CurrentServiceCode, Username, password, data_for_url,data_added)
            
    
            count=count+1
            
            continue
        
        count=count+1
        



checkExpiredFlag()  

# print(test_xml_table['URL'][count])
