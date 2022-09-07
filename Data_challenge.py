from os import listdir
import os 
import pandas as pd
from pandas import read_excel
import numpy as np
import sys
import openpyxl
import time
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

cwd = os.getcwd()
filepath = cwd+'\input_file\\'
area_file = cwd+r'\area_in_blore\Areas_in_blore.xlsx'

def check ():
    list_files = listdir(filepath)
    for f in list_files:
        #1
        if os.path.isfile(filepath+f):
            print("Proccesing file " +f )
        else:
            print("Not files found")
            
        #2
        if os.stat(filepath+f).st_size == 0 :
                print("Empty File")
        else:
                print("File size OK")
        #3      
        if f.endswith('.csv'):
                print("The extension is .csv")
        else:
                print("Wrong extension")
         
        with open(cwd+'\processed_files.txt') as file:
            content = file.read()
            
            if f in content:                        
                print(f + "File already processed")
            else:
                process(f)
                
    
def process(file):

        df1 = pd.read_csv(filepath+file,  sep=',')  
        
        #1. Descriptive fields like address, reviews_list can be cleaned by removing
        #commas, period, exclamations or any other special/junk characters etc.
        
        df1['reviews_list'] = df1['reviews_list'].str.replace('[,]','').str.replace('[!]','').str.replace('[\n]','')
        df1['address'] = df1['address'].str.replace('[,]','').str.replace('[!]','').str.replace('[!]','')
 
        #2 Phone
        #df1 = pd.read_csv(filepath,  sep=',')
        
        #2. Data in the phone field can be validated for correct phone numbers.
        #A Any preceding “+” or spaces should be removed.
        #B Ensure phone numbers are correctly formatted.
        #C The field data can be split and stored in two separate fields e.g.
        #contact number 1 and contact number 2 for easy readability and access.
        
        ### A
        df1['phone'] = df1['phone'].apply(lambda a: str(a).replace('+','')).apply(lambda a: str(a).replace(' ',''))
        ### B
        
        df1['contact number 1'] = df1['phone'].replace({r'\r\n.*': ''}, regex=True)
        df1['contact number 2'] = df1['phone'].replace({r'.*\r\n': ''}, regex=True)
        df1['contact number 1'] = df1['contact number 1'].astype(str).apply(lambda x: '('+x[:3]+')'+x[3:7]+'-'+x[6:12])
        df1['contact number 2'] = df1['contact number 2'].astype(str).apply(lambda x: '('+x[:3]+')'+x[3:7]+'-'+x[6:12])
        df1['contact number 2'] = np.where(df1['contact number 2'] == df1['contact number 1'] , 'Unkown', df1['contact number 2'])
        
        
        #Checks for null values in fields which you think should not have null values.
        #df.isnull().values.any()
        #df1.isnull()
        
        df_clean = df1[df1['name'].isnull() == True]
        df_clean_phone = df1[df1['phone'].isnull() == True]
        df_clean_location = df1[df1['location'].isnull() == True]
        
        df_clean.append(df1[df1['phone'].isnull() == True])       
        df_clean.append(df_clean_phone, ignore_index = True)
        df_clean.append(df_clean_location, ignore_index = True)
        df_clean['Type_of_issue'] = 'Null fields'
      
        
        #Validation of location field for correctness of data by looking up to the Areas_in_blore.csv file.

        df_area = pd.read_excel(area_file,  engine='openpyxl')
        df1['check_area'] = df1.location.isin(df_area.Area).astype(int)
        df1['Row_num_list'] = df1.index.tolist()
        df1['Row_num_list'] = df1['Row_num_list'].add(2)
        
        df_no_area = df1[df1.check_area == 0]
        df_no_area['Type_of_issue'] = 'Wrong location'
        
        df1 = df1[df1.check_area == 1]
        
        #Your Own Data Quality Check: Please come up with one data quality check idea of your
        #own and incorporate it into the code. You can use any of the provided fields in the dataset.
        
        
        if len(df1.columns) != 21 :
            print("Wrong number of columns")
            sys.exit(1)
        print("Number of columns :" + str(len(df1.columns)))
        
        #try:
        #    file_encoding = open(file, encoding='UTF-8').read() 
        #except:
        #    file_encoding = open(file, encoding='other-single-byte-encoding').read() 

        
        #● Capture all the clean records to a .out file.
        #● Capture all the bad records in a .bad file.
               
        df1.drop(['online_order', 'book_table', 'approx_cost(for two people)', 'menu_item', 'listed_in(type)', 'listed_in(city)', 'Row_num_list'], axis=1, inplace=True)
        timestr = time.strftime("%Y_%m_%d-%I_%M_%S_%p")       
        outdir = './out_file'
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        
        fullname = os.path.join(outdir, timestr + '.out')    
        df1.to_csv(fullname)
        
        #● For the .bad file create a metadata file which will contain the following fields:
        #1. Type_of_issue - this is a short keyword for the type of non-conformity.
        #2. Row_num_list - list of all the row numbers which have the issue.
        #For e.g. if there are null records found in the dataset then type_of_issue field will have
        #the value “null” and row_num_list will contain the list of all the row numbers which have the issue.
        
        
        df_no_area.drop(['check_area'], axis=1, inplace=True)
        df_no_area.append(df_clean, ignore_index = True)
        df_no_area.append(df_no_area, ignore_index = True)
        df_no_area.drop(['online_order', 'book_table', 'approx_cost(for two people)', 'menu_item', 'listed_in(type)', 'listed_in(city)', 'Row_num_list'], axis=1, inplace=True)   
       
        outdir = './bad_file'
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        
        fullname = os.path.join(outdir, timestr + '.bad')    
        df_no_area.to_csv(fullname)
        
        file1 = open("processed_files.txt","a")
        file1.writelines(file + '\n')
        file1.close()
        
              
check()