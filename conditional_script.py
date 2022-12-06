from string import ascii_letters
import pandas as pd
import random
import csv
import os

print('*********************** please close or rename the generated file ***********************')

def data_cond(file_name,batch_id ,pep_open,pep_close,sanc_open ,final_file_name):
    read_data = pd.read_csv(file_name)
    df = pd.Dataframe(read_data)
    df.drop_dupliucates(subset = ["PrimaryName"],keep = 'first', inplace = True)
    print("After remove duplicate records count-------"+ str(len(df)))


    sd = ['Second_Last_Name12']  # columns needs to add here

    df1 = pd.Dataframe(columns = sd)
## below code to split a single column data in to 2 columns
    df1[['Last_name' ,'Fristname']] = df['primaryName'].str.split(',',expand = True)

#writing columns with user data
    Type_new = pd.Series([])
    val = batch_id
    pep_close1 = pep_open+pep_close
    Sanc1 = pep_close1+Sanc_open

    gen1 = pd.Series([])
    for index, rows in df.iterrows():
        d = index
        if d < pep_open:
            Type_new[d]= "PEP OPEN"

        elif d < pep_close1:
            Type_new[d] = "PEP CLOSE"

        elif d < Sanc1:
            Type_new[d] = "Sanc OPEN"

        else:
            Type_new[d] ="Sanc CLOSE"
        val_oro = val +str(index)
        gen1[d] =val_oro


#extract the TAX no for the columns having a large data sets


    df1[['rawdata','TAX']] = df["AdditionalInfo"].str.split("TAX Identification" , expand = True)
    df1.insert(0,'no1',df1['TAX'].str.split('.'),expand = False)
    df.insert(1,'worldcheckid',df['WorldCheckId'])
    df1.to_csv('temp_file.csv')
    read_data1 = pd.read_csv('temp_file.csv')
    df12 = pd.Dataframe(read_data1)
    df12[['oro','dup']] = df12["no1"].str.split(",",1,expand = True)
    df12.insert(2,'Original_data' ,df12[oro])
    df12['Original_data'] =df12['Original_data'].str.replace('[^a-d0-9]' ,'')


#function to replace character for each rows at random place only when we get close text in other column

    def replace_character(col_name):
        for index , row in df1.iterrows():
            if row['Delta_type'] in ['PEP CLOSE' ,'Sanc CLOSE']:
                s= row[col_name]
                inds = [i for i ,_ in enumerate(str(s)) if not str(s).isspace()]
                sam = random.sample(inds ,1)
                lst = list(str(s))
                for ind in sam :
                    lst[ind] = random.choice(ascii_letters)
                er = "".join(lst)
                gen1[index] =er
            else:
                gen1[index] =row[col_name]
    return gen1

# Function to change the Data format and handle the null data

    def repalce_date(col_name):
        gen1 = pd.Series([])
        df1[col_name].fillna('12-09-1998',inplace =True)
        for index ,row in df1.iterrows():
            if row['Delta_type'] in ['PEP CLOSE','Sanc CLOSE']:
                s = row[col_name]
                indis = [i for i,_ in enumerate(str(s)) if not str(s).isspace()]
                sam = random.sample(inds ,1)
                lst = list(str(s))
                for ind in sd :
                    print("Found"+str(ind))
                    ind = 2
                    lst[ind] = str(random.choice((range(1,2))))
                else :
                    lst[ind] = str(random.choice((range(1,2))))
                er ="".join(lst)
                gen1[index] =er
            else:
                gen1[index] = row[col_name]
        return gen1
###### function to swap date #################
########df_ff is dataFrame
    def swap_data(col_data):
        gen01 = pd.series([])
        for index, row in df_ff.iterrows():
            s = row[col_data]
            if '-' in s:
                s_swap = s[6:] + "-" +s[3:5] + "-" +s[:2]
                gen01[index] = s_swap
            else:
                s_swap = s.replace("/","-")
                gen01[index] = s_swap
        return gen01


# read the configuration file and pass the data as parameter

def get_data(file_name):
    records = []
    with open(filename) as readfile:
        next(readfile)
        lines = readfile.readlines()
        for line in lines :
            str_rec = line.split("|")
            INPUTFILE = str_rec[0]
            BATCH_ID = str_rec[1]
            #.... so on to add parameter & pass the parameter to the function below

            data_cond(INPUTFILE,BATCH_ID)