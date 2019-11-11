#!/usr/bin/env python
# coding: utf-8

# # Rolling Aggregation # #

print("Program: Rolling Aggregation")
print("Release: 1.0.0")
print("Date: 2019-11-04")
print("Author: David Liau")
print()
print()
print("This program performs a rolling aggregation on a given dataset.")
print()


# Necessary Imports
import pandas as pd
from tkinter import Tk
from tkinter.filedialog import asksaveasfilename, askopenfilename


# In[ ]:


def select_file_in(title):
    file_in = askopenfilename(initialdir="../", title=title,
                              filetypes=(("Comma Separated Values", "*.csv"), ("all files", "*.*")))
    if not file_in:
        input("Program Terminated. Press Enter to continue...")
        exit()

    return file_in

def select_file_out(file_in):
    file_out = asksaveasfilename(initialdir=file_in, title="Select file",
                                 filetypes=(("Comma Separated Values", "*.csv"), ("all files", "*.*")))
    if not file_out:
        input("Program Terminated. Press Enter to continue...")
        exit()

    slash = file_out.rfind('/')
    file_name = file_out[slash:]
    ext = file_name.rfind('.')
    if ext == -1:
        file_out += '.csv'
    # Create an empty output file
    open(file_out, 'a').close()

    return file_out

def column_selection(headers):
    while True:
        try:
            print("Select column.")
            for j, i in enumerate(headers):
                print(str(j) + ": to perform rolling aggregation on column [" + str(i) + "]")
            column = headers[int(input("Enter Selection: "))]
        except:
                print("Input must be integer between 0 and " + str(len(headers)))
                continue
        else:
            print("")
            break
    return column

def agg_selection():
    operations = ['sum', 'prod', 'mean', 'std', 'var', 'min', 'max', 'argmin', 'argmax', 'median',                   'percentile', 'any', 'all', 'count']
    while True:
        try:
            print("Select aggregation operations to be performed:")
            for j, i in enumerate(operations):
                print(str(j) + ": to run aggregation [" + str(i) + "]")
            
            print(str(len(operations)) + ": to run all aggregations")
            
            # Collect aggregation list
            operation_indexes = input("Enter selections separated by spaces: ")
            
            if not operation_indexes:
                print("No input selected.")
                continue
            
            operation_indexes = operation_indexes.split(' ')
            
            operation_list = []
            if str(len(operations)) in operation_indexes:
                operation_list = operations
                print("Selecting all aggregation operations.\n")
                break
            
            for o in operation_indexes:
                operation_list.append(operations[int(o)])
            
        except:
            print("An invalid operation input was detected, please try again.")
            continue
            
        else:
            print("")
            break
    
    return operation_list

def select_index(df):
    headers = list(df.columns.values)
    while True:
        try:
            print("Select column to index your dataset by.")
            for j, i in enumerate(headers):
                print(str(j) + ": to select column [" + str(i) + "] as index")
            print("Press Enter if your dataset is already indexed.")
            
            # If the dataset has already been sorted
            index = input("Enter Selection: ")
            if not index:
                print("Dataset is already indexed.\n")
                return df
            
            # Otherwise, select index column
            column = headers[int(index)]
        
        # Error handling for values out of range
        except:
                print("Input must be integer between 0 and " + str(len(headers)))
                continue
                
        # Successful selecting of a column
        else:
            break
    
    print("Re-indexing and sorting by", column, "...")
    # Setting column as index. Then, sort the dataframe in order for easier aggregation
    try: 
        df[column] = pd.to_datetime(df[column])
    except:
        print("Indexed column is not datetime.")
        
    df = df[df[column].notnull()].sort_values(by=column)
    df = df.set_index(column)
    print("Finished indexing!\n")
    
    return df


# In[ ]:


# Hide Tkinter GUI
Tk().withdraw()

print("Please select the file to run on.")

# Find input file
file_in = select_file_in("Select file input")

# Select output file
file_out = select_file_out(file_in)

main = pd.read_csv(file_in)

# Selecting index to sort values by
main = select_index(main)

# Extracting column list
headers = list(main.columns.values)
# Selecting column to aggregate on
column = column_selection(headers)

agg_period = input("Enter aggregation period: ")

if not agg_period.isdigit():
    agg_period = str(agg_period)
else:
    agg_period = int(agg_period)

# Select which aggregation operations to perform
aggs = agg_selection()

# Aggregation period
print("Beginning rolling aggregation for:", column) 
limit = 0
while limit < 3:
    try:
        # Perform aggregation over time period
        stats = main.rolling(agg_period).agg({column: aggs})
        
        # Append aggregated statistics back to the main DataFrame
        for a in aggs:
            agg_column_name = column + "_" + a
            main[agg_column_name] = stats[column][a]
        main.reset_index(inplace=True)

        main.to_csv(file_out, index=False)
        print("Finished aggregation!")
        break

    except:
        limit += 1
        print("There was an issue with aggregation. Trying again.")
        if limit == 3:
            print("\nIt seems like there have been multiple failures in aggregation.")
            print("This may be because you have selected the incorrect column, picked an incompatible aggregation",                   "for that column, or picked an incorrect aggregation period. Please end the program and retry.")
            exit()




