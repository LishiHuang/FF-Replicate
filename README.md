# FF-Replicate
This is a program to replicate the research of Fama-French Factor Model

## main.ipynb is used to control the program
-Modify these codes in first cell to download data including certain index to the appointed path in setted time range
```
original_index='epspi'
document_path='C:\\Users\\Julia\\Desktop\\Asset pricing data\\test\\'
start_date='01/01/1959'
end_date='8/31/2024'
```
-Modify this line in second cell to sort the portfolios
```
pindex='N'
```
-If you want to design index not in the list, design it through --
1. Reset index name
```
pindex='ep'
```
2. How to calcualte the index
```
ccm2_jun[pindex] = ccm2_jun['epspi'] / ccm2_jun['dec_prc']
    if pindex == 'ep':
        ccm2_jun = ccm2_jun[ccm2_jun[pindex] >= 0]
```


## Update Calendar
-**11/04/2024**  
Move to a new address

-**10/22/2024**  
Updated the newly-designed program frame

-**07/15/2024**  
Create 10 portfolios separately by Assets, Inventory, ppe growth

-**06/07/2024**  
Create 10 momentum portfolio

-**05/10/2024**
Create 10 size portfolio
