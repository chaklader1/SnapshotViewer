

This application will create the latest options chain with their respective hashcode. 

Scripts
-------

The scripts need to be run in the _marketData_ directory. The way to run the scripts 


$ script_name ./location_of_marketData_directory

<br>

$ feedhandler_marketdata_analysis.sh ./

<br>

$ feedengine_marketdata_analysis.sh ./

Both of FE and FH, the marketData directory need to contain the data.csv file. But the content 
inside the file will be different. For the FH, the _data.csv_ content will be - 

    META,META230616P00065000,META/L1Q3.O
    META,META230616P00145000,META/L1Q3B.O
    META,META230616P00305000,META/L1Q3D.O
    META,META230616P00385000,META/L1Q3E.O
    META,META230616P00070000,META/L1Q7.O
    META,META230616P00150000,META/L1Q7B.O

For the FE, the content of the _data.csv_ file needs to be - 

    
    META230616P00065000
    META230616P00145000
    META230616P00305000
    META230616P00385000
    META230616P00070000
    META230616P00150000
    META230616P00230000
    META230616P00310000
    META230616P00390000
    META230616P00470000

