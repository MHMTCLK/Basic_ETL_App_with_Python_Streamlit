##libraries
import streamlit as st
import pandas as pd
import pyodbc as pydb
import datetime
import time
import base64

##file upload configuration
st.set_option('deprecation.showfileUploaderEncoding', False)

def main():

    ##project title
    st.title("BASIC ETL APPLICATION")

    ##queries
    checkTable = '''
    IF NOT EXISTS 
    (
        SELECT	* 
        FROM	sys.objects 
        WHERE	object_id = OBJECT_ID(N'[dbo].[IRISH_DATA]')
        AND		type in (N'U')
    )
    BEGIN 
        CREATE TABLE [dbo].[IRISH_DATA] 
        (
            sepal_length_cm int NULL, 
            sepal_width_cm int NULL, 
            petal_length_cm int NULL,
            petal_width_cm int NULL,
            class nvarchar(50) NULL
        )
    END
    '''
    insertTable = "INSERT INTO DataAnalysisTeam.dbo.IRISH_DATA (sepal_length_cm, sepal_width_cm, petal_length_cm, petal_width_cm, class) VALUES (?,?,?,?,?)"
    selectTable = "SELECT * FROM DataAnalysisTeam.dbo.IRISH_DATA"

    checkLog = '''
    IF NOT EXISTS 
    (
        SELECT	* 
        FROM	sys.objects 
        WHERE	object_id = OBJECT_ID(N'[dbo].[IRISH_LOG]')
        AND		type in (N'U')
    )
    BEGIN 
        CREATE TABLE [dbo].[IRISH_LOG] 
        (
            FLAG nvarchar(25) NULL,
            DATE datetime NULL, 
            MESSAGE nvarchar(500) NULL
        )
    END
    '''
    insertLog = "INSERT INTO DataAnalysisTeam.dbo.IRISH_LOG (FLAG, DATE, MESSAGE) VALUES (?,?,?)"
    
    ##database connetion 
    connection = pd.read_csv(r"connectionDB.csv", sep=";", header=None)
    driver = connection[0][0]
    server = connection[1][0]
    database = connection[2][0]
    userId = connection[3][0]
    password = connection[4][0]
    connectionString = 'Driver='+driver+';Server='+server+';Database='+database+';uid='+userId+';pwd='+password+';'

    ##check data_table and log_table
    conn = pydb.connect(connectionString)
    cursor = conn.cursor()
    cursor.execute(checkTable)
    cursor.execute(checkLog)
    conn.commit()
    cursor.close()
    conn.close()

    ##app type
    typeApp = ['SEND DATA TO DB', 'GET DATA FROM DB']

    ##select type
    selectedType = st.sidebar.selectbox("App Type", typeApp)
    if selectedType == "GET DATA FROM DB":
        st.subheader("GET")

        ##get data from database
        conn = pydb.connect(connectionString)
        data = pd.read_sql(selectTable, conn)
        conn.close()

        ##logging
        conn = pydb.connect(connectionString)
        cursor = conn.cursor()
        cursor.execute(insertLog, 'get dataset', datetime.datetime.now(), "the get operation")
        conn.commit()
        cursor.close()

        ##selected number of record
        rowNumber = st.selectbox('Row Number', ['Show 10 Rows', 'Show Whole Dataset'])
        if rowNumber == 'Show 10 Rows':
            c = 10
        elif rowNumber == 'Show Whole Dataset':
            c = len(data)
        else:
            c = 0

        ##display format
        displayFormat = st.radio("Display Format", ('Table', 'DataFrame', 'HTML', 'Download'))
        if displayFormat == 'Table':
            st.table(data.head(n=c))
        elif displayFormat == 'DataFrame':
            st.dataframe(data.head(n=c))
        elif displayFormat == 'HTML':
            st.markdown(data.head(n=c), unsafe_allow_html=True)
        elif displayFormat == 'Download':
            csv = (data.head(n=c)).to_csv(index=False)
            b64 = base64.b64encode(csv.encode()).decode()
            href = f'<a href="data:file/csv;base64,{b64}">Download CSV File</a> (right-click and save as &lt;some_name&gt;.csv)'
            st.markdown(href, unsafe_allow_html=True)
        else:
            ''



    elif selectedType == "SEND DATA TO DB":
        st.subheader("SEND")
        
        ##read data, check table and insert database
        data = st.file_uploader("Upload Dataset", type="csv")
        if data is not None:
            dataset = pd.read_csv(data)
            if st.button("send to database"):
                file_bar = st.progress(0)
                for percent_complete_file in range(100):
                    time.sleep(0.01)
                    file_bar.progress(percent_complete_file + 1)
                st.write("%100 The file has been read successfully!!!")
                st.write(" ", dataset.shape)
                st.write(" ", dataset.head())
            
                conn = pydb.connect(connectionString)
                cursor = conn.cursor()
                db_bar = st.progress(0)
                for index, row in dataset.iterrows():
                    cursor.execute(insertTable, row['sepal_length_cm'], row['sepal_width_cm'], row['petal_length_cm'], row['petal_width_cm'], row['class']) 
                    conn.commit()
                    db_bar.progress((index+1) / len(dataset))
                cursor.close()
                conn.close()
                st.write("%100 The file has been sent to the database successfully!!!")

                ##logging
                conn = pydb.connect(connectionString)
                cursor = conn.cursor()
                cursor.execute(insertLog, 'send dataset', datetime.datetime.now(), "the send operation")
                conn.commit()
                cursor.close()

    else:
        ''



if __name__ == '__main__':
    main()
