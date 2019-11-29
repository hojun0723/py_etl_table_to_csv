import pyodbc
import datetime
import csv
import sys
import zipfile
import os

rpath = './ISP_Custom_ETL_TABLE_to_CSV.ini'

wpath = 'D:/ISP_Custom_File/TABLEtoCSV/'

#파일을 행 단위로 읽어서 배열에 넣는다
lines = open(rpath).read().splitlines()

#sys.exit()

cnxn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};SERVER=123.123.123.123;DATABASE=database;UID=userid;PWD=password')
cursor = cnxn.cursor()

for tblname in lines:

    sql = """\
    SELECT * FROM
    """
    sql = sql + " " + tblname

    rows = cursor.execute(sql)

    today = datetime.datetime.now()
    setfilename = today.strftime('%Y%m%d')
    filename = tblname + '_' + setfilename

    print('creating...... ' + wpath + filename + '.csv')

    with open(wpath + filename + '.csv', 'w', encoding='UTF-8', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow([x[0] for x in cursor.description])  # column headers

        for row in rows:

            writer.writerow(row)

    print('compression... ' + wpath + filename + '.zip')

    tbl_zip = zipfile.ZipFile(wpath + filename + '.zip', 'w')
    tbl_zip.write(wpath + filename + '.csv', compress_type=zipfile.ZIP_DEFLATED)
 
    tbl_zip.close()
    os.remove(wpath + filename + '.csv')
    print('deleting...... ' + wpath + filename + '.csv')

    #한줄 띄어쓰기
    print('')

print('complete.')
