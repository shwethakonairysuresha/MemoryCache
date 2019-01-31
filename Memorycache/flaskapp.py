import os, re, time, memcache
from flask import Flask, render_template, request, redirect, session
from random import randint
from datetime import datetime
import sys, csv
import pymysql
from werkzeug.utils import secure_filename

ACCESS_KEY_ID = '********'
ACCESS_SECRET_KEY = '********'
BUCKET_NAME = '********'

hostname = '********'
username = '********'
password = '********'
database = '********'
Conn = pymysql.connect( host=hostname, user=username, passwd=password, db=database, autocommit = True, cursorclass=pymysql.cursors.DictCursor, local_infile=True)

print "Database Connected"

application = Flask(__name__)
app = application
app.secret_key = '********'

def memcache_connect():
    #Connecting to the memcache
	memc = memcache.Client(['shwethacache.5qit6s.cfg.use2.cache.amazonaws.com:11211'], debug = 1)
	print "Memcache connected"
	return memc

UPLOAD_FOLDER = '/home/ubuntu/flaskapp/'
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
query= "select * from Education limit"

@app.route("/")
def hello():#For displaying the first page
    return render_template("filehandle.html")

@app.route("/csv_file_upload", methods = ['POST'])
def csv_file_upload():#For uploading the file
	file = request.files['file_upload']	
	filename=file.filename
	session['file_name']=filename
	print "file recieved"
	filename = secure_filename(file.filename)
	file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
	dropquery="drop table IF EXISTS "+ filename[:-4]
	with Conn.cursor() as curs:
		curs.execute(dropquery)
		Conn.commit()
	print "dropped"
	columnname="("
	abs_filename=UPLOAD_FOLDER+filename
	with open(abs_filename, 'r') as f:
		reader = csv.reader(f)
		for row in reader:
			line=row
			break
	for i in line:
		columnname+=i+" VARCHAR(50),"
	query="Create table if not exists " + filename[:-4]+columnname+" sr_no INT NOT NULL AUTO_INCREMENT, PRIMARY KEY(sr_no));"
	print query
	with Conn.cursor() as curs:
		curs.execute(query)
		Conn.commit()
	curs.close()
	print "successfully created"
	
	insert_data="""LOAD DATA LOCAL INFILE '"""+absfilename+ """'INTO TABLE """+ filename[:-4] +""" FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' LINES TERMINATED BY '\n' IGNORE 1 ROWS;"""
	print (insert_data)
	with Conn.cursor() as curs:
		curs.execute(insert_data)
		Conn.commit()
	print "successfully loaded"
	return render_template("filehandle.html")

@app.route('/executesql', methods=['POST'])
def executesql():
    	limits = request.form['limits']
    	start_time = time.time()
    	print(start_time)
    	with Conn.cursor() as curs:
        	curs.execute(query + limit)
	        Conn.commit()
    	curs.close()        
    	end_time = time.time()
    	print('end_time')
    	total_sqltime = end_time - start_time
    	print(total_sqltime)
    	return render_template('filehandle.html', time1=total_sqltime)

@app.route('/latlong',methods=['POST'])
def latlong():
	value=request.form['limit']
	start_time = time.time()
	with Conn.cursor() as curs:
    		save="savepoint s1"
	    	curs.execute(save)
	    	safe_update="SET SQL_SAFE_UPDATES = 0"
	    	curs.execute(safe_update)
	    	clean_query="update Education set STATE='AK' where INSTNM ='"+value+"';"
	    	curs.execute(clean_query)
	    	s_query="select * from Education where INSTNM='"+value+"';"
	    	curs.execute(s_query)
	    	result = curs.fetchall()
    		Conn.commit()
		curs.close()
		end_time = time.time()
    	print('endtime')
    	total_sqltime = end_time - start_time
    	print(total_sqltime)
    	return render_template('filehandle.html',Data1=result,time=total_sqltime)

@app.route('/executesqlrestrict', methods=['POST'])
def executesqlrestrict():
    	limit = request.form['limit']
	print limit
	loc_query="Select * from Education where STATE='"+limit+"';"
    	start_time = time.time()
    	print(start_time)
    	with Conn.cursor() as curs:
        	curs.execute(loc_query)
	        Conn.commit()
    	curs.close()        
    	end_time = time.time()
    	print('endtime')
    	total_sqltime = end_time - start_time
    	print(total_sqltime)
    	return render_template('filehandle.html', time2=total_sqltime)

@app.route('/memexecute', methods=['POST'])
def imp_memcache():#For implementing memcache
	memc = memcache_connect()
	#memc.flush_all()
	limit = request.form['limit']
	loc_query="select * from Education limit"+limit+";"
	print (loc_query)
	new_key = locquery.replace(' ','')
    	value = mc.get(new_key)
	print value
	start_time = time.time()
    	print(start_time)
    	if value is None:
		print "entered memcache"
		with Conn.cursor() as curs:
        		curs.execute(loc_query)
		        rows=curs.fetchall()
			result=" "
			for i in rows:
				result+=str(i)
			curs.close()
			print "fetched"
			print (result)
			status = memc.set(new_key,result) 
			print (status)
			print (memc.get(new_key))			
	else:
		print (memc.get(new_key))        
    	end_time = time.time()
    	print('end_time')
    	total_sqltime = end_time - start_time
    	print(total_sqltime)
    	return render_template('filehandle.html', time2=total_sqltime)
		
@app.route('/query1', methods=['POST'])
def query1():
     State = request.form['State']
     q1="Select * from Education where State='"+State+"';"
     qq1="Select count(*) from Education where State='"+State+"';"
     print (q1)
     print (qq1)
     with Conn.cursor() as curs:
         curs.execute(q1)
         rows=curs.fetchall()
         curs.execute(qq1)
         res=curs.fetchall()
         curs.close()
         return render_template('filehandle.html',answer1=rows,answer2=res[0]['count(*)'])
		
		
@app.route('/query2', methods=['POST'])
def selectQuery():
    State = request.form['State']
    qu1="SELECT * from USZipcodes where STATE like \'%'"+State+"';"
    qqu1="SELECT count(*) from USZipcodes where STATE like \'%'"+State+"';"
    print(qu1)
    print(qqu1)
    with Conn.cursor() as curs:
        curs.execute(qu1)
        rows=curs.fetchall()
        curs.execute(qqu1)
        res=curs.fetchall()
        curs.close()
        return render_template('filehandle.html',answer3=rows,answer4=res[0]['count(*)'])
		
@app.route('/query3', methods=['POST'])
def query3():
    a = request.form['val1']
    b = request.form['val2']
    c = float(x)
    d = float(y)
    temp_1 = w + 1
    temp_2 = z + 1
    long = str(temp_1)
    lat  = str(temp_2)
    loc_query = "Select distinct(Starbucks.Id) from Starbucks where Longitude between " + a + " and " +long+ " and Latitude between " + b + " and " +lat+ ";"
    print (loc_query)
    start_time = time.time()
    with Conn.cursor() as cursor:
        cursor.execute(loc_query)
        result = cursor.fetchall()
        for el in result:
            print(el['Id'])
        Conn.commit()
        cursor.close()
        end_time = time.time()
        total_sqltime = endt_ime - start_time
        return render_template('filehandle.html', res=result)
	
    