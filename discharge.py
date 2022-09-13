import mysql.connector as sql
import pandas as pd
from datetime import datetime as date
import findspark
findspark.init()
from pyspark.sql import SparkSession
import sys
import random as rd


app = SparkSession.builder.master("local[1]").appName("app").getOrCreate()
mydb = sql.connect(host="localhost",user="root",passwd="Jntu@1423",database="mydb1",auth_plugin='mysql_native_password')
cur = mydb.cursor()


pid = sys.argv[1].strip() #53346892

def ex_q(q):
    cur.execute(q)
    x = cur.fetchall()
    if x == [(None,)]:
        return [["No Data"]]
    if x:
        x=[list(i) for i in x ]
        for i in range(len(x)):
            for j in range(len(x[i])):
                if x[i][j] is None:
                    x[i][j]="-"
        return x
    else:
        return [["No Data"]]
    
def cal_age(dob):
    x = date.now()
    try:
        return int(x.strftime("%Y")) - int(dob.strftime("%Y"))
    except:
        return "No data"

def drug(pid):
    data = ex_q("select drug_id,acquired_medicines from drug_utilization where patient_id ="+str(pid))
    if data == [["No Data"]]:
        return "No Data"
    d1={}
    for i in data:
        if i[0] in d1:
            d1[i[0]]+=i[1]
        else:
            d1[i[0]]=i[1]
    dat = ex_q("select drug_id,drug_name,drug_cost from drug_master where drug_id in ("+",".join([str(i) for i in d1])+")")
    return d1,dat

def surg(pid):
    data = ex_q("select surgery_id from surgery_table where patient_id="+str(pid))
    if data == [["No Data"]]:
        return "No Data"
    data = [i[0] for i in data]
    try:
        dat = ex_q("select * from surger_master where surgery_id in ("+",".join([str(i) for i in data if i != "No Data"])+")")
    except:
        dat=["No Data"]
    return data,dat

def dig(pid):
    data = ex_q("select diagnostic_id,diagnostics_test_charges from diagnostics_test_results where patient_id="+str(pid))
    if data == [["No Data"]]:
        return "No Data"
    d1,d2={},{}
    c_list = [i[0] for i in data]
    for i in data:
        if i[0] in d1:
            d1[i[0]]+=i[1]
        else:
            d1[i[0]]=i[1]
    dat = ex_q("select diagnostics_id,diagnostic_name from diagnostics_master where diagnostics_id in ("+",".join([str(i) for i in d1 if i != "No Data" or i != "-"])+")")   
    if dat == [["No Data"]]:
        return "No Data"
    for i in dat:
        d2[i[0]]=i[1]
        
    return c_list,d1,d2


def amb(pid):
    data = ex_q("select ambulance_utilization_charges from ambulance_utilization_details where patient_id ="+str(pid))
    if data == [["No Data"]]:
        return "No Data"
    return [i[0] for i in data]

def pro(pid):
    data = ex_q("select procedure_id from procedure_table where patient_id = "+str(pid))
    if data == [["No Data"]]:
        return "No Data"
    data = [i[0] for i in data]
    dat = ex_q("select * from procedure_master where procedure_id in ("+",".join([str(i) for i in data])+")")
    if dat == [["No Data"]]:
        return "No Data"
    return data,dat



a = str(ex_q("select case_id from case_id_patient where patient_id="+str(pid))[0][0]) + "-" + str(pid)
b = ex_q("select address_city,address_state,address_zipcode from address where patient_id="+str(pid))[0]
c = ex_q("select patient_gender,patient_dob,patient_occupation,patient_primary_email,patient_pan_number,bank_account_number,bank_name,branch_name,ifsc_code from patient_master where patient_id = "+str(pid))[0]
h_id = ex_q("select hospital_id from patient_visit_details where patient_id="+str(pid))[0][0]
e = ex_q("select hospital_name,hospital_address from hospital_master where hospital_id="+str(h_id))[0]
f = ex_q("select patient_admission_cause, admission_timestamp,discharge_timestamp,patient_admitted_dates_calculated,patient_injury_cause,patient_admission_medico_legal,patient_admission_police_reported,patient_admission_MLC_report,patient_admission_system_of_medicine from patient_visit_details where  patient_id="+str(pid))[0]
d_id = str(ex_q("select doctor_id from patient_vitals where patient_id = "+str(pid))[0][0])
g = ex_q("select doctor_first_name,doctor_middle_name,doctor_last_name,doctor_highest_degree,doctor_registration_number from doctor_master where doctor_id = "+d_id)[0]
n = ex_q("select patient_first_name,patient_middle_name,patient_last_name from patient_master where patient_id="+str(pid))[0]
add = ex_q("select address_building_no,address_street_1,address_street_2,address_area from address where patient_id="+str(pid))[0]

d={
    "Patient id":pid,
    "claim no": a,
    "policy_no": a,
    "si.no/cert no": a,
    "name": n[0]+" "+n[1]+" "+n[2],
    "address": add[0]+","+add[1]+","+add[2]+","+add[3],
    "city": b[0],
    "state":b[1],
    "gender":c[0],
    "Dob":c[1],
    "Age":cal_age(c[1]),
    "occupation":c[2],
    "pincode":b[2],
    "email_id":c[3],
    "hospital_id":h_id,
    "name_of_hospital":e[0],
    "hospital_address":e[1],
    "room_category":ex_q("select room_cateogory_id from hospital_room_cateogory where record_creation_time=(select record_creation_time from patient_visit_details where patient_id="+str(pid)+")")[0][0],      #-------> clarify
    "hospitailized_due_to":f[0],
    "Date_of_injury/detected/diagnosed":ex_q("select case_id_record_creation_time from case_id_patient where patient_id="+str(pid))[0][0],
    "date_of_admission":f[1],
    "date_of_discharge":f[2],
    "time":f[3],
    "injure_Cause":f[4],
    "Medico_legal":f[5],
    "Reported_to_the_police":f[6],
    "Mlc_reported_&_police_fir+attached":f[7],
    "System_of_medicine":f[8],
    "pan":c[4],
    "Ambulance Charges":ex_q("select ambulance_utilization_charges from ambulance_utilization_details where patient_id ="+str(pid))[0][0],
    "Surgical Cash":ex_q("select surgery_charges from surger_master where surgery_id in ( select surgery_id from surgery_table where patient_id ="+str(pid)+")")[0][0],
    "bank account number": c[5],
    "bank_name":c[6],
    "branch_name":c[7],
    "ifsc_code":c[8],
    "Doctor name":g[0]+" "+g[1]+" "+g[2],
    "Doctor Qualification":g[3],
    "Doctor registration":g[4],
    "Doctor mail id":ex_q("select email from email where patient_id ="+d_id)[0][0],
    "comorbidities":ex_q("select patient_admission_comorbidities from patient_admission_details where patient_id = "+str(pid))[0][0],
}

Total_sum = 0

bill_data = ex_q("select patient_bill_id,patient_bill_claim_date,patient_bill_issued_by,patient_bill_amount from patient_expenses where patient_id="+str(pid))
if bill_data != [["No Data"]]:
    d["No of Bills"]=len(bill_data)
    sum_bills =0
    for i in bill_data:
        if i != ["No Data"]:
            sum_bills+=i[-1]
            d["Bill no {}".format(bill_data.index(i)+1)] = i[0]
            d["Date {}".format(bill_data.index(i)+1)] = i[1]
            d["Issued By {}".format(bill_data.index(i)+1)] = i[2]
            d["Bill Towards {}".format(bill_data.index(i)+1)] = d["name"]
            d["Amount {}".format(bill_data.index(i)+1)] = i[-1]
    d["Total Bills Amount"]=sum_bills
    Total_sum+=sum_bills

    
data = drug(pid)  
if data != "No Data":
    d1,lis = data
    d["Number of Drugs Used"]=len(lis)
    s=0
    for i in lis:
        if i != "No Data":
            d["Drug{} Name".format(lis.index(i)+1)] = i[1]
            d["Drug{} Count".format(lis.index(i)+1)] = d1[i[0]]
            d["Drug{} Each Cost".format(lis.index(i)+1)] = float(i[2])
            d["Drug{} Total Cost".format(lis.index(i)+1)] = float(i[2])*d1[i[0]]
            s+=float(i[2])*d1[i[0]]
    d["Total Drugs Cost"]=s
    Total_sum+=s


data = dig(pid)
if data!="No Data":
    v1,v2,v3 = data
    c=0
    s=0
    d["Number of Diagnostics"]=len(v1)
    for i in set(v1):
        if i != "No Data":
            c+=1
            d["Diagnostic{} Name".format(c)] = v3[i]
            d["Diagnostic{} Count".format(c)] = v1.count(i)
            d["Diagnostic{} Each Cost".format(c)] = v2[i]
            d["Diagnostic{} Total Cost".format(c)] = v2[i]*v1.count(i)
            try:
                s+=v2[i]*v1.count(i)
            except:
                s+=0
    d["Total Diagnostic Cost"]=s
    Total_sum+=s


data = surg(pid)
if data!="No Data":
    v4,v5=data
    d["Number of Surgeries"]=len(v4)
    c=0
    s=0
    for i in set(v4):
        c+=1
        if i != "No Data":
            d["Surgery{} Name".format(c)] = [j[1] for j in v5 if j[0]==i][0]
            d["Surgery{} Count".format(c)] = v4.count(i)
            d["Surgery{} Each Cost".format(c)] = [j[2] for j in v5 if j[0]==i][0]
            d["Surgery{} Total Cost".format(c)] = [j[2] for j in v5 if j[0]==i][0]*v4.count(i)
            s+=[j[2] for j in v5 if j[0]==i][0]*v4.count(i)
    d["Total Surgery Cost"]=s
    Total_sum+=s

    
v6=amb(pid)
if v6 != "No Data":
    d["Number of Times Ambulance Used"]=len(v6)
    c=0
    for i in v6:
        if i != "No Data":
            c+=1
            d["Ambulance{} Charges".format(c)] = i
    d["Total Ambulance Charges"]=sum(v6)
    Total_sum+=sum(v6)
    

data = pro(pid)
if data!="No Data":
    v7,v8=data
    d["Total number of Procedures"]=len(v7)
    c,s=0,0
    for i in set(v7):
        if i != "No Data":
            c+=1
            d["Procedures{} Name".format(c)]=[j[1] for j in v8 if j[0]==i][0]
            d["Procedures{} count".format(c)]=v7.count(i)
            d["Procedures{} Each cost".format(c)]=[j[2] for j in v8 if j[0]==i][0]
            d["Procedures{} Total cost".format(c)]=[j[2] for j in v8 if j[0]==i][0]*v7.count(i)
            s+=[int(j[2]) for j in v8 if j[0]==i][0]*v7.count(i)
    d["Total Procedures Cost"]=s
    Total_sum+=s
    
d["Total Expenses"]=Total_sum 

dis = ['Canavan disease', 'Hemophilia', 'Fabry disease', 'Galactosemia']

d["disease_name"] = rd.choice(dis)

df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in d.items() ]))

df.to_csv("./panda_excel/{}.csv".format(pid))

# df.to_csv("panda_excel/{}.csv".format(pid))

s_df = app.createDataFrame(df)
s_df.write.csv( header="true", mode="overwrite",path="/usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/"+str(pid)+'.csv')

# /usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/

# /part-00011-cf315c36-fc57-4e5a-9e3b-1039b836b3f6-c000.csv

mydb.close()
