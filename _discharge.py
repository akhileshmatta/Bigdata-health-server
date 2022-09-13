import mysql.connector as sql
import pandas as pd
from datetime import datetime as date
import findspark
findspark.init()
from pyspark.sql import SparkSession
import sys


app = SparkSession.builder.master("local[1]").appName("app").getOrCreate()
mydb = sql.connect(host="localhost",user="root",passwd="Jntu@1423",database="mydb1",auth_plugin='mysql_native_password')
cur = mydb.cursor()


pid = sys.argv[1] #53346892


def ex_q(q):
    cur.execute(q)
    x = cur.fetchall()
    if x == [(None,)]:
        return [["No Data","No Data","No Data","No Data","No Data","No Data"]]
    if x:
        return x
    else:
        return [["No Data","No Data","No Data","No Data","No Data","No Data","No Data","No Data"]]


def cal_age(dob):
    x = date.now()
    return int(x.strftime("%Y")) - int(dob.strftime("%Y"))


def drug(pid):
    data = ex_q("select drug_id,acquired_medicines from drug_utilization where patient_id ="+str(pid))
    d1={}
    for i in data:
        if i[0] in d1:
            d1[i[0]]+=i[1]
        else:
            d1[i[0]]=i[1]
    dat = []
    if d1.get('No Data', -1) == -1:
        dat = ex_q("select drug_id,drug_name,drug_cost from drug_master where drug_id in ("+",".join([str(i) for i in d1])+")")
    return d1,dat


def surg(pid):
    data = ex_q("select surgery_id from surgery_table where patient_id="+str(pid))
    data = [i[0] for i in data]
    dat = ex_q("select * from surger_master where surgery_id in ("+",".join([str(i) for i in data])+")")
    return data,dat


def dig(pid):
    data = ex_q("select diagnostic_id,diagnostics_test_charges from diagnostics_test_results where patient_id="+str(pid))
    d1,d2={},{}
    c_list = [i[0] for i in data]
    for i in data:
        if i[0] in d1:
            d1[i[0]]+=i[1]
        else:
            d1[i[0]]=i[1]
    dat = []
    if d1.get('No Data', -1) == -1:
        dat = ex_q("select diagnostics_id,diagnostic_name from diagnostics_master where diagnostics_id in ("+",".join([str(i) for i in d1])+")")
    for i in dat:
        if i[0] != 'No Data':
            d2[i[0]]=i[1]        
    return c_list,d1,d2


def amb(pid):
    data = ex_q("select ambulance_utilization_charges from ambulance_utilization_details where patient_id ="+str(pid))
    print('amb details is',pid)
    return [i[0] for i in data]


def pro(pid):
    data = ex_q("select procedure_id from procedure_table where patient_id = "+str(pid))
    data = [i[0] for i in data]
    dat = []
    if d1.get('No Data', -1) == -1:
        dat = ex_q("select * from procedure_master where procedure_id in ("+",".join([str(i) for i in data])+")")
    return data,dat


d = {
    "Patient id":pid,
    "claim no": "-".join([str(i) for i in list(ex_q("select case_id,patient_id from case_id_patient where patient_id="+str(pid))[0])]),
    "policy_no": "-".join([str(i) for i in list(ex_q("select case_id,patient_id from case_id_patient where patient_id="+str(pid))[0])]),
    "si.no/cert no": "-".join([str(i) for i in list(ex_q("select case_id,patient_id from case_id_patient where patient_id="+str(pid))[0])]),
    "name": " ".join(list(ex_q("select patient_first_name,patient_middle_name,patient_last_name from patient_master where patient_id="+str(pid))[0])),
    "address": ",".join(list(ex_q("select address_building_no,address_street_1,address_street_2,address_area from address where patient_id="+str(pid))[0])),
    "city": ex_q("select address_city from address where patient_id="+str(pid))[0][0],
    "state":ex_q("select address_state from address where patient_id="+str(pid))[0][0],
    "gender":ex_q("select patient_gender from patient_master where patient_id="+str(pid))[0][0],
    "Dob":ex_q("select patient_dob from patient_master where patient_id="+str(pid))[0][0],
    "Age":cal_age(ex_q("select patient_dob from patient_master where patient_id="+str(pid))[0][0]),
    "occupation":ex_q("select patient_occupation from patient_master where patient_id="+str(pid))[0][0],
    "address_2": " ".join(list(ex_q("select address_building_no,address_street_1,address_street_2,address_area from address where patient_id=" + str(pid))[0])),
    "pincode":ex_q("select address_zipcode from address where patient_id="+str(pid))[0][0],
    "email_id":ex_q("select patient_primary_email from patient_master where patient_id="+str(pid))[0][0],
    "hospital_id":ex_q("select hospital_id from patient_visit_details where patient_id="+str(pid))[0][0],
    "name_of_hospital":ex_q("select hospital_name from hospital_master where hospital_id=(select hospital_id from patient_visit_details where patient_id="+str(pid)+")")[0][0],
    "hospital_address":ex_q("select hospital_address from hospital_master where hospital_id=(select hospital_id from patient_visit_details where patient_id="+str(pid)+")")[0][0],
    "room_category":ex_q("select room_cateogory_id from hospital_room_cateogory where record_creation_time=(select record_creation_time from patient_visit_details where patient_id="+str(pid)+")")[0][0],      #-------> clarify
    "hospitailized_due_to":ex_q("select patient_admission_cause from patient_visit_details where  patient_id="+str(pid))[0][0],
    "Date_of_injury/detected/diagnosed":ex_q("select case_id_record_creation_time from case_id_patient where patient_id="+str(pid))[0][0],
    "date_of_admission":ex_q("select admission_timestamp from patient_visit_details where patient_id="+str(pid))[0][0],
    "date_of_discharge":ex_q("select discharge_timestamp from patient_visit_details where patient_id="+str(pid))[0][0],
    "time":ex_q("select patient_admitted_dates_calculated from patient_visit_details where patient_id="+str(pid))[0][0],
    "injure_Cause":ex_q("select patient_injury_cause from patient_visit_details where patient_id="+str(pid))[0][0],
    "Medico_legal":ex_q("select patient_admission_medico_legal from patient_visit_details where patient_id="+str(pid))[0][0],
    "Reported_to_the_police":ex_q("select patient_admission_police_reported from patient_visit_details where patient_id="+str(pid))[0][0],
    "Mlc_reported_&_police_fir+attached":ex_q("select patient_admission_MLC_report from patient_visit_details where patient_id="+str(pid))[0][0],
    "System_of_medicine":ex_q(" select patient_admission_system_of_medicine from patient_visit_details where patient_id= "+str(pid))[0][0],
    "pan":ex_q("select patient_pan_number from patient_master where patient_id="+str(pid))[0][0],
    "Surgical Cash":ex_q("select surgery_charges from surger_master where surgery_id in ( select surgery_id from surgery_table where patient_id ="+str(pid)+")")[0][0],
    "bank account number": ex_q("select bank_account_number from patient_master where patient_id = "+str(pid))[0][0],
    "bank_name":ex_q("select bank_name from patient_master where patient_id = "+str(pid))[0][0],
    "branch_name":ex_q("select branch_name from patient_master where patient_id = "+str(pid))[0][0],
    "ifsc_code":ex_q("select ifsc_code from patient_master where patient_id = "+str(pid))[0][0],
    "Doctor name":" ".join([i if i else '-' for i in list(ex_q("select doctor_first_name,doctor_middle_name,doctor_last_name from doctor_master where doctor_id in ( select doctor_id from patient_vitals where patient_id = "+str(pid)+")")[0])]),
    "Doctor Qualification":ex_q("select doctor_highest_degree from doctor_master where doctor_id in ( select doctor_id from patient_vitals where patient_id = "+str(pid)+")")[0],
    "Doctor registration":ex_q("select doctor_registration_number from doctor_master where doctor_id in ( select doctor_id from patient_vitals where patient_id = "+str(pid)+")")[0],
    "Doctor mail id":ex_q("select email from email where patient_id in ( select doctor_id from patient_vitals where patient_id ="+str(pid)+")")[0][0],
    "comorbidities":ex_q("select patient_admission_comorbidities from patient_admission_details where patient_id = "+str(pid))[0][0],
    
}

Total_sum = 0

bill_data = ex_q("select patient_bill_id,patient_bill_claim_date,patient_bill_issued_by,patient_bill_amount from patient_expenses where patient_id="+str(pid))
d["No of Bills"]=len(bill_data)
sum_bills =0
for i in bill_data:
    print(i)
    sum_bills+=int(0 if i[-1] == 'No Data' else i[-1])
    d["Bill no {}".format(bill_data.index(i)+1)] = i[0]
    d["Date {}".format(bill_data.index(i)+1)] = i[1]
    d["Issued By {}".format(bill_data.index(i)+1)] = i[2]
    d["Bill Towards {}".format(bill_data.index(i)+1)] = d["name"]
    d["Amount {}".format(bill_data.index(i)+1)] = i[-1]
d["Total Bills Amount"]=sum_bills
Total_sum+=sum_bills

d1,lis = drug(pid)
d["Number of Drugs Used"]=len(lis)
s=0
for i in lis:
    d["Drug{} Name".format(lis.index(i)+1)] = i[1]
    d["Drug{} Count".format(lis.index(i)+1)] = d1[i[0]]
    d["Drug{} Each Cost".format(lis.index(i)+1)] = float(i[2])
    d["Drug{} Total Cost".format(lis.index(i)+1)] = float(i[2])*d1[i[0]]
    s+=float(i[2])*d1[i[0]]
d["Total Drugs Cost"]=s
Total_sum+=s

v1,v2,v3 = dig(pid)
c=0
s=0
d["Number of Diagnostics"]=len(v1)
for i in set(v1):
    c+=1
    d["Diagnostic{} Name".format(c)] = v3[i]
    d["Diagnostic{} Count".format(c)] = v1.count(i)
    d["Diagnostic{} Each Cost".format(c)] = v2[i]
    d["Diagnostic{} Total Cost".format(c)] = v2[i]*v1.count(i)
    s+=v2[i]*v1.count(i)
d["Total Diagnostic Cost"]=s
Total_sum+=s

v4,v5=surg(pid)
d["Number of Surgeries"]=len(v4)
c=0
s=0
for i in set(v4):
    c+=1
    d["Surgery{} Name".format(c)] = [j[1] for j in v5 if j[0]==i][0]
    d["Surgery{} Count".format(c)] = v4.count(i)
    d["Surgery{} Each Cost".format(c)] = [j[2] for j in v5 if j[0]==i][0]
    d["Surgery{} Total Cost".format(c)] = [j[2] for j in v5 if j[0]==i][0]*v4.count(i)
    s+=[j[2] for j in v5 if j[0]==i][0]*v4.count(i)
d["Total Surgery Cost"]=s
Total_sum+=s

v6=amb(pid)
d["Number of Times Ambulance Used"]=len(v6)
c=0
for i in v6:
    c+=1
    d["Ambulance{} Charges".format(c)] = i
d["Total Ambulance Charges"]=sum(v6)
Total_sum+=sum(v6)

v7,v8=pro(pid)
d["Total number of Procedures"]=len(v7)
c,s=0,0
for i in set(v7):
    c+=1
    d["Procedures{} Name".format(c)]=[j[1] for j in v8 if j[0]==i][0]
    d["Procedures{} count".format(c)]=v7.count(i)
    d["Procedures{} Each cost".format(c)]=[j[2] for j in v8 if j[0]==i][0]
    d["Procedures{} Total cost".format(c)]=[j[2] for j in v8 if j[0]==i][0]*v7.count(i)
    s+=[int(j[2]) for j in v8 if j[0]==i][0]*v7.count(i)
d["Total Procedures Cost"]=s
Total_sum+=s
d["Total Expenses"]=Total_sum 

print("Data in dictionary is ")
print(d)


df = pd.DataFrame(dict([ (k,pd.Series(v)) for k,v in d.items() ]))

df.to_csv("./panda_excel/{}.csv".format(pid))

s_df = app.createDataFrame(df)
s_df.write.csv( header="true", mode="overwrite",path="/usr/local/hdsetup/hadoop_store/hdfs/datanode/current/project/"+str(pid)+'.csv')

mydb.close()
