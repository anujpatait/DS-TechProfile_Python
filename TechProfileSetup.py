import cx_Oracle
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

def main():
    conn=cx_Oracle.connect("DISPAPP","u39qLk8z93wbj6","mnetdb.uswc.uswest.com:1521/mnet.world")
    start_time = datetime.now()
    print(start_time)
    latest_emp_dump = pd.read_sql("SELECT * FROM EMAILDIR.DISPAPP_IDM", conn)
    query_end_time = datetime.now()
    print(query_end_time)
    print(query_end_time - start_time)
    print(" Total Count: " + str(len(latest_emp_dump.index)))
    svp_tech = latest_emp_dump[latest_emp_dump["LOGIN"]=="AC59768"]
    evp_tech = latest_emp_dump[latest_emp_dump["LOGIN"]== svp_tech['MGR_LOGIN'].iloc[0]]
    evp2_tech = latest_emp_dump[latest_emp_dump["LOGIN"]== evp_tech['MGR_LOGIN'].iloc[0]]
    
    if evp_tech["MIDDLE"].iloc[0] is None:
        svp_tech['MGR_NAME'] = evp_tech["LAST_NAME"].iloc[0]+", "+evp_tech["FIRST_NAME"].iloc[0]
    else:
        svp_tech['MGR_NAME'] = evp_tech["LAST_NAME"].iloc[0]+", "+evp_tech["FIRST_NAME"].iloc[0]+" "+evp_tech["MIDDLE"].iloc[0]

    if evp2_tech["MIDDLE"].iloc[0] is None:
        svp_tech['MGR2_NAME'] = evp2_tech["LAST_NAME"].iloc[0]+", "+evp2_tech["FIRST_NAME"].iloc[0]
    else:
        svp_tech['MGR2_NAME'] = evp2_tech["LAST_NAME"].iloc[0]+", "+evp2_tech["FIRST_NAME"].iloc[0]+" "+evp2_tech["MIDDLE"].iloc[0]

    svp_tech['MGR2_LOGIN'] = evp2_tech["LOGIN"].iloc[0]

    emp_hierarchy = pd.DataFrame(svp_tech)
    emp_hierarchy['HIERARCHY_LEVEL'] = 0
     #Add manager name and manager's manager name
    emp_hierarchy['PROCESSED'] = False

    while(len(emp_hierarchy.loc[emp_hierarchy['PROCESSED'] == False]) > 0):
        unprocess_emp = emp_hierarchy.loc[emp_hierarchy['PROCESSED'] == False]
        emp_hierarchy = set_hierarchy(emp_hierarchy,unprocess_emp,latest_emp_dump)
    print(len(emp_hierarchy))
    print(emp_hierarchy)
    emp_hierarchy.to_csv('out1.csv')
    end_time = datetime.now()
    print(end_time)
    print(end_time - start_time)
    print(end_time - query_end_time)

def set_hierarchy(emp_hierarchy,unprocess_emp,latest_emp_dump):
    for index, row in unprocess_emp.iterrows():
        if(row['PROCESSED'] == False and int(row['HAS_EMPLOYEE_DIRECT_REPORTS']) > 0):
            mgr_reports = get_direct_reports(row['LOGIN'],row['HIERARCHY_LEVEL'],latest_emp_dump)
            unprocess_emp= unprocess_emp.append(mgr_reports)
            unprocess_emp.at[index,'PROCESSED'] = True
        if(row['PROCESSED'] == False and int(row['HAS_EMPLOYEE_DIRECT_REPORTS']) == 0):
            unprocess_emp.at[index,'PROCESSED'] = True
    emp_hierarchy = emp_hierarchy.append(unprocess_emp) 
    emp_hierarchy = emp_hierarchy.sort_values(by=['HIERARCHY_LEVEL','LOGIN','PROCESSED'])
    emp_hierarchy = emp_hierarchy.drop_duplicates(subset=['LOGIN'],keep='last')
    return emp_hierarchy


def get_direct_reports(manager_id,manager_level,latest_emp_dump):
    mgr_reports = latest_emp_dump[latest_emp_dump["MGR_LOGIN"]== manager_id]
    mgr_details = latest_emp_dump[latest_emp_dump["LOGIN"]== manager_id]
   
    mgr2_details = latest_emp_dump[latest_emp_dump["LOGIN"]==mgr_details['MGR_LOGIN'].iloc[0]]

    mgr_reports['HIERARCHY_LEVEL'] = manager_level+1
    if mgr_details["MIDDLE"].iloc[0] is None:
        mgr_reports['MGR_NAME'] = mgr_details["LAST_NAME"].iloc[0]+", "+mgr_details["FIRST_NAME"].iloc[0]
    else:
        mgr_reports['MGR_NAME'] = mgr_details["LAST_NAME"].iloc[0]+", "+mgr_details["FIRST_NAME"].iloc[0]+" "+mgr_details["MIDDLE"].iloc[0]

    if mgr2_details["MIDDLE"].iloc[0] is None:
        mgr_reports['MGR2_NAME'] = mgr2_details["LAST_NAME"].iloc[0]+", "+mgr2_details["FIRST_NAME"].iloc[0]
    else:
        mgr_reports['MGR2_NAME'] = mgr2_details["LAST_NAME"].iloc[0]+", "+mgr2_details["FIRST_NAME"].iloc[0]+" "+mgr2_details["MIDDLE"].iloc[0]

    mgr_reports['MGR2_LOGIN'] = mgr2_details["LOGIN"].iloc[0]
    mgr_reports['PROCESSED'] = False
    return mgr_reports

if __name__ == '__main__':
    main()