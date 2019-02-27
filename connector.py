import requests
import json
import ConfigParser
import sys
import time
import os
import logging
import urllib3

def checkPID():
    return os.path.isfile('/var/run/hiro-connect/snow-connector.pid')

def createPIDfile():
    pidfile = open('/var/run/hiro-connect/snow-connector.pid', 'w')
    pid = os.getpid()
    pidfile.write("%s\n" % pid)
    pidfile.close()

def removePIDfile():
    os.remove('/var/run/hiro-connect/snow-connector.pid')

def main():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    log_path = '/var/log/autopilot/connectit/snow-connector.log'

    logging.basicConfig(filename=log_path, level=logging.DEBUG, format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    logging.debug("DEBUG: Connector is starting")
    logging.info("INFO: Connector is starting")
    logging.warning("WARNING: Connector is starting")

    if ( len(sys.argv) != 2):
        logging.warning('Usage: ' + sys.argv[0] + ' <config_file_path>. Exiting....')
        exit(1)

    if (checkPID() == True):
        logging.debug('Cannot run - pid file exists with PID:')
        exit(2)

    createPIDfile()

    config = ConfigParser.ConfigParser()
    config.read(sys.argv[1])

    sdf = config.get('HIRO', 'sdf')
    sdf = json.loads(sdf)
    user = config.get('SNOW', 'user')
    pwd = config.get('SNOW', 'password')
    headers = {"Content-Type":"application/json","Accept":"application/json"}
    hiro_url = config.get('HIRO', 'url')
    date_format = config.get('CONNECTOR', 'date_format')

    while True:
        dat_filter = []
        dat_file = config.get('CONNECTOR', 'dat_file')
        with open(dat_file) as h:
            for line in h:
                dat_filter.append(line.rstrip('\n'))
        h.close()

        day = (time.strftime("%Y-%m-%d"))
        hour = (time.strftime("%H:%M:%S"))
        sysdate = day + '%20' + hour
        url = config.get('SNOW', 'url') + '?sysparm_query=sys_updated_on%3E=' + str(dat_filter[0]) + "^sys_updated_by!=" + user + "&" + config.get('SNOW', 'filter_field') + "=" + config.get('SNOW', 'assignment_group_name')
        try:
            response = requests.get(url, auth=(user, pwd), headers=headers)
        except:
            time.sleep(60) ### interval to be read form config
            continue
        else:
            data = response.json()
            task_count = len(data['result'])
            logging.info('received ' + str(task_count) + ' objects')
            counter = 0
            while ( counter < task_count ):
                sdf['mand']['sourceId'] = 'SNOW DEV' ###instance to be read from config
                sdf['mand']['sourceTicketId'] = str(data['result'][counter]['number'])
                sdf['mand']['description'] = data['result'][counter]['short_description']
                sdf['mand']['eventName'] = data['result'][counter]['number']
                sdf['free']['technical_details'] =  data['result'][counter]['description']
                sdf['free']['State'] = "New"
                sdf['free']['impact'] = data['result'][counter]['impact']
                sdf['opt']['sourceStatus'] = "New"
                sdf['mand']['summary'] = data['result'][counter]['short_description']
                sdf['free']['assigned_to'] = data['result'][counter]['assigned_to']
                sdf['free']['sys_id'] = data['result'][counter]['sys_id']
                sdf['free']['worknotes'] = "Ticket sent to HIRO for processing"
                try:
                    sdf['free']['affectedCI'] =   data['result'][counter]['cmdb_ci']['value']
                except:
                    sdf['free']['affectedCI'] = "DefaultIssueNode"
                try:
                    sdf['opt']['affectedCI'][0] =   data['result'][counter]['cmdb_ci']['value']
                except:
                    sdf['opt']['affectedCI'][0] = "DefaultIssueNode"
                try:
                    url = str(data['result'][counter]['u_requestor']['link'])
                except:
                    sdf['free']['u_requestor'] = 'empty'
                else:
                    try:
                        req_data = requests.get(url, auth=(user, pwd), headers=headers)
                    except:
                        sdf['free']['u_requestor'] = 'empty'
                    else:
                        if req_data.status_code != 200:
                            logging.debug('Wrong username in SNOW')
                            logging.debug(req_data.json())
                        req_data_json = req_data.json()
                        try:
                            sdf['free']['u_requestor'] = req_data_json['result']['user_name']
                        except:
                            sdf['free']['u_requestor'] = 'empty'
                url2 =  data['result'][counter]['u_incident_severity_type']['link']
                try:
                    response2 = requests.get(url2, auth=(user, pwd), headers=headers)
                except:
                    sdf['free']['u_incident_severity_type'] = "undefined"
                else:
                    severity = response2.json()
                    try:
                        sdf['free']['u_incident_severity_type'] = severity['result']['label']
                    except:
                        sdf['free']['u_incident_severity_type'] = "undefined"
                hiro_headers = {"Content-Type":"application/json", "Cache-Control": "no-cache"}
                payload = (json.dumps(sdf)).decode('utf-8')
                logging.info('Created incident in HIRO: ' + sdf['mand']['sourceTicketId'] + ' requested by ' + sdf['free']['u_requestor'])
                try:
                    post = requests.post(hiro_url, headers=hiro_headers, data=payload, verify=False)
                except:
                    logging.debug('POST error for ' + str(sdf['mand']['sourceTicketId']))
                else:
                    if post.status_code != 200:
                        logging.debug('POST error')
                        logging.debug(post.json())
                counter = counter + 1
        with open(dat_file, "w") as h:
            h.write("%s\n" % sysdate)
        h.close()
        time.sleep(60)
    removePIDfile()


if( __name__ == "__main__" ):
    main()
