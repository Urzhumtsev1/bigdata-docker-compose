import requests, json, time, textwrap

from airflow import AirflowException

headers = {'Content-Type': 'application/json'}
host = 'http://172.28.1.6:8998'
sessCmd = "/sessions"
stmtCmd = "/statements"

def getSessId(appName, doRaise=True):
    """ возвращает ID сессии по имени приложения в формате /ID или райзит эксепшн (можно отключить) """
    
    res = requests.get(host+sessCmd, headers=headers)
    if res:
        for sess in res.json()['sessions']:
            if sess['name'] == appName:
                return "/" + str(sess['id'])
            
    if doRaise:
        print("**getSessId ERROR**: no such app",appName)
        raise AirflowException
    else:
        return None
    
def createSess(name, executorMemory=None, numExecutors=None, executorCores=None):
    """ создает сессию с заданными параметрами или райзит ексепшн """
    
    args = locals()
    
    data = {
        'kind': 'pyspark'
    }
    for k, v in args.items():
        if v is not None:
            data[k] = v
    
    res = requests.post(host+sessCmd, data=json.dumps(data), headers=headers)
    if res:
        sessDict = res.json()
        sessId = "/"+str(sessDict['id'])
        while sessDict["state"] != "idle":
            time.sleep(1)
            sessDict = requests.get(host+sessCmd+sessId, headers=headers).json()
    else:
        print("**createSess ERROR**: status_code", res.status_code, ", text:", res.text)
        raise AirflowException

def execStmt(appName,stmt,doFail=False):
    """ выполняет python код в сессии, дожидаясь его завершения
    райзит ексепшн, если код дал ошибку и в параметрах не задано игнорирование ошибки
    """

    ssid = getSessId(appName)
    
    r = requests.post(host+sessCmd+ssid+stmtCmd, data=json.dumps({'code': stmt }), headers=headers)
    if r.ok:
        stmtDict = r.json()
        stmtId = "/"+str(stmtDict['id'])
        while stmtDict["state"]!="available":
            time.sleep(1)
            r = requests.get(host+sessCmd+ssid+stmtCmd+stmtId, headers=headers)
            if r.ok:
                stmtDict = r.json()
            else:
                print("**execStmt request failed**: r ",r)
                raise AirflowException
        # проверяем результат выполнения - райзим ошибку если не ОК
        print("**execStmt finished**: output ",str(stmtDict["output"]))
        if stmtDict["output"]["status"]!="ok" and doFail:
            raise AirflowException
    else:
        print("**execStmt creation ERROR**: status_code",r.status_code, ", text:",r.text)
        if doFail:
            raise AirflowException

def closeSess(appName):
    """ закрывает сессию """

    ssid = getSessId(appName,False)
    if ssid is not None:
        requests.delete(host+sessCmd+ssid, headers=headers)

def createSparkSession(appName):

    stmt = textwrap.dedent(f"""
        from pyspark.sql import SparkSession  
        sp = SparkSession.builder.master("http://172.28.1.1").appName({appName}).getOrCreate()      
    """)

    execStmt(appName,stmt)

def simpleTask(appName, n):

    stmt = textwrap.dedent(f"""
        df = sp.read.format("csv") \
            .option("mode", "FAILFAST") \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .option("path", "data/grades.csv") \
            .load()
        
        df.show()        
    """)

    execStmt(appName,stmt)
