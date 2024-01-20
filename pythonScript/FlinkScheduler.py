# encoding:utf-8
import argparse
import os
import urllib
import urllib2
import json
import time
import shutil
import MySQLdb
import sys


flink_jar = "/opt/soft/flink/lib"
remoteSqlPluginPath = "/opt/soft/flinkstreamsql/plugins"
localSqlPluginPath = "/opt/soft/flinkstreamsql/plugins"
mode = "yarnPer"
yarnconf = "/opt/soft/hadoop/etc/hadoop"
flinkconf = "/opt/soft/flink/conf"
savePointKey="flinkCheckpointDataURI";
savePoint="hdfs://zzcluster/flink/flink-checkpoints"
isClearSavePointPath = "sql.checkpoint.cleanup.mode"
hostName="192.168.187.245"
userName="root"
password="root"
dbName="skynet"
yarnSchuedlerURL= "http://10.126.98.197:8088/ws/v1/cluster/scheduler"
coreThreshold = 0.5
memoryThreshold = 0.5

def getUserName(pyName):
    """
    根据OA的名获取中文名
    """
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName,password , dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "select name from t_account where pyname = '" + pyName + "'"
    username = ""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            username = row[0]
            # 打印结果
            # print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return username

def getParams(zzdpJobId):
    """
      根据zzdpJobId获取参数
      """
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName, password, dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "select `sql`,settings from t_zstream_job_config where id = " + zzdpJobId
    sql_str = ""
    settings=""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            sql_str = row[0]
            settings = row[1]
            # 打印结果
            # print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return sql_str,settings

def write_sql_file(application_name,sql_str):
    """
     写SQL文件
    :param application_name:
    :return:
    """
    sql_path = "/tmp/" + application_name + ".sql"
    isExists = os.path.exists(sql_path)
    if (isExists):
        os.remove(sql_path)
    with open(sql_path, "w") as f:
        f.write(sql_str)
    return sql_path





def getJobInfo(zzdpJobId):
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName, password, dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "select name,creator,proxy_code from t_zstream_job where id = "+ zzdpJobId
    creator = ""
    proxy_code = ""
    proxy_name = ""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            name = row[0]
            creator = row[1]
            proxy_code = row[2]
            # 打印结果
            #print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return name,creator,proxy_code;


def getUDXids(zzdpJobId):
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName, password, dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    udx_ids = []
    # SQL 查询语句
    sql = "select udx_id from t_zstream_job_udxs where job_id = "+ zzdpJobId
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            udx_ids.append(row[0])
            # 打印结果
            #print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return udx_ids;

def getUDXInfo(udx_id):
    """
    根据UDX id 获取 udx path
    :return:
    """
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName, password, dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    url = ""
    # SQL 查询语句
    sql = "select url from t_zstream_udx where id = " + udx_id
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            url = row[0]
            # 打印结果
            # print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return url;

def load_jar(udx_jars,department,oa):
    """
     把HDFS上的Jar包下载到本地
    :param udx_jars:
    :return:拼接好的字符串
    """
    ##如果目录不存在就创建目录
    userJarDir = flink_jar + "/udf/" + department + "/" + oa
    isExists = os.path.exists(userJarDir)
    if (isExists == False):
        os.makedirs(userJarDir)
     #拼接字符串
    jar_param="\\["
    for jar  in  udx_jars:
        fields = jar.split("/")
        jar_name=fields[fields.__len__() -1]

        userJarPath = flink_jar + "/udf/" + department + "/" + oa + "/"+ jar_name;
        if(os.path.exists(userJarPath)):
            os.remove(userJarPath)

        command = "hadoop fs -get "+jar + " "+userJarPath
        #print command
        os.system(command)
        jar_param += "\\\""+userJarPath+"\\\""+","

    ##记得拼接字符串
    #\[\"/opt/soft/flink/lib/tmp/udf/algo/myflinkudf.jar\",\"/opt/soft/flink/lib/tmp/udf/algo/udx.jar\"\]
    jar_param = jar_param[0:jar_param.__len__()-1]
    jar_param+="\\]"

    return jar_param




def analysisParam(setting):
    """
     解析JSON 拼接参数
    :param setting:
    :return:
    """
    myjson = json.loads(setting)
    flink_param="\\{"
    for key in myjson:
        if(key.__eq__( "retryTimes") or key.__eq__( "queue") or key.__eq__("cluster")):
            continue
        value = myjson[key]
        if value.__len__() ==0:
            continue
        if(value.isdigit()):
            if "sql.checkpoint.interval".__eq__(str(key)):
                value = str(int(value) * 1000)
            if "taskmanager.memory.mb".__eq__(str(key)):
                value = str(int(value) * 1024)
            flink_param += "\\\""+key+"\\\""+":"+value+","
        else:
            flink_param += "\\\""+ key +"\\\"" + ":" + "\\\"" + value + "\\\"" + ","
    ##默认都加上，当参数里面有sql.checkpoint.interval的参数的时候才会生效
    flink_param += "\\\""+savePointKey+"\\\""+":"+ "\\\"" + savePoint + "\\\"" + ","
    flink_param += "\\\"" + isClearSavePointPath + "\\\"" + ":false"  + ","

    flink_param = flink_param[0:flink_param.__len__() -1]+"}"
    return flink_param


def getValueByKey(setting,key):
    """
    根据传入的key 获取value值
    :param setting:
    :param key:
    :return:
    """
    myjson = json.loads(setting)
    return myjson[key]


def getFlinkJobId(zzdpJobId):
    """
    获取任务的JobId
    :param zzdpJobId:
    :return:
    """
    # 打开数据库连接
    db = MySQLdb.connect(hostName, userName, password, dbName, charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "select application_job_id from t_zstream_job where id = "+ zzdpJobId
    application_job_id = ""
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            application_job_id = row[0]
            # 打印结果
            #print "flink_job_id=%s" % (flink_job_id)
    except:
        print "Error: unable to fecth data"
    # 关闭数据库连接
    db.close()
    return application_job_id


def getSavePointPath(zzdpJobId):
    """
    根据jobId找SavePointPath
    :param flink_job_id:
    :return:SavePointPath
    """
    flink_job_id = getFlinkJobId(zzdpJobId)
    command = " hadoop fs -ls "+savePoint+"/"+flink_job_id+"/chk* | grep metadata | awk -F ' ' '{print $8}'"
    result = os.popen(command)
    flink_savepoint_path = result.read()
    return flink_savepoint_path


def createCommand(zzdpJobId):
    name,creator,proxy_code=getJobInfo(zzdpJobId)
    ##获取用户中文名
    username = getUserName(creator)

    ##获取参数
    sql_str, settings = getParams(zzdpJobId)
    ##SQL文件处理
    application_name = zzdpJobId + "_" + username + "_" + creator + "_" + name
    sql_path = write_sql_file(application_name,sql_str)
    ##UDX处理
    udx_ids = getUDXids(zzdpJobId)
    jar_param = ""
    if(udx_ids.__len__() > 0):
        udx_jars = []
        if (udx_ids.__len__() != 0):
            for udx_id in udx_ids:
                udx_jars.append(getUDXInfo(str(udx_id)))
        # 下载UDX文件，并且拼接好字符串#
        jar_param = load_jar(udx_jars, proxy_code, creator)

    ##解析参数settings
    flink_param = analysisParam(settings)
    #获取队列
    queue = getValueByKey(settings,'queue')
    #获取Flink版本
    cluster = getValueByKey(settings, 'cluster')
    command="";
    if cluster == "flink-1.6":
        if udx_ids.__len__() > 0:
            ##如果有Jar 包
            command += "/opt/soft/flinkstreamsql/bin/submit.sh -sql " + sql_path + " -name " + application_name + \
                       " -flinkJarPath " + flink_jar + " -addjar " + jar_param + " -remoteSqlPluginPath " + remoteSqlPluginPath + \
                       " -localSqlPluginPath " + localSqlPluginPath + "  -mode " + mode + " -yarnconf " + yarnconf + \
                       " -flinkconf " + flinkconf + " -queue " + queue + " -confProp " + flink_param
        else:
            ##没有udx jar包
            command += "/opt/soft/flinkstreamsql/bin/submit.sh -sql " + sql_path + " -name " + application_name + \
                       " -flinkJarPath " + flink_jar + " -remoteSqlPluginPath " + remoteSqlPluginPath + \
                       " -localSqlPluginPath " + localSqlPluginPath + "  -mode " + mode + " -yarnconf " + yarnconf + \
                       " -flinkconf " + flinkconf + " -queue " + queue + " -confProp " + flink_param
    return command






def createSavePointCommand(zzdpJobId):
    """
     ## status != 0  参数里有：sql.checkpoint.interval 这个参数 才能进行恢复数据
    :param zzdpJobId:
    :return:
    """
    command = createCommand(zzdpJobId)
    ##恢复数据
    savePointMetadata = getSavePointPath(zzdpJobId)
    command += " -savePointPath " + savePointMetadata
    return command

def isSubmit():
    """
    判断资源是否充足
    :return: 如果返回true代表资源充足可以提交任务，如果false资源不够
    """
    url = yarnSchuedlerURL # 页面的地址
    response = urllib2.urlopen(url)  # 调用urllib2向服务器发送get请求
    statusInfo=response.read()  # 获取服务器返回的页面信息
    statusInfoJson = json.loads(statusInfo)
    clusterResources=statusInfoJson['scheduler']['schedulerInfo']['rootQueue']['clusterResources']
    usedResources=statusInfoJson['scheduler']['schedulerInfo']['rootQueue']['usedResources']
    clusterResourcesVcores=clusterResources['vCores']
    clusterResourcesMemory = clusterResources['memory']
    usedResourcesVcores=usedResources['vCores']
    usedResourcesMemory = usedResources['memory']
    vcoresPerce=float(usedResourcesVcores) / float(clusterResourcesVcores)
    memoryPerce =float(usedResourcesMemory) / float(clusterResourcesMemory)
    if(vcoresPerce > coreThreshold or memoryPerce > memoryThreshold):
        return False;
    return True

if __name__ == "__main__":
    """
    接收参数
    """
    ##设置字符集
    reload(sys)
    sys.setdefaultencoding("utf-8")


    parser = argparse.ArgumentParser(description='Process some integers.')
    # zzdp任务的Job ID
    parser.add_argument('-zzdpJobId', type=str, default=None)
    # 提交任务 还是进行SQL语义校验
    parser.add_argument('-checkSQL', type=str, default=None)
    # 是否需要 从上一次停的地方继续消费
    parser.add_argument('-recoveryOffset',type=str, default=None)
    args = parser.parse_args()
    zzdpJobId = args.zzdpJobId
    checkSQL = args.checkSQL
    recoveryOffset = args.recoveryOffset


    command =""
    if("check".__eq__(checkSQL)):
        print "检测SQL语法....."
        command = createCommand(zzdpJobId)
        command += " -checkSQL check"
        checkSubmit = os.popen(command)
        checkResult = checkSubmit.read()
        print checkResult
        exit()
    if("submit".__eq__(checkSQL)):
        # 判断资源是否充足#
        retry = 10
        while retry > 0:
            try:
                result = isSubmit()
                ##如果资源充足
                if (result):
                    print "资源足够，可以提交任务..."
                    ##调恢复数据的方法
                    if("savepoint".__eq__(recoveryOffset)):
                        print "调用SavePoint方法恢复数据..."
                        command = createSavePointCommand(zzdpJobId)
                        print command
                    else:
                        ##调正常的方法
                        print "正常启动任务..."
                        command = createCommand(zzdpJobId)
                        print command
                    command += "-checkSQL submit"
                    submitResult = os.popen(command)
                    print submitResult.read()
                else:
                    print "Zstream_code is ######[1]######"
                break;
            except urllib2.HTTPError as resut:
                retry -= 1
                time.sleep(1)
                print "检测资源请求失败异常信息"













