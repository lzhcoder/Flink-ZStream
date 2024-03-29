/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dtstack.flink.sql.sink.redis;

import com.bj58.zhuanzhuan.jodis.RoundRobinJedisPool;
import com.dtstack.flink.sql.sink.MetricOutputFormat;
import com.dtstack.flink.sql.sink.redis.table.RedisDataType;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import redis.clients.jedis.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

public class RedisOutputFormat extends MetricOutputFormat {

    private String url;

    private String database;

    private String tableName;

    private String password;

    private int redisType;

    private String maxTotal;

    private String maxIdle;

    private String minIdle;

    private String masterName;

    //add by lixiyuan
    private String dataType;

    //codis 集群信息
    private String codisZkClient;
    private String codisZkDir;
    private String codisKey;
    private String codisTeam;
    private int expire;


    protected String[] fieldNames;

    protected TypeInformation<?>[] fieldTypes;

    protected List<String> primaryKeys;

    protected int timeout;


    private JedisPool pool;

    private JedisCommands jedis;
    //转转codis集群
    private com.bj58.zhuanzhuan.redis.clients.jedis.Jedis jedis58;

    private JedisSentinelPool jedisSentinelPool;

    private GenericObjectPoolConfig poolConfig;

    private RedisOutputFormat(){
    }
    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        establishConnection();
        initMetric();
    }

    private GenericObjectPoolConfig setPoolConfig(String maxTotal, String maxIdle, String minIdle){
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        if (maxTotal != null){
            config.setMaxTotal(Integer.parseInt(maxTotal));
        }
        if (maxIdle != null){
            config.setMaxIdle(Integer.parseInt(maxIdle));
        }
        if (minIdle != null){
            config.setMinIdle(Integer.parseInt(minIdle));
        }
        return config;
    }

    private void establishConnection() {
        poolConfig = setPoolConfig(maxTotal, maxIdle, minIdle);
        String firstIp = "";
        String firstPort = "";
        Set<HostAndPort> addresses = new HashSet<>();
        Set<String> ipPorts = new HashSet<>();
        //判断是否有url
       if(url !=null && url.length() > 8){
           String[] nodes = url.split(",");
           String[] firstIpPort = nodes[0].split(":");
           firstIp = firstIpPort[0];
           firstPort = firstIpPort[1];

           for (String ipPort : nodes) {
               ipPorts.add(ipPort);
               String[] ipPortPair = ipPort.split(":");
               addresses.add(new HostAndPort(ipPortPair[0].trim(), Integer.valueOf(ipPortPair[1].trim())));
           }
       }
        if (timeout == 0){
            timeout = 10000;
        }
        if (database == null)
        {
            database = "0";
        }

        switch (redisType){
            //单机
            case 1:
                pool = new JedisPool(poolConfig, firstIp, Integer.parseInt(firstPort), timeout, password, Integer.parseInt(database));
                jedis = pool.getResource();
                break;
            //哨兵
            case 2:
                jedisSentinelPool = new JedisSentinelPool(masterName, ipPorts, poolConfig, timeout, password, Integer.parseInt(database));
                jedis = jedisSentinelPool.getResource();
                break;
            //集群
            case 3:
                jedis = new JedisCluster(addresses, timeout, timeout,10, password, poolConfig);
                //转转codis集群
            case 4:
                RoundRobinJedisPool jodisPool = RoundRobinJedisPool.create()
                        .curatorClient(codisZkClient, 30000)
                        .zkProxyDir(codisZkDir)
                        .team(codisTeam)
                        .appKey(codisKey)
                        .password(password).build();
                jedis58=jodisPool.getResource();


        }
    }



    @Override
    public void writeRecord(Tuple2 record) throws IOException {

        Tuple2<Boolean, Row> tupleTrans = record;
        Boolean retract = tupleTrans.getField(0);
        if (!retract) {
            return;
        }
        Row row = tupleTrans.getField(1);
        if (row.getArity() != fieldNames.length) {
            return;
        }

        HashMap<String, Integer> map = new HashMap<>();

        for (String primaryKey : primaryKeys){
            for (int i=0; i<fieldNames.length; i++){
                if (fieldNames[i].equals(primaryKey)){
                    map.put(primaryKey, i);
                }
            }
        }

        List<String> kvList = new LinkedList<>();
        for (String primaryKey : primaryKeys){
            StringBuilder primaryKV = new StringBuilder();
            int index = map.get(primaryKey).intValue();
            primaryKV.append(primaryKey).append(":").append(row.getField(index));
            kvList.add(primaryKV.toString());
        }

        String perKey = String.join(":", kvList);


        for (int i = 0; i < fieldNames.length; i++) {
            StringBuilder key = new StringBuilder();
            //zz:testtopic:hdp_ubu_zhuanzhuan_infologic:testtopic

//            key.append(codisKey).append("---").append(codisTeam).append("---").append(codisZkClient).append("---").append(codisZkDir);
//            key.append("===").append(dataType);

            key.append(tableName).append(":").append(perKey).append(":").append(fieldNames[i]);
            switch (redisType){
                case 1:
                case 2:
                case 3:
                    //方式一：redis
                    if(RedisDataType.READIS_DATA_TYPE_STRING.equalsIgnoreCase(dataType)){
                        jedis.set(key.toString(), row.getField(i).toString());
                        //设置过期时间
                        if(expire != 0){
                            jedis.expire(key.toString(),expire);
                        }
                    }else  if(RedisDataType.READIS_DATA_TYPE_LIST.equalsIgnoreCase(dataType)){
                        jedis.lpush(key.toString(), row.getField(i).toString());
                        //设置过期时间
                        if(expire != 0){
                            jedis.expire(key.toString(),expire);
                        }
                    }else{//可以支持更多类型
                        //nothing
                    }
                    break;
                case 4:
                    //方式二:转转的codis
                    if(RedisDataType.READIS_DATA_TYPE_STRING.equalsIgnoreCase(dataType)){
                        jedis58.set(key.toString(), row.getField(i).toString());
                        //设置过期时间
                        if(expire != 0){
                            jedis58.expire(key.toString(),expire);
                        }
                    }else  if(RedisDataType.READIS_DATA_TYPE_LIST.equalsIgnoreCase(dataType)){
                        jedis58.lpush(key.toString(), row.getField(i).toString());
                        //设置过期时间
                        if(expire != 0){
                            jedis58.expire(key.toString(),expire);
                        }
                    }else{//可以支持更多类型
                        //nothing
                    }
                    break;

            }




        }
        outRecords.inc();
    }

    @Override
    public void close() throws IOException {
        if (jedisSentinelPool != null) {
            jedisSentinelPool.close();
        }
        if (pool != null) {
            pool.close();
        }
        if (jedis != null){
            if (jedis instanceof Closeable){
                ((Closeable) jedis).close();
            }
        }
        //释放资源
        if(jedis58 != null){
            if (jedis58 instanceof Closeable){
                ((Closeable) jedis58).close();
            }
        }

    }

    public static RedisOutputFormatBuilder buildRedisOutputFormat(){
        return new RedisOutputFormatBuilder();
    }

    public static class RedisOutputFormatBuilder{
        private final RedisOutputFormat redisOutputFormat;

        protected RedisOutputFormatBuilder(){
            this.redisOutputFormat = new RedisOutputFormat();
        }

        public RedisOutputFormatBuilder setUrl(String url){
            redisOutputFormat.url = url;
            return this;
        }

        public RedisOutputFormatBuilder setDatabase(String database){
            redisOutputFormat.database = database;
            return this;
        }

        public RedisOutputFormatBuilder setTableName(String tableName){
            redisOutputFormat.tableName = tableName;
            return this;
        }

        public RedisOutputFormatBuilder setPassword(String password){
            redisOutputFormat.password = password;
            return this;
        }

        public RedisOutputFormatBuilder setFieldNames(String[] fieldNames){
            redisOutputFormat.fieldNames = fieldNames;
            return this;
        }

        public RedisOutputFormatBuilder setFieldTypes(TypeInformation<?>[] fieldTypes){
            redisOutputFormat.fieldTypes = fieldTypes;
            return this;
        }

        public RedisOutputFormatBuilder setPrimaryKeys(List<String > primaryKeys){
            redisOutputFormat.primaryKeys = primaryKeys;
            return this;
        }

        public RedisOutputFormatBuilder setTimeout(int timeout){
            redisOutputFormat.timeout = timeout;
            return this;
        }

        public RedisOutputFormatBuilder setRedisType(int redisType){
            redisOutputFormat.redisType = redisType;
            return this;
        }

        public RedisOutputFormatBuilder setMaxTotal(String maxTotal){
            redisOutputFormat.maxTotal = maxTotal;
            return this;
        }

        public RedisOutputFormatBuilder setMaxIdle(String maxIdle){
            redisOutputFormat.maxIdle = maxIdle;
            return this;
        }

        public RedisOutputFormatBuilder setMinIdle(String minIdle){
            redisOutputFormat.minIdle = minIdle;
            return this;
        }

        public RedisOutputFormatBuilder setMasterName(String masterName){
            redisOutputFormat.masterName = masterName;
            return this;
        }

        //add by lixiyuan
        public RedisOutputFormatBuilder setDataType(String dataType){
            redisOutputFormat.dataType = dataType;
            return this;
        }

        //codis
        public RedisOutputFormatBuilder setCodisZkClient(String codisZkClient){
            redisOutputFormat.codisZkClient = codisZkClient;
            return this;
        }


        public RedisOutputFormatBuilder setCodisZkDir(String codisZkDir){
            redisOutputFormat.codisZkDir = codisZkDir;
            return this;
        }

        public RedisOutputFormatBuilder setCodisKey(String codisKey){
            redisOutputFormat.codisKey = codisKey;
            return this;
        }

        public RedisOutputFormatBuilder setCodisTeam(String codisTeam){
            redisOutputFormat.codisTeam = codisTeam;
            return this;
        }

        public RedisOutputFormatBuilder setExpire(int expire){
            redisOutputFormat.expire=expire;
            return this;
        }


        public RedisOutputFormat finish(){
            //不检查URL了
//            if (redisOutputFormat.url == null){
//                throw new IllegalArgumentException("No URL supplied.");
//            }

            if (redisOutputFormat.tableName == null){
                throw new IllegalArgumentException("No tablename supplied.");
            }

            return redisOutputFormat;
        }
    }
}
