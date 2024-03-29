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

package com.dtstack.flink.sql.sink.kafka.table;

import com.dtstack.flink.sql.table.TargetTableInfo;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
/**
 *

 *
 */
public class KafkaSinkTableInfo extends TargetTableInfo {
    //version
    private static final String CURR_TYPE = "kafka10";

    public KafkaSinkTableInfo(){
        super.setType(CURR_TYPE);
    }

    public Map<String,String> kafkaParam = new HashMap<String,String>();

    public void addKafkaParam(String key,String value){
        kafkaParam.put(key,value);
    }

    public String getKafkaParam(String key){
        return kafkaParam.get(key);
    }

    public Set<String> getKafkaParamKeys(){
        return kafkaParam.keySet();
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(kafkaParam.get("bootstrap.servers"), "kafka of bootstrapServers is required");
        Preconditions.checkNotNull(kafkaParam.get("topic"), "kafka of topic is required");
        //Preconditions.checkNotNull(kafkaParam.get("groupId"), "kafka of groupId is required");
        return false;
    }

    @Override
    public String getType() {
        return super.getType();
    }
}
