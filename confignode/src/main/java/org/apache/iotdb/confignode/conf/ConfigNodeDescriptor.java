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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.confignode.conf;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.exception.BadNodeUrlException;
import org.apache.iotdb.commons.utils.NodeUrlUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Properties;

public class ConfigNodeDescriptor {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeDescriptor.class);

  private final ConfigNodeConf conf = new ConfigNodeConf();
  private final CommonConfig commonConfig = CommonConfig.getInstance();

  private ConfigNodeDescriptor() {
    loadProps();
  }

  public ConfigNodeConf getConf() {
    return conf;
  }

  /** init common config according to iotdb config */
  private void initCommonConfig() {
    // first init the user and role folder in common config
    commonConfig.setUserFolder(conf.getSystemDir() + File.separator + "users");
    commonConfig.setRoleFolder(conf.getSystemDir() + File.separator + "roles");
  }

  /**
   * get props url location
   *
   * @return url object if location exit, otherwise null.
   */
  public URL getPropsUrl() {
    initCommonConfig();
    // Check if a config-directory was specified first.
    String urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_CONF, null);
    // If it wasn't, check if a home directory was provided
    if (urlString == null) {
      urlString = System.getProperty(ConfigNodeConstant.CONFIGNODE_HOME, null);
      if (urlString != null) {
        urlString =
            urlString
                + File.separatorChar
                + "conf"
                + File.separatorChar
                + ConfigNodeConstant.CONF_FILE_NAME;
      } else {
        // When start ConfigNode with the script, the environment variables CONFIGNODE_CONF
        // and CONFIGNODE_HOME will be set. But we didn't set these two in developer mode.
        // Thus, just return null and use default Configuration in developer mode.
        return null;
      }
    }
    // If a config location was provided, but it doesn't end with a properties file,
    // append the default location.
    else if (!urlString.endsWith(".properties")) {
      urlString += (File.separatorChar + ConfigNodeConstant.CONF_FILE_NAME);
    }

    // If the url doesn't start with "file:" or "classpath:", it's provided as a no path.
    // So we need to add it to make it a real URL.
    if (!urlString.startsWith("file:") && !urlString.startsWith("classpath:")) {
      urlString = "file:" + urlString;
    }
    try {
      return new URL(urlString);
    } catch (MalformedURLException e) {
      return null;
    }
  }

  private void loadProps() {
    URL url = getPropsUrl();
    if (url == null) {
      LOGGER.warn(
          "Couldn't load the ConfigNode configuration from any of the known sources. Use default configuration.");
      return;
    }

    try (InputStream inputStream = url.openStream()) {

      LOGGER.info("start reading ConfigNode conf file: {}", url);

      Properties properties = new Properties();
      properties.load(inputStream);

      conf.setRpcAddress(properties.getProperty("rpc_address", conf.getRpcAddress()));

      conf.setRpcPort(
          Integer.parseInt(properties.getProperty("rpc_port", String.valueOf(conf.getRpcPort()))));

      conf.setConsensusPort(
          Integer.parseInt(
              properties.getProperty("consensus_port", String.valueOf(conf.getConsensusPort()))));

      String targetConfigNode = properties.getProperty("target_confignode", null);
      if (targetConfigNode != null) {
        conf.setTargetConfigNode(NodeUrlUtils.parseTEndPointUrl(targetConfigNode));
      }

      conf.setSeriesPartitionSlotNum(
          Integer.parseInt(
              properties.getProperty(
                  "series_partition_slot_num", String.valueOf(conf.getSeriesPartitionSlotNum()))));

      conf.setSeriesPartitionExecutorClass(
          properties.getProperty(
              "series_partition_executor_class", conf.getSeriesPartitionExecutorClass()));

      conf.setDataNodeConsensusProtocolClass(
          properties.getProperty(
              "data_node_consensus_protocol_class", conf.getDataNodeConsensusProtocolClass()));

      conf.setRpcAdvancedCompressionEnable(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_advanced_compression_enable",
                  String.valueOf(conf.isRpcAdvancedCompressionEnable()))));

      conf.setRpcThriftCompressionEnabled(
          Boolean.parseBoolean(
              properties.getProperty(
                  "rpc_thrift_compression_enable",
                  String.valueOf(conf.isRpcThriftCompressionEnabled()))));

      conf.setRpcMaxConcurrentClientNum(
          Integer.parseInt(
              properties.getProperty(
                  "rpc_max_concurrent_client_num",
                  String.valueOf(conf.getRpcMaxConcurrentClientNum()))));

      conf.setThriftDefaultBufferSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_init_buffer_size", String.valueOf(conf.getThriftDefaultBufferSize()))));

      conf.setThriftMaxFrameSize(
          Integer.parseInt(
              properties.getProperty(
                  "thrift_max_frame_size", String.valueOf(conf.getThriftMaxFrameSize()))));

      conf.setConnectionTimeoutInMS(
          Integer.parseInt(
              properties.getProperty(
                  "connection_timeout_ms", String.valueOf(conf.getConnectionTimeoutInMS()))));

      conf.setSelectorNumOfClientManager(
          Integer.parseInt(
              properties.getProperty(
                  "selector_thread_nums_of_client_manager",
                  String.valueOf(conf.getSelectorNumOfClientManager()))));

      conf.setSystemDir(properties.getProperty("system_dir", conf.getSystemDir()));

      conf.setDataDirs(properties.getProperty("data_dirs", conf.getDataDirs()[0]).split(","));

      conf.setConsensusDir(properties.getProperty("consensus_dir", conf.getConsensusDir()));

      conf.setDefaultTTL(
          Long.parseLong(
              properties.getProperty("default_ttl", String.valueOf(conf.getDefaultTTL()))));

      conf.setTimePartitionInterval(
          Long.parseLong(
              properties.getProperty(
                  "time_partition_interval", String.valueOf(conf.getTimePartitionInterval()))));

      conf.setSchemaReplicationFactor(
          Integer.parseInt(
              properties.getProperty(
                  "schema_replication_factor", String.valueOf(conf.getSchemaReplicationFactor()))));

      conf.setDataReplicationFactor(
          Integer.parseInt(
              properties.getProperty(
                  "data_replication_factor", String.valueOf(conf.getDataReplicationFactor()))));

      conf.setInitialSchemaRegionCount(
          Integer.parseInt(
              properties.getProperty(
                  "initial_schema_region_count",
                  String.valueOf(conf.getInitialSchemaRegionCount()))));

      conf.setInitialDataRegionCount(
          Integer.parseInt(
              properties.getProperty(
                  "initial_data_region_count", String.valueOf(conf.getInitialDataRegionCount()))));
      commonConfig.setUserFolder(conf.getSystemDir() + File.separator + "users");
      commonConfig.setRoleFolder(conf.getSystemDir() + File.separator + "roles");

      String addresses = properties.getProperty("config_node_group_address_list", "0.0.0.0:22278");

      conf.setConfigNodeGroupAddressList(
          CommonUtils.parseNodeUrl(addresses).toArray(new TEndPoint[0]));
    } catch (IOException | BadNodeUrlException e) {
      LOGGER.warn("Couldn't load ConfigNode conf file, use default config", e);
    } finally {
      conf.updatePath();
    }
  }

  public static ConfigNodeDescriptor getInstance() {
    return ConfigNodeDescriptorHolder.INSTANCE;
  }

  private static class ConfigNodeDescriptorHolder {

    private static final ConfigNodeDescriptor INSTANCE = new ConfigNodeDescriptor();

    private ConfigNodeDescriptorHolder() {
      // empty constructor
    }
  }
}
