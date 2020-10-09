/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.util

import java.util.concurrent.TimeUnit

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.security.UserGroupInformation

/**
 * The motivation for caching is that destroying UGIs is slow. The alternative, creating and
 * destroying a UGI per-request, is wasteful.
 * <p>
 * Also need to cache UGI or do FileSystem.closeAllForUGI in order to avoid leaking Filesystems.
 * Refer to https://issues.apache.org/jira/browse/HDFS-3545 for details.
 */
private[spark] object UGICache {

  private val cache = CacheBuilder.newBuilder
    .expireAfterAccess(30, TimeUnit.MINUTES)
    .maximumSize(32)
    .build[String, UserGroupInformation]()

  def getUGI(user: String): UserGroupInformation = {
    cache.get(user, () => UserGroupInformation.createRemoteUser(user))
  }
}
