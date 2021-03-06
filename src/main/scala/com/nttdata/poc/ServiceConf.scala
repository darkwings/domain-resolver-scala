package com.nttdata.poc

case class Topics(source:String,
                  dest: String,
                  lookup: String,
                  dlq:String,
                  retry: String,
                  minInsyncReplicas:Int)

case class StateStore(dir:String,
                      ttlCheckEnabled:Boolean,
                      ttlCheckPeriodMs:Long,
                      ttlMs:Long,
                      cacheMaxBytesBuffering:Int,
                      commitIntervalMs:Long)

case class ExternalSystem(endpointUrl:String,
                          permitsPerSecond:String,
                          payloadRequestPath:String)

case class ServiceConf(bootstrapServers:String,
                       applicationId:String,
                       topics:Topics,
                       stateStore:StateStore,
                       externalSystem: ExternalSystem)
