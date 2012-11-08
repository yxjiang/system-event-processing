/*
 * collector.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef COLLECTOR_H_
#define COLLECTOR_H_

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>
#include <errno.h>
#include <map>
#include <boost/shared_ptr.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "../common/utility.pb.h"
#include "../common/common.h"

namespace event
{

typedef struct
{
  char *machineIP;
  int communicationFailCount;
} MonitorProfile;

typedef struct
{
  boost::uuids::uuid uuid;
  char *queryContent;
  long lastCalled;
  int queryInterval;
} QueryProfile;

class Collector
{
public:
  Collector(std::vector<std::string> vecPeerCollectorIPs, int communicationPort, int dataPort);
  ~Collector();

  void Run();
  void RegisterQuery(const std::string &queryContent, int queryInterval);

protected:
  /*!
   * Receive the commands.
   */
  static void *_CommandService(void *arg);\
  /*!
   * The worker thread to process commands.
   */
  static void *_CommandServiceWorker(void *arg);
  /*!
   * Scan the registered query every second, and execute query if necessary
   */
  static void *_SubscribeExecutor(void *arg);
  /*!
   * Send the query to all monitors via multicast.
   */
  static void *_SubscribeExecutorWorker(void *arg);
//  static void *_DataReceiveService(void *arg);
//  static void *_DataReceiveWorker(void *arg);

private:
  static std::vector<MonitorProfile*> monitorProfile_;
  static pthread_rwlock_t monitorProfileRwLock_;
  static pthread_attr_t threadAttr_;
  static pthread_t commandServicePid_;
  static int commandServicePort_;

  static int monitorCommandServicePort_;
  static pthread_t subscribeExecutorPid_; //  the thread that in charge of sending query to monitors
  static std::map<boost::uuids::uuid, QueryProfile*> registeredQueryProfiles_;
  static pthread_rwlock_t registeredQueryProfileRwlock_;
  //  static int dataServicePort_;
  //  static bool dataServiceStop_;

};

}


#endif /* COLLECTOR_H_ */
