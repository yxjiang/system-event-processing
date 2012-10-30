/*
 * monitor.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef MONITOR_H_
#define MONITOR_H_

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>
#include <map>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "../common/common.h"
#include "../common/utility.pb.h"
#include "crawler.h"

namespace event
{

/*!
 * Data structure to store the status of a crawler.
 */
typedef struct
{
  Crawler *crawler;
  bool running;
  pthread_t pid;
} CrawlerStatus;

typedef struct
{
  std::string collectorIP;
  std::string compressedContent;
} DataPackage;

/*!
 * A monitor is deployed on a single machine.
 * It periodically grabs the system utilization data and push it to collector.
 * Typically, a monitor contains a set of crawler plugins, each grab a certain type of utilization data.
 *
 */
class Monitor
{
public:
  /*!
   * Initialize the monitor.
   * It does several works:
   * 1. Wait on commandPort for remote command.
   * 2. Start all crawlers.
   * 3. Register to collectors.
   * \param     commandPort     The port number for monitor to communicate with collectors.
   * \param     vecCollectorIps The list of IPs for all the collectors.
   * \param     rateInSecond    The monitoring rate, default is 1 second.
   */
  Monitor(std::vector<std::string> vecCollectorIps, int rateInSecond = 1, int commandPort = 32100, int collectorRegistrationPort = 32167,
      int collectorDataPort = 32168);

  /*!
   * Deinitialize the monitor.
   */
  ~Monitor();

  /*!
   * Start the monitor.
   */
  void Run();

  /*!
   * Get the names of all the crawlers.
   */
  std::vector<std::string> GetCrawlerNames();

  /*!
   * Attach a new crawler.
   */
  void Attach(Crawler *crawler);

  /*!
   * Detach a crawler by its name.
   */
  void Detach(const std::string &name);

  /*
   * Get a particular crawler by its name.
   */
  Crawler *GetCrawler(const std::string &name) const;

private:

  /*!
   * The thread function entry for fetching data.
   */
  static void *_CrawlerService(void *arg);

//  /*!
//   * The thread entry function for command service task.
//   */
//  static void *_CommandService(void *arg);
//
//  /*!
//   * Register to the collectors by sending the profiles of the monitor.
//   * Also, the stable meta-data grabbed by crawlers are running running  also sent.
//   */
//  static void _RegisterToCollectors();
//
//  /*!
//   * Handle the collector renew event.
//   * This event happens when the corresponding collector for this monitor recover from crash.
//   * And then the recovered collector asks for the reconnection for all the monitors it previously communicated.
//   */
//  static void _HandleCollectorRenew(int socketFd);
//
//  /*!
//   * Thread entry function.
//   * Push the meta-data to collectors periodically.
//   */
//  static void *_PushDataMainThread(void *arg);
//
//  /*!
//   * The worker thread to push data to specified collector
//   */
//  static void *_PushDataWorkerThread(void *arg);
//
//  /*!
//   * Generate the json and return it as text.
//   */
//  static const std::string _AssembleStatbleMetaDataJson();
//
//  /*!
//   * Generate the json and return it as text.
//   */
//  static const std::string _AssembleDynamicMetaDataJson();




private:
  static char machineName_[256];
  static std::map<std::string, CrawlerStatus> crawlers_;

  /*    fetch data task related     */
  static int monitoringRate_;
  static pthread_rwlock_t stopSymbolrwlock_;
  static bool fetchDataServiceStop_;


  static pthread_rwlock_t collectorStatusrwlock_;

  static int collectorRegistrationPort_;
  static int collectorDataPort_;

  static int communicationServicePort_;
  static int commandServiceSocketFd;        //      file descriptor for command socket
  pthread_t communicationServicePid_;
  static bool commandServiceStop_;
  pthread_t pushDataServicePid_;
  static bool pushDataServiceStop_;
  static std::map<std::string, bool> collectorStatus_; //  each entry indicates whether the collector works properly or crash

};


}

#endif /* MONITOR_H_ */
