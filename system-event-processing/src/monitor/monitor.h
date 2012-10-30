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
#include <boost/algorithm/string/predicate.hpp>
#include "../common/common.h"

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
  Monitor(int commandPort, std::vector< std::string > vecCollectorIps, int collectorRegistrationPort = 32167, int collectorDataPort = 32168, int rateInSecond = 1);

  /*!
   * Deinitialize the monitor.
   */
  ~Monitor();

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
   * The thread entry function for command service task.
   */
  static void *_CommandService(void *arg);

  /*!
   * Register to the collectors by sending the profiles of the monitor.
   * Also, the stable meta-data grabbed by crawlers are running running  also sent.
   */
  static void _RegisterToCollectors();

  /*!
   * Handle the collector renew event.
   * This event happens when the corresponding collector for this monitor recover from crash.
   * And then the recovered collector asks for the reconnection for all the monitors it previously communicated.
   */
  static void _HandleCollectorRenew(int socketFd);

  /*!
   * Thread entry function.
   * Push the meta-data to collectors periodically.
   */
  static void *_PushDataThread(void *arg);

  /*!
   * Generate the json and return it as text.
   * The data should be compressed by protocol buffer.
   */
  static const std::string _AssembleJson();

  /*!
   * The thread function entry for fetching data.
   */
  static void *_CrawlerService(void *arg);


private:
  std::map<std::string, bool> collectorStatus_;
  pthread_rwlock_t stopSymbolrwlock_;
  int monitoringRate_;
  static int collectorRegistrationPort_;
  static int collectorDataPort_;
  bool fetchDataServiceStop_;
  int communicationServicePort_;
  int commandServiceSocketFd;        //      file descriptor for command socket
  pthread_t communicationServicePid_;
  bool commandServiceStop_;
  pthread_t pushDataServicePid_;
  bool pushDataServiceStop_;
  static std::map<std::string, bool> collectorStatus_; //  each entry indicates whether the collector works properly or crash
  static std::map<std::string, CrawlerStatus> crawlers_;
};


}

#endif /* MONITOR_H_ */
