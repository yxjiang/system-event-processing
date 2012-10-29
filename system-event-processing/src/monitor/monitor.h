/*
 * monitor.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef MONITOR_H_
#define MONITOR_H_

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
 * A monitor is deployed on a single node.
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
   */
  Monitor(int commandPort, std::vector< std::string > vecCollectorIps, int rateInSecond);

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
  void _RegisterToCollectors();

  /*!
   * Handle the collector renew event.
   * This event happens when the corresponding collector for this monitor recover from crash.
   * And then the recovered collector asks for the reconnection for all the monitors it previously communicated.
   */
  void _HandleCollectorRenew();

  /*!
   * Thread entry function.
   * Push the meta-data to collectors periodically.
   */
  static void *_PushData(void *arg);


private:
  int monitoringRate_;
  int commandListeningPort_;
  pthread_t commandLiseningPid_;
  std::map<std::string, bool> collectorStatus_; //  each entry indicates whether the collector works properly or crash
};


}

#endif /* MONITOR_H_ */
