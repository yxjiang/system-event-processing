/*
 * monitor.cc
 *
 *  Created on: Oct 29, 2012
 *      Author: yexijiang
 */

#include "monitor.h"

namespace event
{
  using namespace std;

  map<string, CrawlerStatus> Monitor::crawlers_;
  Monitor::Monitor(int communicationServicePort, std::vector<std::string> vecCollectorIps, int rateInSecond)
      : monitoringRate_(rateInSecond), communicationServicePort_(communicationServicePort)
  {
    pthread_rwlock_init(&rwlock_, NULL);
    //  start all crawlers

    //  initialize communication service

    //  register to collectors by sending profile, including stable meta-data

  }

  Monitor::~Monitor()
  {
    pthread_rwlock_wrlock(&rwlock_);
    this->fetchDataServiceStop_ = true;
    this->commandServiceStop_ = true;
    this->pushDataServiceStop_ = true;
    pthread_rwlock_unlock(&rwlock_);

    //  release crawlers
    map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
    for(; itr != crawlers_.end(); ++itr)
        delete crawlers_;

    //  close all services


    pthread_rwlock_destroy(&rwlock_);
  }

  vector<string> Monitor::GetCrawlerNames()
  {
    vector<string> vecNames;
    map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
    for(; itr != crawlers_.end(); ++itr)
    {
      vecNames.push_back(itr->first);
    }
    return vecNames;
  }

  void Monitor::Attach(Crawler *crawler)
  {
    CrawlerStatus status;
    status.crawler = crawler;
    status.running = false;
    status.pid = 0;
    crawlers_.insert(make_pair<string, CrawlerStatus>(crawler->GetCrawlerName(), status));
  }

  void Monitor::Detach(const string &name)
  {
    crawlers_.erase(name);
  }

  Crawler *Monitor::GetCrawler(const std::string &name) const
  {
    return crawlers_.find(name)->second.crawler;
  }

  /*!
   * Thread entry function for communication service.
   */
  void *_CommandService(void *arg)
  {

  }

  /*!
   * Send profile data to collectors.
   */
  void _RegisterToCollectors()
  {

  }

  /*!
   * Handle renew event. Update crawlers information accordingly.
   * Resume to send meta-data to the recovered collector.
   */
  void _HandleCollectorRenew()
  {

  }

  /*!
   * Push data to collectors. The data is compressed by protocol buffer.
   */
  void *_PushData(void *arg)
  {

  }

  /*!
   * Assemble the meta-data grabbed by crawlers into JSON format.
   */
  std::string _AssembleJson()
  {

  }
}

