/*
 * crawler.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef CRAWLER_H_
#define CRAWLER_H_

#include <pthread.h>
#include <map>
#include <iostream>
#include <fstream>
#include <sstream>
#include <boost/shared_ptr.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "../common/common.h"

//#include <sstream>

namespace event
{

/*
 * The basic observe value, each node contains the observed timestamp and the observed value.
 */
typedef struct
{
public:
  time_t timestamp; //  the timestamp of the observed value
  boost::shared_ptr<std::map<std::string, std::string> > properties_;
} ObserveData;

/*
 * The crawler that retrieve a particular type of meta-data.
 * We do not need any lock mechanism since only one thread operates the data.
 */
class Crawler
{
public:
  /*!
   * Initialize the crawler, including set the type and get the stable meta-data.
   */
  Crawler();
  /*!
   * Deinitialize the crawler.
   */
  virtual ~Crawler();

  /*!
   * Initialize the crawler.
   */
  void Init();
  /*!
   * Get the name of the crawler.
   */
  std::string GetCrawlerName() const;
  /*!
   * Get the type of the meta-data.
   */
  std::string GetStreamType() const;
  /*!
   * Crawl the stable meta-data.
   */
  std::map<std::string, std::string> GetStableData() const;
  /*!
   * Crawl the meta-data.
   */
  virtual void FetchMetaData() = 0;
  /*!
   * Get currently observed meta-data
   */
  ObserveData GetData();

protected:
  /*!
   * Set the type of the crawler.
   */
  virtual void SetCrawlerType() = 0;
  /*!
   * Crawl the stable meta-data.
   */
  virtual void FetchStableMetaData() = 0;

protected:
  std::string name_;
  std::string type_;
  ObserveData curData_;
  std::map<std::string, std::string> stableMetaData_;
  pthread_rwlock_t rwlock_;
};

class DummyCrawler : public Crawler
{
public:
  DummyCrawler();
  virtual ~DummyCrawler();

  void FetchMetaData();

protected:
  void SetCrawlerType();
  void FetchStableMetaData();

private:

};

/*
 * Crawl that fetch the data of
 */
class CPUCrawler: public Crawler
{
public:
  enum Mode
  {
    TOTAL_CPU = 0, //   get the overall stat
    SEPARATE_CPU = 1
  //   stat each CPU separately
  };

  CPUCrawler(Mode mode = SEPARATE_CPU);
  virtual ~CPUCrawler();
  /*
   * Fetch the CPU usage.
   */
  void FetchMetaData();

protected:
  void SetCrawlerType();
  void FetchStableMetaData();

private:
  static std::string statFile_;
//    int cpuIndex_;
  Mode mode_;
};

///*
// * Crawl the memory usage of the target system.
// */
//class MemCrawler : public Crawler
//{
//public:
//    MemCrawler();
//    virtual ~MemCrawler();
//    /*
//     * Fetch the Memory usage of the target system.
//     */
//    void FetchMetaData();
//
//private:
//    static std::string memStatFile_;
//};
//
///*
// *  Crawl the network usage of the target system.
// */
//class NetCrawler : public Crawler
//{
//public:
//    NetCrawler();
//    virtual ~NetCrawler();
//    /*
//     * Fetch the Network usage of the target system.
//     */
//    void FetchMetaData();
//
//private:
//    static std::string netStatFile_;
//};
//
///*
// * Crawl the disk usage of the target system.
// */
//class DiskCrawler : public Crawler
//{
//public:
//    DiskCrawler();
//    virtual ~DiskCrawler();
//    /*
//     * Fetch the Disk usage of the target system.
//     */
//    void FetchMetaData();
//
//private:
//    static std::string diskStatPipe_;
//};
//
}
;

#endif /* CRAWLER_H_ */
