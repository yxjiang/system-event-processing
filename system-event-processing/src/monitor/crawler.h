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
#include <sstream>
#include <boost/shared_ptr.hpp>
#include "../common/common.h"


//#include <sstream>

namespace event {

/*
 * The basic observe value, each node contains the observed timestamp and the observed value.
 */
typedef struct
{
public:
    time_t timestamp;   //  the timestamp of the observed value
    boost::shared_ptr<std::map<std::string, std::string> > properties_;
} ObserveData;


/*
 * The crawler that retrieve a particular type of meta-data.
 * We do not need any lock mechanism since only one thread operates the data.
 */
class Crawler
{
public:
    Crawler(const std::string &name);

    virtual ~Crawler();
    /*
     * Get the name of the crawler.
     */
    std::string GetCrawlerName() const;
    /*
     * Get the type of the meta-data.
     */
    std::string GetStreamType() const;
    /*
     * Get currently observed meta-data
     */
    ObserveData GetData();
    /*
     * Crawl the meta-data.
     */
    virtual void FetchData() = 0;

protected:
    std::string name_;
    std::string type_;
    ObserveData cur_data;
    pthread_rwlock_t rwlock_;
};

/*
 * Crawl that fetch the data of
 */
class CPUCrawler : public Crawler
{
public:
    enum Mode
    {
        TOTAL_CPU = 0,
        SEPARATE_CPU = 1
    };

    CPUCrawler(const std::string &name, Mode mode);
    virtual ~CPUCrawler();

    /*
     * Fetch the CPU usage.
     */
    void FetchData();

private:
    static std::string stat_file_;
    int cpu_index_;
    Mode mode_;
};


/*
 * Crawl the memory usage of the target system.
 */
class MemCrawler : public Crawler
{
public:
    MemCrawler(const std::string &name);
    virtual ~MemCrawler();
    /*
     * Fetch the Memory usage of the target system.
     */
    void FetchData();

private:
    static std::string mem_stat_file_;
};

/*
 *  Crawl the network usage of the target system.
 */
class NetCrawler : public Crawler
{
public:
    NetCrawler(const std::string &name);
    virtual ~NetCrawler();
    /*
     * Fetch the Network usage of the target system.
     */
    void FetchData();

private:
    static std::string net_stat_file_;
};

/*
 * Crawl the disk usage of the target system.
 */
class DiskCrawler : public Crawler
{
public:
    DiskCrawler(const std::string &name);
    virtual ~DiskCrawler();
    /*
     * Fetch the Disk usage of the target system.
     */
    void FetchData();

private:
    static std::string disk_stat_pipe;
};

};


#endif /* CRAWLER_H_ */
