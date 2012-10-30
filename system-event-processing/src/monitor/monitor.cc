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
Monitor::Monitor(int communicationServicePort, std::vector<std::string> vecCollectorIps,
    int rateInSecond) :
    monitoringRate_(rateInSecond), communicationServicePort_(communicationServicePort)
{
  pthread_rwlock_init(&rwlock_, NULL);
  //  start all crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    pthread_create(&(itr->second.pid), NULL, _CrawlerService, (void*) (itr->second.crawler));

  //  initialize communication service
  pthread_create(&(this->communicationServicePid_), NULL, _CommandService, NULL);
  //  register to collectors by sending profile, including stable meta-data
  pthread_create(&(this->pushDataServicePid_), NULL, _PushDataThread, NULL);

  _RegisterToCollectors();

  //  wait for all threads to stop
  itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    if (itr->second.pid > 0)
      pthread_join(itr->second.pid, NULL);

  pthread_join(this->communicationServicePid_, NULL);
  pthread_join(this->pushDataServicePid_, NULL);
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
  for (; itr != crawlers_.end(); ++itr)
    delete crawlers_;

  //  close socket
  close(this->commandServiceSocketFd);

  pthread_rwlock_destroy(&rwlock_);
}

vector<string> Monitor::GetCrawlerNames()
{
  vector<string> vecNames;
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
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
void *Monitor::_CommandService(void *arg)
{
  struct sockaddr_in serverAddr; // Server Internet address
  //  initialize server address
  bzero(&serverAddr, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htons(INADDR_ANY);
  serverAddr.sin_port = htons(this->communicationServicePort_);

  int communicationServiceSocket = socket(AF_INET, SOCK_STREAM, 0);
  if (communicationServiceSocket < 0)
  {
    fprintf(stderr, "[%s] Monitor communication service creates socket failed.\n",
        GetCurrentTime());
    exit(1);
  } else
    printf("[%s] Monitor communication service socket created...\n", GetCurrentTime());

  //  bind socket and address
  if (bind(communicationServiceSocket, (struct sockaddr*) &serverAddr, sizeof(serverAddr)))
  {
    fprintf(stderr, "[%s] Monitor communication service bind port: %d failed.\n", GetCurrentTime(),
        this->communicationServicePid_);
    exit(1);
  } else
    printf("[%s] Monitor communication service port binded to port %d...\n", GetCurrentTime(),
        this->communicationServicePid_);

  //  listen on port
  if (listen(communicationServiceSocket, 5))
  {
    fprintf(stderr, "[%s] Monitor communication service listen failed.\n", GetCurrentTime());
    exit(1);
  } else
    printf("[%s] Monitor service listening on port %d...\n", GetCurrentTime(),
        this->communicationServicePort_);

  while (true)
  {
    pthread_rwlock_rdlock(&this->rwlock_);
    if (this->commandServiceStop_ == true)
      break;
    pthread_rwlock_unlock(&this->rwlock_);

    int connectionSocket = accept(communicationServiceSocket, NULL, 0);
    stringstream recvContent;
    int recvBytes;
    char buffer[1024];
    while ((recvBytes = recv(connectionSocket, buffer, 1024, 0)) > 0)
    {
      if (recvBytes < 0)
      {
        fprintf(stderr, "[%s] Monitor receive command error.\n", GetCurrentTime());
      }
      recvContent << buffer;
    }

    string contentString = recvContent.str();

    if (boost::starts_with(contentString, "renew"))
      _HandleCollectorRenew(connectionSocket);

    close(connectionSocket);
  }

  return NULL;
}

/*!
 * Send profile data to collectors.
 */
void Monitor::_RegisterToCollectors()
{

}

/*!
 * Handle renew event. Update crawlers information accordingly.
 * Resume to send meta-data to the recovered collector.
 */
void Monitor::_HandleCollectorRenew(int connectionSocket)
{

}

/*!
 * Push data to collectors. The data is compressed by protocol buffer.
 */
void *Monitor::_PushDataThread(void *arg)
{

  return NULL;
}

/*!
 * Assemble the meta-data grabbed by crawlers into JSON format.
 */
const std::string Monitor::_AssembleJson()
{

}

}
