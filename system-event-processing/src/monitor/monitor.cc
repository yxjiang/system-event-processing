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

int Monitor::collectorRegistrationPort_ = 32167;
int Monitor::collectorDataPort_ = 32168;
map<string, CrawlerStatus> Monitor::crawlers_;
Monitor::Monitor(int communicationServicePort, std::vector<std::string> vecCollectorIps,
    int collectorRegistrationPort, int collectorDataPort, int rateInSecond) :
    monitoringRate_(rateInSecond), communicationServicePort_(communicationServicePort)
{
  collectorRegistrationPort_ = collectorRegistrationPort;
  collectorDataPort_ = collectorDataPort;
  pthread_rwlock_init(&stopSymbolrwlock_, NULL);
  //  start all crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    pthread_create(&(itr->second.pid), NULL, _CrawlerService, (void*) (itr->second.crawler));

  //  initialize communication service
  pthread_create(&(this->communicationServicePid_), NULL, _CommandService, NULL);
  //  register to collectors by sending profile, including stable meta-data
  pthread_create(&(this->pushDataServicePid_), NULL, _PushDataThread, NULL);

  //  register to collectors
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
  pthread_rwlock_wrlock(&stopSymbolrwlock_);
  this->fetchDataServiceStop_ = true;
  this->commandServiceStop_ = true;
  this->pushDataServiceStop_ = true;
  pthread_rwlock_unlock(&stopSymbolrwlock_);

  //  release crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    delete crawlers_;

  //  close socket
  close(this->commandServiceSocketFd);

  pthread_rwlock_destroy(&stopSymbolrwlock_);
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

  bool stopService = false;
  while (true)
  {
    pthread_rwlock_rdlock(&this->stopSymbolrwlock_);
    stopService = this->commandServiceStop_;
    pthread_rwlock_unlock(&this->stopSymbolrwlock_);
    if(true == stopService)
      break;

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
  //  prepare the data need to be sent
  utility::MetaData stableMetaData;
  char machineName[256];
  gethostname(machineName, sizeof(machineName));
  stableMetaData.set_monitorname(machineName);
  stableMetaData.set_jsonstring(_AssembleStatbleMetaDataJson());
  string compressedContent = stableMetaData.SerializeAsString();

  //  iterative register to all collector
  struct hostent *collectorHostent = NULL;
  struct sockaddr_in collectorAddress;
  map<string, bool>::iterator collectorItr = collectorStatus_.begin();
  for (; collectorItr != collectorStatus_.end(); ++collectorItr)
  {
    //  send profile to collector via socket
    int socketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0)
    {
      fprintf(stderr, "[%s] During registration to collectors, failed to create socket. Monitor terminated\n",
          GetCurrentTime());
      exit(1);
    }

    if (collectorHostent != NULL)
      free(collectorHostent);
    collectorHostent = gethostbyname(collectorItr->first.c_str());
    if (collectorHostent == NULL)
    {
      fprintf(stderr, "[%s] During registration to collectors, no such collector with IP %s.\n",
          GetCurrentTime(), collectorItr->first.c_str());
      collectorItr->second = false;
      close(socketFd);
      continue;
    }

    bzero(&collectorAddress, sizeof(collectorAddress));
    collectorAddress.sin_family = AF_INET;
    bcopy((char *) collectorHostent->h_addr, (char *) &collectorAddress.sin_addr.s_addr, collectorHostent->h_length);
    collectorAddress.sin_port = htons(collectorRegistrationPort_);
    int numberOfTry = 0;
    bool success = false;
    //  retry for 3 times
    while (connect(socketFd, (struct sockaddr*) &collectorAddress, sizeof(collectorAddress)) < 0)
    {
      if (++numberOfTry > 3)
        break;
    }
    if (false == success)
    {
      fprintf(stderr, "[%s] During registration to collectors, connect to collector [%s] failed.\n",
          GetCurrentTime(), collectorItr->first.c_str());
      close(socketFd);
      exit(1);
    }

    int bytesSent;
    if (send(socketFd, compressedContent.c_str(), strlen(compressedContent.c_str()), 0) < 0)
    {
      fprintf(stderr,
          "[%s] During registration to collectors, failed to send the registration info.",
          GetCurrentTime());
      close(socketFd);
      exit(1);
    }
    close(socketFd);
    collectorItr->second = true;
  }

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
  string compressedDynamicInfo = _AssembleDynamicMetaDataJson();
  bool stopService = false;
  while(true)
  {
    pthread_rwlock_rdlock(&stopSymbolrwlock_);
    stopService = this->pushDataServiceStop_;
    pthread_rwlock_unlock(&stopSymbolrwlock_);
    if(true == stopService)
      break;

    map<string, bool>::iterator collectorItr = collectorStatus_.begin();
    for(; collectorItr != collectorStatus_.end(); ++collectorItr)
    {
      if(false == collectorItr->second)
        continue;

      //  send compressed data to collector
      struct hostent *collectorHostent = NULL;
      struct sockaddr_in collectorAddress;
      int socketFd = socket(AF_INET, SOCK_STREAM, 0);
      if (socketFd < 0)
      {
        fprintf(stderr, "[%s] During push data to collectors, failed to create socket. Monitor terminated\n",
            GetCurrentTime());
        continue;
      }

      if (collectorHostent != NULL)
        free(collectorHostent);
      collectorHostent = gethostbyname(collectorItr->first.c_str());
      if (collectorHostent == NULL)
      {
        fprintf(stderr, "[%s] During push data to collectors, no such collector with IP %s.\n",
            GetCurrentTime(), collectorItr->first.c_str());
        close(socketFd);
        continue;
      }

      bzero(&collectorAddress, sizeof(collectorAddress));
      collectorAddress.sin_family = AF_INET;
      bcopy((char *) collectorHostent->h_addr, (char *) &collectorAddress.sin_addr.s_addr, collectorHostent->h_length);
      collectorAddress.sin_port = htons(collectorRegistrationPort_);
      int numberOfTry = 0;
      bool success = false;
      //  retry for 3 times
      while (connect(socketFd, (struct sockaddr*) &collectorAddress, sizeof(collectorAddress)) < 0)
      {
        if (++numberOfTry > 3)
          break;
      }
      if (false == success)
      {
        fprintf(stderr, "[%s] During push data to collectors, connect to collector [%s] failed.\n",
            GetCurrentTime(), collectorItr->first.c_str());
        close(socketFd);
        exit(1);
      }

      int bytesSent;
      if (send(socketFd, compressedDynamicInfo.c_str(), strlen(compressedDynamicInfo.c_str()), 0) < 0)
      {
        fprintf(stderr,
            "[%s] During registration to collectors, failed to send the registration info.",
            GetCurrentTime());
        close(socketFd);
        exit(1);
      }
      close(socketFd);

    }

    ThreadSleep(monitoringRate_, 0);
  }
  return NULL;
}

/*!
 * Assemble the stable meta-data grabbed by crawler into JSON format.
 */
const std::string Monitor::_AssembleStatbleMetaDataJson()
{

}

/*!
 * Assemble the dynamic meta-data grabbed by crawlers into JSON format.
 */
const std::string Monitor::_AssembleDynamicMetaDataJson()
{

}

}
