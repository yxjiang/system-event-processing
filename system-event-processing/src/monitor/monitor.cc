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
using namespace boost::property_tree;

string Monitor::machineUuidStr_;
char Monitor::machineName_[256];
map<string, CrawlerStatus> Monitor::crawlers_;
EventStream Monitor::stream_;
pthread_attr_t Monitor::threadAttr_;    //  thread attribute

int Monitor::monitoringRate_ = 1;
pthread_mutex_t Monitor::dataFetchedMutex_ = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t Monitor::dataFetchedCond_ = PTHREAD_COND_INITIALIZER;
bool Monitor::firstDataFetched_ = false; //  push data would not start until first batch of data is running

pthread_rwlock_t Monitor::collectorStatusrwlock_;
std::map<std::string, bool > Monitor ::collectorStatus_;    //  record status of all collectors
int Monitor::commandServicePort_ = 32101; //  default port number of communication service
int Monitor::commandServiceSocketFd_ = -1;
int Monitor::collectorCommandPort_ = 32100;    //  default collector command port











int Monitor::queryServiceSocketFd_;








Monitor::Monitor(std::vector<std::string> vecCollectorIps, int rateInSecond,
    int streamSize, int commandServicePort, int collectorCommandPort)
{

  //  initialize fetch data service
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  machineUuidStr_ = boost::lexical_cast<std::string>(uuid);
  monitoringRate_ = rateInSecond;
  //  get IP address of current machine
  char buf[128];
  gethostname(buf, sizeof(buf));
  struct hostent *hostAddr;
  struct in_addr addr;
  hostAddr = gethostbyname(buf);
  memcpy(&addr.s_addr, hostAddr->h_addr, sizeof(addr.s_addr));
  strcpy(this->machineName_, inet_ntoa(addr));
  commandServicePort_ = commandServicePort;
  collectorCommandPort_ = collectorCommandPort;

  stream_.SetStreamBufferSize(streamSize);

  //  initialize collector status
  for (size_t i = 0; i < vecCollectorIps.size(); ++i)
    collectorStatus_.insert(make_pair<string, bool>(vecCollectorIps[i], false));

  //  initialize thread attribute
  int ret = pthread_attr_init(&threadAttr_);
  if (ret != 0)
  {
    fprintf(stderr, "[%s] Initialize thread attribute failed. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }
  ret = pthread_attr_setdetachstate(&threadAttr_, PTHREAD_CREATE_DETACHED); //  make work thread resource immediately available when finished
  if (ret != 0)
  {
    fprintf(stderr, "[%s] Set thread attribute failed. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  //  register to collectors
  _RegisterToCollectors();

  //  initialize locks
  pthread_rwlock_init(&collectorStatusrwlock_, NULL);

}

Monitor::~Monitor()
{
  //  release crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    delete itr->second.crawler;

  //  release resource
  pthread_attr_destroy(&threadAttr_);
  pthread_rwlock_destroy(&collectorStatusrwlock_);
}

void Monitor::Run()
{
  //  start all crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    pthread_create(&(itr->second.pid), NULL, _CrawlerService,
        (void*) (itr->second.crawler));

  //  start to collect meta-data from crawlers and put into stream
  pthread_create(&(this->collectThreadPid_), NULL, _CollectDataFromCrawlers, NULL);

  //  initialize command service
  pthread_create(&(this->commandServicePid_), NULL, _CommandService, NULL);

  //  initialize query service
//  pthread_create(&(this->queryServicePid_), NULL, _QueryService, NULL);


//
//  //  register to collectors
//  _RegisterToCollectors();

  //  push data periodically to collectors

//  pthread_create(&(this->pushDataServicePid_), &threadAttr_, _PushDataMainThread, NULL);

  //  wait for all threads to stop
  itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    if (itr->second.pid > 0)
      pthread_join(itr->second.pid, NULL);

  pthread_join(this->collectThreadPid_, NULL);
  pthread_join(this->queryServicePid_, NULL);
  pthread_join(this->commandServicePid_, NULL);
//  pthread_join(this->pushDataServicePid_, NULL);
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
  pthread_rwlock_wrlock(&collectorStatusrwlock_);
  crawlers_.insert(make_pair<string, CrawlerStatus>(crawler->GetCrawlerName(), status));
  pthread_rwlock_unlock(&collectorStatusrwlock_);
}

void Monitor::Detach(const string &name)
{
  pthread_rwlock_wrlock(&collectorStatusrwlock_);
  crawlers_.erase(name);
  pthread_rwlock_unlock(&collectorStatusrwlock_);
}

Crawler *Monitor::GetCrawler(const std::string &name) const
{
  return crawlers_.find(name)->second.crawler;
}

/*!
 * Asks the crawler to fetch data periodically.
 */
void *Monitor::_CrawlerService(void *arg)
{
  bool stopService = true;
  Crawler *crawler = (Crawler *) arg;
  while (true)
  {
    if (true == stopService)
      break;

    crawler->FetchMetaData();
    ThreadSleep(monitoringRate_, 0);
    pthread_mutex_lock(&dataFetchedMutex_);
    firstDataFetched_ = true;
    pthread_cond_signal(&dataFetchedCond_);
    pthread_mutex_unlock(&dataFetchedMutex_);

  }
  pthread_exit(NULL);
  return NULL;
}

/*!
 * Collect the data from crawlers periodically.
 */
void *Monitor::_CollectDataFromCrawlers(void *arg)
{
  while (true)
  {
    ptree root;
    map<string, CrawlerStatus>::iterator crawlerItr = crawlers_.begin();
    //  put timestamp of the first crawler
    ObserveData curData = crawlerItr->second.crawler->GetData();
    root.put<string>("machineName", machineName_); // set sender
    root.put<long int>("timestamp", curData.timestamp); //  set timestamp
    //  iterates all the crawlers to assemble the monitoring meta-data
    for (; crawlerItr != crawlers_.end(); ++crawlerItr)
    {
      string streamType = crawlerItr->second.crawler->GetStreamType();
      ObserveData stableMetaData = crawlerItr->second.crawler->GetData();
      ptree subNode;
      map<string, string> properties = *stableMetaData.properties_.get();
      map<string, string>::iterator propertyItr = properties.begin(); //  crawler may replace the map at the same time
      for (; propertyItr != properties.end(); ++propertyItr)
        subNode.put<string>(propertyItr->first, propertyItr->second);
      root.add_child(streamType, subNode);
    }

    stream_.AddData(root);
//    fprintf(stdout, "%s\n", _AssembleDynamicMetaData());
    ThreadSleep(monitoringRate_, 0);
  }
  pthread_exit(NULL);
  return NULL;
}

/*!
 * Assemble the dynamic meta-data grabbed by crawlers into JSON format.
 */
const char *Monitor::_AssembleDynamicMetaData()
{
  boost::property_tree::ptree tree = stream_.GetLatest();

  stringstream ss;
  write_json(ss, tree);
  string strJson = ss.str();

  if (strJson.size() == 0)
  {
    fprintf(stderr, "size = 0\n");
  }

  utility::MetaData metaData;
  metaData.set_monitoruuid(machineUuidStr_);
  metaData.set_jsonstring(strJson);

//  return strJson.c_str();
  return metaData.SerializeAsString().c_str();
}

void Monitor::_RegisterToCollectors()
{
  pthread_rwlock_rdlock(&collectorStatusrwlock_);
  std::map<std::string, bool> collectorStatus = collectorStatus_;
  pthread_rwlock_unlock(&collectorStatusrwlock_);

  boost::property_tree::ptree commandJson;
  commandJson.put<string>("commandType", "registration");
  commandJson.put<string>("machineName", machineName_);
  stringstream ss;
  write_json(ss, commandJson);
  string strJson = ss.str();

  struct hostent *serverHostent;
  struct sockaddr_in serverAddr;
  bzero(&serverAddr, sizeof(serverAddr));
  serverAddr.sin_port = htons(collectorCommandPort_);
  serverAddr.sin_family = AF_INET;

  std::map<std::string, bool>::iterator collectorStatusItr = collectorStatus.begin();
  for(; collectorStatusItr != collectorStatus.end(); ++collectorStatusItr)
  {
    int connectionSocketFd = socket(AF_INET, SOCK_STREAM, 0);
    if(connectionSocketFd < 0)
    {
      fprintf(stderr, "[%s] Failed to create socket for monitor registration. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
      continue;
    }
    serverHostent = gethostbyname(collectorStatusItr->first.c_str());
    bcopy(serverHostent->h_addr, &serverAddr.sin_addr.s_addr, serverHostent->h_length);

    if(connect(connectionSocketFd, (struct sockaddr*) &serverAddr, sizeof(serverAddr)) < 0)
    {
      fprintf(stderr, "[%s] Failed to connect to collector for registration. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
      close(connectionSocketFd);
      continue;
    }

    if(send(connectionSocketFd, strJson.c_str(), strJson.size(), 0) < 0)
    {
      fprintf(stderr, "[%s] Failed to send registration data to collector. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
      close(connectionSocketFd);
      continue;
    }
    close(connectionSocketFd);
  }


}

/*!
 * Thread entry function for command service.
 */
void *Monitor::_CommandService(void *arg)
{
  struct sockaddr_in serverAddr; // Server Internet address
  //  initialize server address
  bzero(&serverAddr, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = INADDR_ANY;
  serverAddr.sin_port = htons(commandServicePort_);

  commandServiceSocketFd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (commandServiceSocketFd_ < 0)
  {
    fprintf(stderr, "[%s] Monitor command service creates socket failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  //  bind socket and address
  if (bind(commandServiceSocketFd_, (struct sockaddr*) &serverAddr, sizeof(serverAddr)) < 0)
  {
    fprintf(stderr, "[%s] Monitor command service bind port: %d failed. Reason: %s.\n", GetCurrentTime().c_str(), commandServicePort_, strerror(errno));
    close(commandServiceSocketFd_);
    exit(1);
  }

  //  listen on port
  if (listen(commandServiceSocketFd_, 50) < 0)
  {
    fprintf(stderr, "[%s] Monitor command service listen failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    close(commandServiceSocketFd_);
    exit(1);
  }
  else
    printf("[%s] Monitor service listening on port %d...\n", GetCurrentTime().c_str(), commandServicePort_);

  while (true)
  {
    int connectionSocket = accept(commandServiceSocketFd_, NULL, 0);
    stringstream recvContent;
    int recvBytes;
    char buffer[BUFFER_SIZE];
    while ((recvBytes = recv(connectionSocket, buffer, BUFFER_SIZE, 0)) > 0)
    {
      if (recvBytes < 0)
        fprintf(stderr, "[%s] Monitor receive command error.\n", GetCurrentTime().c_str());
      recvContent << buffer;
    }
    bzero(buffer, sizeof(buffer));

    string contentString = recvContent.str();
    CommandPackage *package = new CommandPackage;
    package->content = contentString;
    pthread_t workerPid;
    pthread_create(&workerPid, &threadAttr_, _CommandServiceWorker, (void *)package);

    close(connectionSocket);
  }

  return NULL;
}

void *Monitor::_CommandServiceWorker(void *arg)
{
  CommandPackage *package = (CommandPackage *)arg;
  fprintf(stdout, "[%s] Receive command %s.\n", GetCurrentTime().c_str(), package->content.c_str());

  delete package;
  pthread_exit(NULL);
  return NULL;
}
//
///*!
// * Send profile data to collectors.
// */
//void Monitor::_RegisterToCollectors()
//{
//  //  prepare the data need to be sent
//  utility::MetaData stableMetaData;
//  char machineName[256];
//  gethostname(machineName, sizeof(machineName));
//  stableMetaData.set_monitorname(machineName);
//  stableMetaData.set_jsonstring(_AssembleStatbleMetaDataJson());
//  string compressedContent = stableMetaData.SerializeAsString();
//
//  //  iterative register to all collector
//  struct hostent *collectorHostent = NULL;
//  struct sockaddr_in collectorAddress;
//  map<string, bool>::iterator collectorItr = collectorStatus_.begin();
//  for (; collectorItr != collectorStatus_.end(); ++collectorItr)
//  {
//    //  send profile to collector via socket
//    int socketFd = socket(AF_INET, SOCK_STREAM, 0);
//    if (socketFd < 0)
//    {
//      fprintf(
//          stderr,
//          "[%s] During registration to collectors, failed to create socket. Monitor terminated\n",
//          GetCurrentTime().c_str());
//      exit(1);
//    }
//
//    if (collectorHostent != NULL)
//      free(collectorHostent);
//    collectorHostent = gethostbyname(collectorItr->first.c_str());
//    if (collectorHostent == NULL)
//    {
//      fprintf(stderr,
//          "[%s] During registration to collectors, no such collector with IP %s.\n",
//          GetCurrentTime().c_str(), collectorItr->first.c_str());
//      pthread_rwlock_wrlock(&collectorStatusrwlock_);
//      collectorItr->second = false;
//      pthread_rwlock_unlock(&collectorStatusrwlock_);
//      close(socketFd);
//      continue;
//    }
//
//    bzero(&collectorAddress, sizeof(collectorAddress));
//    collectorAddress.sin_family = AF_INET;
//    bcopy((char *) collectorHostent->h_addr,
//        (char *) &collectorAddress.sin_addr.s_addr, collectorHostent->h_length);
//    collectorAddress.sin_port = htons(collectorRegistrationPort_);
//    int numberOfTry = 0;
//    bool success = false;
//    //  retry for 3 times
//    while (connect(socketFd, (struct sockaddr*) &collectorAddress, sizeof(collectorAddress)) < 0)
//    {
//      if (++numberOfTry > 3)
//        break;
//    }
//    if (false == success)
//    {
//      fprintf(stderr,
//          "[%s] During registration to collectors, connect to collector [%s] failed.\n",
//          GetCurrentTime().c_str(), collectorItr->first.c_str());
//      close(socketFd);
//      exit(1);
//    }
//
//    if (send(socketFd, compressedContent.c_str(), strlen(compressedContent.c_str()), 0) < 0)
//    {
//      fprintf(stderr,
//          "[%s] During registration to collectors, failed to send the registration info.",
//          GetCurrentTime().c_str());
//      close(socketFd);
//      exit(1);
//    }
//    close(socketFd);
//    pthread_rwlock_wrlock(&collectorStatusrwlock_);
//    collectorItr->second = true;
//    pthread_rwlock_unlock(&collectorStatusrwlock_);
//  }
//
//}
//
///*!
// * Handle renew event. Update crawlers information accordingly.
// * Resume to send meta-data to the recovered collector.
// */
//void Monitor::_HandleCollectorRenew(int connectionSocket)
//{
//
//}
//
///*!
// * Push data to collectors. The data is compressed by protocol buffer.
// */
//void *Monitor::_PushDataMainThread(void *arg)
//{
//  bool stopService = false;
//
//  pthread_mutex_lock(&dataFetchedMutex_);
//  while (false == firstDataFetched_)
//    pthread_cond_wait(&dataFetchedCond_, &dataFetchedMutex_);
//  pthread_mutex_unlock(&dataFetchedMutex_);
//
//  while (true)
//  {
//    string compressedDynamicInfo = _AssembleDynamicMetaData();
//    cout << compressedDynamicInfo.c_str() << endl;
//    stopService = pushDataServiceStop_;
//    if (true == stopService)
//      break;
//
////    cout << compressedDynamicInfo << endl;
//
//    map<string, bool>::iterator collectorItr = collectorStatus_.begin();
//    for (; collectorItr != collectorStatus_.end(); ++collectorItr)
//    {
////      if (false == collectorItr->second)
////        continue;
//
//      DataPackage package;
//      package.collectorIP = collectorItr->first;
//      package.compressedContent = compressedDynamicInfo;
//
//      pthread_t workerPid;
//      pthread_create(&workerPid, &threadAttr_, _PushDataWorkerThread,
//          (void *) &package);
////      fprintf(stdout, "send %s to %s\n", package.compressedContent.c_str(), collectorItr->first.c_str());
//    }
//
//    ThreadSleep(monitoringRate_, 0);
//  }
//  pthread_exit(NULL);
//  return NULL;
//}
//
///*!
// * The worker thread to push data to a specified collector.
// */
//void *Monitor::_PushDataWorkerThread(void *arg)
//{
//  DataPackage *package = (DataPackage*) arg;
//  string ip = package->collectorIP;
//  string compressedDynamicInfo = package->compressedContent;
//
//  //  send compressed data to collector
//  struct hostent *collectorHostent = NULL;
//  struct sockaddr_in collectorAddress;
//  int socketFd = socket(AF_INET, SOCK_STREAM, 0);
//  if (socketFd < 0)
//  {
//    fprintf(
//        stderr,
//        "[%s] During push data to collectors, failed to create socket. Monitor terminated. Reason: %s.\n",
//        GetCurrentTime().c_str(), strerror(errno));
//    return NULL;
//  }
//
//  if (collectorHostent != NULL
//    )
//    free(collectorHostent);
//
//  collectorHostent = gethostbyname(ip.c_str());
//  if (collectorHostent == NULL)
//  {
//    fprintf(
//        stderr,
//        "[%s] During push data to collectors, no such collector with IP %s. Reason: %s.\n",
//        GetCurrentTime().c_str(), ip.c_str(), strerror(errno));
//    close(socketFd);
//    return NULL;
//  }
//
//  bzero(&collectorAddress, sizeof(collectorAddress));
//  collectorAddress.sin_family = AF_INET;
//  bcopy((char *) collectorHostent->h_addr,
//      (char *) &collectorAddress.sin_addr.s_addr, collectorHostent->h_length);
//  collectorAddress.sin_port = htons(collectorDataPort_);
//  int numberOfTry = 0;
//  pthread_rwlock_wrlock(&collectorStatusrwlock_);
//  pthread_rwlock_unlock(&collectorStatusrwlock_);
//  //    retry for 3 times
//  while (connect(socketFd, (struct sockaddr*) &collectorAddress,
//      sizeof(collectorAddress)) < 0)
//  {
//    if (++numberOfTry > 3)
//    {
//      fprintf(stderr,
//          "[%s] During push data to collectors, connect to collector [%s] failed. Reason: %s.\n",
//          GetCurrentTime().c_str(), ip.c_str(), strerror(errno));
//      close(socketFd);
//      return NULL;
//    }
//  }
//
//  if (send(socketFd, compressedDynamicInfo.c_str(),
//      strlen(compressedDynamicInfo.c_str()), 0) < 0)
//  {
//    fprintf(
//        stderr,
//        "[%s] During registration to collectors, failed to send the registration info. Reason: %s.\n",
//        GetCurrentTime().c_str(), strerror(errno));
//    close(socketFd);
//    return NULL;
//  }
//  close(socketFd);
//  pthread_exit(NULL);
//  return NULL;
//}
//
///*!
// * Assemble the stable meta-data grabbed by crawler into JSON format.
// */
//const string Monitor::_AssembleStatbleMetaDataJson()
//{
//  ptree root;
//  map<string, CrawlerStatus>::iterator crawlerItr = crawlers_.begin();
//  root.put("machine-name", machineName_);
//
//  //  iterates all the crawlers to assemble the monitoring meta-data
//  for(; crawlerItr != crawlers_.end(); ++crawlerItr)
//  {
//      string streamType = crawlerItr->second.crawler->GetStreamType();
//      map<string, string> stableMetaData = crawlerItr->second.crawler->GetStableData();
//      ptree subNode;
//      map<string, string> properties = stableMetaData;
//      map<string, string>::iterator propertyItr = properties.begin(); //  crawler may replace the map at the same time
//      for(; propertyItr != properties.end(); ++propertyItr)
//          subNode.put<string>(propertyItr->first, propertyItr->second);
//
//      root.add_child(streamType, subNode);
//  }
//
//  stringstream ss;
//  write_json(ss, root);
//  string strJson = ss.str();
//
//  if(strJson.size() == 0)
//    fprintf(stderr, "size = 0\n");
//
//  return strJson;
//}
//


} //  end of namespace event

int main(int argc, char *argv[])
{
  using namespace std;
  using namespace event;

  int monitorRate = 1;
  int streamSize = 60;
  int commandPort = 32101;
  int collectorCommandPort = 32100;

  if (argc == 1 || argc < 2 || argc > 6)
  {
    printf(
        "\nusage: monitor ips [monitor-rate] [stream-size] [command-port] [collector-cmd-port]\n");
    printf("Options:\n");
    printf("\tips\t\t\t\tList of IPs of collectors, separated by ','.\n");
    printf(
        "\tmonitor-rate\t\t\tRate of monitoring in second, e.g. 3 indicates monitor the system every 3 seconds.\n");
    printf(
        "\tstream-size\t\t\tSize of stream, e.g. 60 indicates store the latest 60 records.\n");
    printf(
        "\tcommand-port\t\t\tPort number of command service. Default is 32100.\n");
    printf(
        "\tcollector-cmd-port\t\tCommand port of remote collectors. Default is 32100.\n");
    exit(1);
  }
  vector<string> vecIPs;
  if (argc >= 2)
  {
    string ipStr(argv[1]);
    Split(ipStr, ',', vecIPs, true);
  }

  if (argc >= 3)
  {
    monitorRate = atoi(argv[2]);
    if(monitorRate <= 0)
      monitorRate = 1;
  }

  if(argc >= 4)
  {
    streamSize = atoi(argv[3]);
    if(streamSize < 10)
      streamSize = 10;
  }

  if (argc >= 5)
    commandPort = atoi(argv[3]);

  if (argc >= 6)
    collectorCommandPort = atoi(argv[4]);

  CPUCrawler *cpuCrawler = new CPUCrawler;
  cpuCrawler->Init();
  DummyCrawler *dummyCrawler = new DummyCrawler;
  dummyCrawler->Init();
  Monitor monitor(vecIPs, monitorRate, streamSize, commandPort, collectorCommandPort);
//  monitor.Attach(dummyCrawler);
  monitor.Attach(cpuCrawler);
  monitor.Run();

  return 0;
}
