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

int Monitor::monitoringRate_ = 1;
bool Monitor::fetchDataServiceStop_ = false; //  push data service is running by default
pthread_mutex_t Monitor::dataFetchedMutex_ = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t Monitor::dataFetchedCond_ = PTHREAD_COND_INITIALIZER;
bool Monitor::firstDataFetched_ = false; //  push data would not start until first batch of data is running

int Monitor::communicationServicePort_ = 32100; //  default port number of communication service
int Monitor::commandServiceSocketFd = -1;
bool Monitor::commandServiceStop_ = false;

int Monitor::collectorDataPort_ = 32168;//  default data port for collector
std::map<std::string, bool > Monitor ::collectorStatus_;    //  record status of all collectors
pthread_attr_t Monitor::threadAttr_;    //  thread attribute

//bool Monitor::pushDataServiceStop_ = false;//  push data is running by default


pthread_rwlock_t Monitor::collectorStatusrwlock_;
int Monitor::collectorRegistrationPort_ = 32167; //  default registration port for collector



Monitor::Monitor(std::vector<std::string> vecCollectorIps, int rateInSecond,
    int streamSize, int communicationServicePort, int collectorRegistrationPort,
    int collectorDataPort)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  machineUuidStr_ = boost::lexical_cast<std::string>(uuid);
  monitoringRate_ = rateInSecond;
  gethostname(this->machineName_, sizeof(this->machineName_));
  communicationServicePort_ = communicationServicePort;
  collectorRegistrationPort_ = collectorRegistrationPort;
  collectorDataPort_ = collectorDataPort;

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

  //  initialize locks
  pthread_rwlock_init(&collectorStatusrwlock_, NULL);

}

Monitor::~Monitor()
{
//  pthread_rwlock_wrlock(&stopSymbolrwlock_);
  this->fetchDataServiceStop_ = true;
  this->commandServiceStop_ = true;
//  this->pushDataServiceStop_ = true;
//  pthread_rwlock_unlock(&stopSymbolrwlock_);

  //  release crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    delete itr->second.crawler;

  //  close socket
  close(this->commandServiceSocketFd);

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
  pthread_create(&(this->collectThreadPid_), NULL, _CollectDataFromCrawlers,
      NULL);

  //  initialize communication service
  pthread_create(&(this->communicationServicePid_), NULL, _CommandService, NULL);

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

  pthread_join(this->communicationServicePid_, NULL);
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
    stopService = fetchDataServiceStop_;
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
 * Thread entry function for communication service.
 */
void *Monitor::_CommandService(void *arg)
{
  struct sockaddr_in serverAddr; // Server Internet address
  //  initialize server address
  bzero(&serverAddr, sizeof(serverAddr));
  serverAddr.sin_family = AF_INET;
  serverAddr.sin_addr.s_addr = htons(INADDR_ANY);
  serverAddr.sin_port = htons(MULTICAST_PORT);
  struct ip_mreq multicastRequest;  //  multicast request
  u_int yes = 1;

  commandServiceSocketFd = socket(AF_INET, SOCK_DGRAM, 0);
  if (commandServiceSocketFd < 0)
  {
    fprintf(stderr, "[%s] Monitor communication service creates socket failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  else
    printf("[%s] Monitor communication service socket created...\n", GetCurrentTime().c_str());

  //  allow multiple sockets to use the same PORT number
//  if(setsockopt(commandServiceSocketFd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
//  {
//    fprintf(stderr, "[%s] Monitor communication service set multicast failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
//    exit(1);
//  }

  //  bind socket and address
  if (bind(commandServiceSocketFd, (struct sockaddr*) &serverAddr, sizeof(serverAddr)))
  {
    fprintf(stderr, "[%s] Monitor communication service bind port: %d failed.\n", GetCurrentTime().c_str(), communicationServicePort_);
    exit(1);
  }
  else
    printf("[%s] Monitor communication service port binded to port %d...\n", GetCurrentTime().c_str(), communicationServicePort_);

  multicastRequest.imr_multiaddr.s_addr = inet_addr(MULTICAST_GROUP);
  multicastRequest.imr_interface.s_addr = htons(INADDR_ANY);
  if(setsockopt(commandServiceSocketFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &multicastRequest, sizeof(multicastRequest)) < 0)
  {
    fprintf(stderr, "[%s] Monitor communication service set multicast failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  int nbytes
  if(())



  //  listen on port
  if (listen(commandServiceSocketFd, 5))
  {
    fprintf(stderr, "[%s] Monitor communication service listen failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  else
    printf("[%s] Monitor service listening on port %d...\n", GetCurrentTime().c_str(), communicationServicePort_);

  while (true)
  {
    if (true == commandServiceStop_)
      break;

    int connectionSocket = accept(commandServiceSocketFd, NULL, 0);
    stringstream recvContent;
    int recvBytes;
    char buffer[1024];
    while ((recvBytes = recv(connectionSocket, buffer, 1024, 0)) > 0)
    {
      if (recvBytes < 0)
        fprintf(stderr, "[%s] Monitor receive command error.\n", GetCurrentTime().c_str());
      recvContent << buffer;
    }

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

} //  end of namespace event

int main(int argc, char *argv[])
{
  using namespace std;
  using namespace event;

  int monitorRate = 1;
  int commandPort = 32100;
  int collectorRegistrationPort = 32167;
  int collectorDataPort = 32168;

  if (argc == 1 || argc < 2 || argc > 6)
  {
    printf(
        "\nusage: monitor ips [monitor-rate] [command-port] [collector-reg-port] [collector-data-port]\n");
    printf("Options:\n");
    printf("\tips\t\t\t\tList of IPs of collectors, separated by ','.\n");
    printf(
        "\tmonitor-rate\t\t\tRate of monitoring in second, e.g. 3 indicates monitor the system every 3 seconds.\n");
    printf(
        "\tcommand-port\t\t\tPort number of command service. Default is 32100.\n");
    printf(
        "\tcollector-reg-port\t\tRegistration port of remote collectors. Default is 32167.\n");
    printf(
        "\tcollector-data-port\t\tData port of remote collectors. Default is 32168.\n");
    exit(1);
  }
  vector<string> vecIPs;
  if (argc >= 2)
  {
    string ipStr(argv[1]);
    Split(ipStr, ',', vecIPs, true);
  }

  if (argc >= 3)
    monitorRate = atoi(argv[2]);

  if (argc >= 4)
    commandPort = atoi(argv[3]);

  if (argc >= 5)
    collectorRegistrationPort = atoi(argv[4]);

  if (argc == 6)
    collectorDataPort = atoi(argv[5]);

  CPUCrawler *cpuCrawler = new CPUCrawler;
  cpuCrawler->Init();
  DummyCrawler *dummyCrawler = new DummyCrawler;
  dummyCrawler->Init();
  Monitor monitor(vecIPs, monitorRate, commandPort, collectorRegistrationPort,
      collectorDataPort);
//  monitor.Attach(dummyCrawler);
  monitor.Attach(cpuCrawler);
  monitor.Run();

  return 0;
}
