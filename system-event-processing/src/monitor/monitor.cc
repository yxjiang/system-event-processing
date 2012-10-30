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

char Monitor::machineName_[256];
map<string, CrawlerStatus> Monitor::crawlers_;
int Monitor::monitoringRate_ = 1;
pthread_rwlock_t Monitor::stopSymbolrwlock_;
bool Monitor::fetchDataServiceStop_ = false;


pthread_rwlock_t Monitor::collectorStatusrwlock_;
int Monitor::collectorRegistrationPort_ = 32167; //  default registration port for collector
int Monitor::collectorDataPort_ = 32168; //  default data port for collector
int Monitor::communicationServicePort_ = 32100;
int Monitor::commandServiceSocketFd = -1;
bool Monitor::commandServiceStop_ = false;
bool Monitor::pushDataServiceStop_ = false;
std::map<std::string, bool> Monitor::collectorStatus_;


Monitor::Monitor(std::vector<std::string> vecCollectorIps, int rateInSecond, int communicationServicePort, int collectorRegistrationPort,
    int collectorDataPort)
{
  monitoringRate_ = rateInSecond;
  gethostname(this->machineName_, sizeof(this->machineName_));
  communicationServicePort_ = communicationServicePort;
  collectorRegistrationPort_ = collectorRegistrationPort;
  collectorDataPort_ = collectorDataPort;

  //  initialize locks
  pthread_rwlock_init(&stopSymbolrwlock_, NULL);
  pthread_rwlock_init(&collectorStatusrwlock_, NULL);

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
    delete itr->second.crawler;

  //  close socket
//  close(this->commandServiceSocketFd);

  //  destroy locks
  pthread_rwlock_destroy(&stopSymbolrwlock_);
  pthread_rwlock_destroy(&collectorStatusrwlock_);
}

void Monitor::Run()
{
  //  start all crawlers
  map<string, CrawlerStatus>::iterator itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    pthread_create(&(itr->second.pid), NULL, _CrawlerService, (void*) (itr->second.crawler));

//  //  initialize communication service
//  pthread_create(&(this->communicationServicePid_), NULL, _CommandService,
//      NULL);
//  //  register to collectors by sending profile, including stable meta-data
//  pthread_create(&(this->pushDataServicePid_), NULL, _PushDataMainThread, NULL);
//
//  //  register to collectors
//  _RegisterToCollectors();

  //  wait for all threads to stop
  itr = crawlers_.begin();
  for (; itr != crawlers_.end(); ++itr)
    if (itr->second.pid > 0)
      pthread_join(itr->second.pid, NULL);
//
//  pthread_join(this->communicationServicePid_, NULL);
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
  Crawler *crawler = (Crawler *)arg;
  while(true)
  {
    cout << "Crawler " << crawler->GetStreamType() << " fetch data." << endl;
    pthread_rwlock_wrlock(&stopSymbolrwlock_);
    stopService = fetchDataServiceStop_;
    pthread_rwlock_unlock(&stopSymbolrwlock_);
    if(true == stopService)
      break;

    crawler->FetchMetaData();
    ThreadSleep(monitoringRate_, 0);
  }
  return NULL;
}

///*!
// * Thread entry function for communication service.
// */
//void *Monitor::_CommandService(void *arg)
//{
//  struct sockaddr_in serverAddr; // Server Internet address
//  //  initialize server address
//  bzero(&serverAddr, sizeof(serverAddr));
//  serverAddr.sin_family = AF_INET;
//  serverAddr.sin_addr.s_addr = htons(INADDR_ANY);
//  serverAddr.sin_port = htons(communicationServicePort_);
//
//  commandServiceSocketFd = socket(AF_INET, SOCK_STREAM, 0);
//  if (commandServiceSocketFd < 0)
//  {
//    fprintf(stderr,
//        "[%s] Monitor communication service creates socket failed.\n",
//        GetCurrentTime().c_str());
//    exit(1);
//  }
//  else
//    printf("[%s] Monitor communication service socket created...\n",
//        GetCurrentTime().c_str());
//
//  //  bind socket and address
//  if (bind(commandServiceSocketFd, (struct sockaddr*) &serverAddr,
//      sizeof(serverAddr)))
//  {
//    fprintf(stderr,
//        "[%s] Monitor communication service bind port: %d failed.\n",
//        GetCurrentTime().c_str(), communicationServicePort_);
//    exit(1);
//  }
//  else
//    printf("[%s] Monitor communication service port binded to port %d...\n",
//        GetCurrentTime().c_str(), communicationServicePort_);
//
//  //  listen on port
//  if (listen(commandServiceSocketFd, 5))
//  {
//    fprintf(stderr, "[%s] Monitor communication service listen failed.\n",
//        GetCurrentTime().c_str());
//    exit(1);
//  }
//  else
//    printf("[%s] Monitor service listening on port %d...\n", GetCurrentTime().c_str(), communicationServicePort_);
//
//  bool stopService = false;
//  while (true)
//  {
//    pthread_rwlock_rdlock(&stopSymbolrwlock_);
//    stopService = commandServiceStop_;
//    pthread_rwlock_unlock(&stopSymbolrwlock_);
//    if (true == stopService)
//      break;
//
//    int connectionSocket = accept(commandServiceSocketFd, NULL, 0);
//    stringstream recvContent;
//    int recvBytes;
//    char buffer[1024];
//    while ((recvBytes = recv(connectionSocket, buffer, 1024, 0)) > 0)
//    {
//      if (recvBytes < 0)
//      {
//        fprintf(stderr, "[%s] Monitor receive command error.\n", GetCurrentTime().c_str());
//      }
//      recvContent << buffer;
//    }
//
//    string contentString = recvContent.str();
//
//    if (boost::starts_with(contentString, "renew"))
//      _HandleCollectorRenew(connectionSocket);
//
//    close(connectionSocket);
//  }
//
//  return NULL;
//}
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
//  string compressedDynamicInfo = _AssembleDynamicMetaDataJson();
//  bool stopService = false;
//  while (true)
//  {
//    pthread_rwlock_rdlock(&stopSymbolrwlock_);
//    stopService = pushDataServiceStop_;
//    pthread_rwlock_unlock(&stopSymbolrwlock_);
//    if (true == stopService)
//      break;
//
//    map<string, bool>::iterator collectorItr = collectorStatus_.begin();
//    for (; collectorItr != collectorStatus_.end(); ++collectorItr)
//    {
//      if (false == collectorItr->second)
//        continue;
//
//      DataPackage package;
//      package.collectorIP = collectorItr->first;
//      package.compressedContent = compressedDynamicInfo;
//
//      pthread_t workerPid;
//      pthread_create(&workerPid, NULL, _PushDataWorkerThread, (void *) &package);
//    }
//
//    ThreadSleep(monitoringRate_, 0);
//  }
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
//    fprintf(stderr,
//        "[%s] During push data to collectors, failed to create socket. Monitor terminated\n",
//        GetCurrentTime().c_str());
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
//    fprintf(stderr,
//        "[%s] During push data to collectors, no such collector with IP %s.\n",
//        GetCurrentTime().c_str(), ip.c_str());
//    close(socketFd);
//    return NULL;
//  }
//
//  bzero(&collectorAddress, sizeof(collectorAddress));
//  collectorAddress.sin_family = AF_INET;
//  bcopy((char *) collectorHostent->h_addr, (char *) &collectorAddress.sin_addr.s_addr, collectorHostent->h_length);
//  collectorAddress.sin_port = htons(collectorRegistrationPort_);
//  int numberOfTry = 0;
//  pthread_rwlock_wrlock(&collectorStatusrwlock_);
//  bool success = false;
//  pthread_rwlock_unlock(&collectorStatusrwlock_);
//  //  retry for 3 times
//  while (connect(socketFd, (struct sockaddr*) &collectorAddress, sizeof(collectorAddress)) < 0)
//  {
//    if (++numberOfTry > 3)
//      break;
//  }
//  if (false == success)
//  {
//    fprintf(stderr,
//        "[%s] During push data to collectors, connect to collector [%s] failed.\n",
//        GetCurrentTime().c_str(), ip.c_str());
//    close(socketFd);
//    return NULL;
//  }
//
//  if (send(socketFd, compressedDynamicInfo.c_str(), strlen(compressedDynamicInfo.c_str()), 0) < 0)
//  {
//    fprintf(stderr,
//        "[%s] During registration to collectors, failed to send the registration info.",
//        GetCurrentTime().c_str());
//    close(socketFd);
//    return NULL;
//  }
//  close(socketFd);
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
///*!
// * Assemble the dynamic meta-data grabbed by crawlers into JSON format.
// */
//const string Monitor::_AssembleDynamicMetaDataJson()
//{
//  ptree root;
//  map<string, CrawlerStatus>::iterator crawlerItr = crawlers_.begin();
//  //  put timestamp of the first crawler
//  ObserveData curData = crawlerItr->second.crawler->GetData();
//  root.put<long int>("timestamp", curData.timestamp);    //  set timestamp
//
//  //  iterates all the crawlers to assemble the monitoring meta-data
//  for(; crawlerItr != crawlers_.end(); ++crawlerItr)
//  {
//      string streamType = crawlerItr->second.crawler->GetStreamType();
//      ObserveData data = crawlerItr->second.crawler->GetData();
//      ptree stream_node;
//      boost::shared_ptr<map<string, string> > properties = data.properties_;
//      map<string, string>::iterator propertyItr = properties->begin();
//      for(; propertyItr != properties->end(); ++propertyItr)
//          stream_node.put<string>(propertyItr->first, propertyItr->second);
//
//      root.add_child(streamType, stream_node);
//  }
//
//  stringstream ss;
//  write_json(ss, root);
//  string strJson = ss.str();
//
//  if(strJson.size() == 0)
//  {
//      fprintf(stderr, "size = 0\n");
//  }
//
//  return strJson;
//}

}   //  end of namespace event

int main(int argc, char *argv[])
{
  using namespace std;
  using namespace event;

  int monitorRate = 1;
  int commandPort = 32100;
  int collectorRegistrationPort = 32167;
  int collectorDataPort = 32168;

  if(argc == 1 || argc < 2 || argc > 6)
  {
    printf("usage: monitor ips [monitor-rate] [command-port] [collector-reg-port] [collector-data-port]\n");
    printf("Options:");
    printf("\tips\t\tList of IPs of collectors, separated by ','\n");
    printf("\tmonitor-rate\t\tRate of monitoring in second, e.g. 3 indicates monitor the system every 3 seconds.\n");
    printf("\tcommand-port\t\tPort number of command service. Default is 32100.\n");
    printf("\tcollector-reg-port\t\tRegistration port of remote collectors. Default is 32167.\n");
    printf("\tcollector-data-port\t\tData port of remote collectors. Default is 32168.\n");
    exit(1);
  }
  vector<string> vecIPs;
  if(argc >= 2)
  {
    string ipStr(argv[1]);
    Split(ipStr, ',', vecIPs, true);
  }

  if(argc >= 3)
    monitorRate = atoi(argv[2]);

  if(argc >= 4)
    commandPort = atoi(argv[3]);

  if(argc >= 5)
    collectorRegistrationPort = atoi(argv[4]);

  if(argc == 6)
    collectorDataPort = atoi(argv[5]);

  CPUCrawler *cpuCrawler = new CPUCrawler;
  cpuCrawler->Init();
  Monitor monitor(vecIPs, monitorRate, commandPort, collectorRegistrationPort, collectorDataPort);
  monitor.Attach(cpuCrawler);
  monitor.Run();

  return 0;
}
