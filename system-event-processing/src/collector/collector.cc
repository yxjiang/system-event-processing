/*
 * collector.cc
 *
 *  Created on: Oct 31, 2012
 *      Author: yexijiang
 */

#include "collector.h"

namespace event
{
using namespace std;

char Collector::machineIP_[256];
std::map<string, MonitorProfile> Collector::monitorProfile_;
pthread_rwlock_t Collector::monitorProfileRwLock_;
pthread_attr_t Collector::threadAttr_;
pthread_t Collector::commandServicePid_;
int Collector::commandServicePort_ = 32100;

int Collector::monitorCommandServicePort_ = 32101; //  default port for monitor to receive commands
pthread_t Collector::subscribeExecutorPid_;
map<string, QueryProfile*> Collector::registeredQueryProfiles_;
pthread_rwlock_t Collector::registeredQueryProfileRwlock_;

pthread_t Collector::multicastCommandListenerPid_;

//int Collector::dataServicePort_ = 32168;    //  default port number to receive data
//bool Collector::dataServiceStop_ = false;    //  data service is running by default

Collector::Collector(int communicationPort,
    int monitorCommunicationPort)
{
  monitorCommandServicePort_ = monitorCommunicationPort;

  //  get IP address of current machine
  char buf[128];
  gethostname(buf, sizeof(buf));
  struct hostent *hostAddr;
  struct in_addr addr;
  hostAddr = gethostbyname(buf);
  memcpy(&addr.s_addr, hostAddr->h_addr, sizeof(addr.s_addr));
  strcpy(machineIP_, inet_ntoa(addr));

  //  suppress the protobuf logger
//  google::protobuf::SetLogHandler(NULL);
  int ret = pthread_attr_init(&threadAttr_);
  if (ret != 0)
  {
    fprintf(stderr, "[%s] Initialize thread attribute failed. Reason: %s.\n",
        GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }
  ret = pthread_attr_setdetachstate(&threadAttr_, PTHREAD_CREATE_DETACHED);
  if (ret != 0)
  {
    fprintf(stderr, "[%s] Set thread attribute failed. Reason: %s.\n",
        GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  //  initialize lock related things
  ret = pthread_rwlockattr_init(&this->wrLockAttr);
  if(ret < 0)
  {
    fprintf(stderr, "[%s] Initialize read write lock attribute failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  ret = pthread_rwlockattr_setkind_np(&this->wrLockAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  if(ret < 0)
  {
    fprintf(stderr, "[%s] Set read write lock attribute failed.\n", GetCurrentTime().c_str());
    exit(1);
  }

  pthread_rwlock_init(&monitorProfileRwLock_, &this->wrLockAttr);
  pthread_rwlock_init(&registeredQueryProfileRwlock_, &this->wrLockAttr);

}

Collector::~Collector()
{
}

void Collector::Run()
{
  //  start command service to receive commands
  pthread_create(&commandServicePid_, NULL, _CommandService, NULL);

  pthread_create(&multicastCommandListenerPid_, NULL, _MulticastCommandListener, NULL);

  _JoinIn();

//  pthread_t dataServicePid;
//  pthread_create(&dataServicePid, &threadAttr, _DataReceiveService, NULL);

//  pthread_join(dataServicePid, NULL);
  pthread_join(commandServicePid_, NULL);
  pthread_join(subscribeExecutorPid_, NULL);
  pthread_join(multicastCommandListenerPid_, NULL);
}

void *Collector::_CommandService(void *arg)
{
  struct sockaddr_in commandServiceAddr;
  bzero(&commandServiceAddr, sizeof(sockaddr_in));
  commandServiceAddr.sin_family = AF_INET;
  commandServiceAddr.sin_port = htons(commandServicePort_);
  commandServiceAddr.sin_addr.s_addr = INADDR_ANY;

  int commandServerSocketFd = socket(AF_INET, SOCK_STREAM, 0);
  if (commandServerSocketFd < 0)
  {
    fprintf(stderr, "[%s] Collector command service creates socket failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  //  bind socket and address
  if (bind(commandServerSocketFd, (struct sockaddr*) &commandServiceAddr, sizeof(commandServiceAddr)))
  {
    fprintf(stderr, "[%s] Collector command service bind port: %d failed. Reason: %s.\n", GetCurrentTime().c_str(), commandServicePort_, strerror(errno));
    close(commandServerSocketFd);
    exit(1);
  }

  //  listen
  if (listen(commandServerSocketFd, 500))
  {
    fprintf(stderr, "[%s] Collector command service listen failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    close(commandServerSocketFd);
    exit(1);
  }
  else
    fprintf(stdout, "[%s] Collector command service listening on port %d...\n", GetCurrentTime().c_str(), commandServicePort_);

  while (true)
  {
    int connectionSocketFd = accept(commandServerSocketFd, NULL, 0);
    if (connectionSocketFd < 0)
    {
      fprintf(stderr, "[%s] Received error request. Reason: %s.\n",
          GetCurrentTime().c_str(), strerror(errno));
      continue;
    }

    //  create worker to receive data
    pthread_t dataReceiveWorkerPid;
    pthread_create(&dataReceiveWorkerPid, &threadAttr_, _CommandServiceWorker, (void *) &connectionSocketFd);
  }

  close(commandServerSocketFd);
  pthread_exit(NULL);
  return NULL;
}

void *Collector::_CommandServiceWorker(void *arg)
{
  int *connectionSocketFd = (int *)arg;
  char contentBuffer[BUFFER_SIZE];
  int nbytes = 0;
  if((nbytes = recv(*connectionSocketFd, contentBuffer, sizeof(contentBuffer), 0)) < 0)
  {
    fprintf(stderr, "[%s] Receive command failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    close(*connectionSocketFd);
    pthread_exit(NULL);
    return NULL;
  }

  stringstream ss;
  ss << contentBuffer;
  boost::property_tree::ptree commandTree;
  read_json(ss, commandTree);
  const char *commandType = commandTree.get<string>("commandType").c_str();


  //  registration command
  if(0 == strcmp(commandType, "registration"))
  {
    const char *machineName = commandTree.get<string>("machineName").c_str();
    string machineNameStr(machineName);
    MonitorProfile newMonitorProfile;
    newMonitorProfile.machineIP = machineNameStr;
    time_t curTime;
    time(&curTime);
    newMonitorProfile.communicationFailCount = 0;
    fprintf(stdout, "[%s] New registration request from (%s) come.\n", GetCurrentTime().c_str(), newMonitorProfile.machineIP.c_str());
    pthread_rwlock_wrlock(&monitorProfileRwLock_);
    if(monitorProfile_.find(machineNameStr) != monitorProfile_.end())
    {
      fprintf(stdout, "[%s] Monitor (%s) reconnected.\n", GetCurrentTime().c_str(), newMonitorProfile.machineIP.c_str());
    }
    else
    {
      monitorProfile_.insert(pair<string, MonitorProfile>(machineNameStr, newMonitorProfile));
    }

    size_t curMonitorSize = monitorProfile_.size();
    pthread_rwlock_unlock(&monitorProfileRwLock_);
    fprintf(stdout, "[%s] Register new monitor with name [%s], now size is %lu.\n",
        GetCurrentTime().c_str(), newMonitorProfile.machineIP.c_str(), curMonitorSize);
  }
  else if(0 == strcmp(commandType, "dynamic"))
  {

  }

  pthread_exit(NULL);
  return NULL;
}

void *Collector::_MulticastCommandListener(void *arg)
{
  struct sockaddr_in multicastAddress;
  multicastAddress.sin_addr.s_addr = htons(INADDR_ANY);
  multicastAddress.sin_family = AF_INET;
  multicastAddress.sin_port = htons(MULTI_PORT);

  int mutlicastSocketFd;
  if ((mutlicastSocketFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
  {
    fprintf(stderr, "[%s] Error when creating socket for multicast command listener. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  if (bind(mutlicastSocketFd, (struct sockaddr *) &multicastAddress, sizeof(multicastAddress)) < 0)
  {
    fprintf(stderr, "[%s] Error when binding multicast command listener. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  struct ip_mreq mreq;
  mreq.imr_multiaddr.s_addr = inet_addr(MULTI_ADDR);
  mreq.imr_interface.s_addr = htonl(INADDR_ANY);
  if (setsockopt(mutlicastSocketFd, IPPROTO_IP, IP_ADD_MEMBERSHIP, &mreq, sizeof(mreq)) < 0)
  {
    fprintf(stderr, "[%s] Error setsockopt. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }
  else
  {
    fprintf(stdout, "[%s] Multicast command listener listen on port %d.\n", GetCurrentTime().c_str(), MULTI_PORT);
  }

  int nbytes;
  char buf[1024];
  int addrlen = sizeof(multicastAddress);
  bzero(buf, strlen(buf));
  while (true)
  {
    if ((nbytes = recvfrom(mutlicastSocketFd, buf, 1024, 0, (struct sockaddr *) &multicastAddress, (socklen_t *) &addrlen)) < 0)
    {
      fprintf(stderr, "[%s] Receive multicast command error. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    }
    else
    {
      fprintf(stdout, "[%s] [Multicast Command Listener]: Receive command [%s]\n", GetCurrentTime().c_str(), buf);
      vector<string> vecStr;
      Split(buf, ' ', vecStr, true);
      if(vecStr[0].compare("registration-request") == 0)
      {
        _HandleMonitorRegistration(vecStr[1]);
      }
    }
    bzero(buf, strlen(buf));
  }

  pthread_exit(NULL);
  return NULL;
}

void Collector::_HandleMonitorRegistration(const string &monitorIP)
{
  //  at this version, collector receive all monitors' requests

  MonitorProfile monitorProfile;
  monitorProfile.machineIP = monitorIP;
  monitorProfile.communicationFailCount = 0;

  //  send registration offer to monitor
  struct sockaddr_in monitorAddr;
  bzero(&monitorAddr, sizeof(monitorAddr));
  monitorAddr.sin_family = AF_INET;
  monitorAddr.sin_port = htons(monitorCommandServicePort_);

  struct hostent *hostent;
  hostent = gethostbyname(monitorIP.c_str());
  bcopy(hostent->h_addr, &monitorAddr.sin_addr.s_addr, hostent->h_length);

  char message[1024] = "registration-offer ";
  strcat(message, machineIP_);
  int retry = 0;

  while (retry++ < 3)
  {
    int registrationOfferSocketFd = socket(AF_INET, SOCK_STREAM, 0);
    if (registrationOfferSocketFd < 0)
    {
      fprintf(stderr, "[%s] Create socket for sending registration offer failed. Reason: %s.\n",
          GetCurrentTime().c_str(), strerror(errno));
      continue;
    }
    if (connect(registrationOfferSocketFd, (struct sockaddr *) &monitorAddr, sizeof(monitorAddr)) < 0)
    {
      fprintf(stderr, "[%s] Cannot connect to monitor %s:%d. Reason: %s\n", GetCurrentTime().c_str(), monitorIP.c_str(), monitorCommandServicePort_,
          strerror(errno));
      close(registrationOfferSocketFd);
      continue;
    }

    size_t nBytesSent = send(registrationOfferSocketFd, message, strlen(message), 0);
    if (nBytesSent != strlen(message))
    {
      fprintf(stderr, "[%s] Send failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
      close(registrationOfferSocketFd);
      continue;
    }
    else
    {
      fprintf(stdout, "[%s] Send registration offer to %s:%d.\n", GetCurrentTime().c_str(), monitorIP.c_str(), monitorCommandServicePort_);
      close(registrationOfferSocketFd);
      return;
    }
  }

}

void Collector::RegisterQuery(const string &queryContent, int queryInterval)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  QueryProfile *profile = new QueryProfile;
  stringstream ss;
  ss << uuid;
  profile->uuid = ss.str();
  profile->lastCalled = -1;
  int contentLength = queryContent.size();
  profile->queryContent = new char[contentLength];
  strcpy(profile->queryContent, queryContent.c_str());
  profile->queryInterval = queryInterval;
  pthread_rwlock_wrlock(&registeredQueryProfileRwlock_);
  registeredQueryProfiles_.insert(make_pair<string, QueryProfile*>(profile->uuid, profile));
  pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
  fprintf(stdout, "[%s] New query registered: %s.\n", GetCurrentTime().c_str(), profile->queryContent);
}

void Collector::_JoinIn()
{
  struct sockaddr_in senderAddr;
  int joinInSocketFd;

  senderAddr.sin_family = AF_INET;
  senderAddr.sin_addr.s_addr = inet_addr(MULTI_ADDR);
  senderAddr.sin_port = htons(MULTI_PORT);

  if((joinInSocketFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
  {
    fprintf(stderr, "[%s] Create join in socket failed. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    pthread_exit(NULL);
    exit(1);
  }

  stringstream ss;
  ss << "collector-join-in " << machineIP_ << "\n";
  const char *message = ss.str().c_str();

  int retry = 0;
  while(retry++ < 3)
  {
    if(sendto(joinInSocketFd, message, sizeof(message), 0, (struct sockaddr *)&senderAddr, sizeof(senderAddr)) < 0)
    {
      fprintf(stderr, "[%s] Send join in information failed. Reason: %s\n", GetCurrentTime().c_str(), strerror(errno));
    }
    else
    {
      close(joinInSocketFd);
      return;
    }
  }
  close(joinInSocketFd);
}

//void *Collector::_SubscribeExecutor(void *arg)
//{
//
//  stringstream queryStringStream;
//  while (true)
//  {
//    queryStringStream.str("");
//    boost::property_tree::ptree commandJsonTree;
//    commandJsonTree.put("commandType", "query");
//    boost::property_tree::ptree queryJsonTree;  //  put all the queries into a property tree
//    map<string, QueryProfile*>::iterator profileItr = registeredQueryProfiles_.begin();
//    pthread_rwlock_rdlock(&monitorProfileRwLock_);
//    fprintf(stdout, "[%s] There are %lu monitor registered.\n", GetCurrentTime().c_str(), monitorProfile_.size());
//    pthread_rwlock_unlock(&monitorProfileRwLock_);
//    int wakedQueryCount = 0;
//    time_t curTime;
//    time(&curTime);
//    for (; profileItr != registeredQueryProfiles_.end(); ++profileItr)
//    {
//      QueryProfile *profile = profileItr->second;
//      if (profile->lastCalled == -1 || (curTime - profile->lastCalled >= profile->queryInterval)) //  time is up
//      {
//        boost::property_tree::ptree separateQueryJsonTree;  //  put each query into a property tree
//        ++wakedQueryCount;
//        pthread_rwlock_wrlock(&registeredQueryProfileRwlock_);
//        profile->lastCalled = curTime;
//        separateQueryJsonTree.put<string>("query-uuid", profile->uuid);
//        separateQueryJsonTree.put<char*>("query-content", profile->queryContent);
//        pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
//        queryJsonTree.push_back(make_pair("", separateQueryJsonTree));
//      }
//    }
////    fprintf(stdout, "[%s] There are %d queries waked.\n", GetCurrentTime().c_str(), wakedQueryCount);
////    if(wakedQueryCount == 0)
////    {
////      for(profileItr = registeredQueryProfiles_.begin(); profileItr != registeredQueryProfiles_.end(); ++profileItr)
////      {
////        fprintf(stdout, "\t[%s] curTime: %lu, lastCalled: %lu, distinct: %lu\n", GetCurrentTime().c_str(),
////            curTime, profileItr->second->lastCalled, (curTime - profileItr->second->lastCalled));
////      }
////    }
//    commandJsonTree.push_back(make_pair("queries", queryJsonTree));
//    write_json(queryStringStream, commandJsonTree);
//    if(wakedQueryCount > 0)
//    {
//      char queryStr[65536];
//      strcpy(queryStr, queryStringStream.str().c_str());
//
//      pthread_t workerPid;
//      pthread_create(&workerPid, &threadAttr_, _SubscribeExecutorWorker, (void *) queryStr);
//    }
//    ThreadSleep(1, 0);
//  }
//
//  pthread_exit(NULL);
//  return NULL;
//}

//void *Collector::_SubscribeExecutorWorker(void *arg)
//{
//
//  const char *queryStr = (const char *)arg;
//  struct sockaddr_in monitorAddr;
//  bzero(&monitorAddr, sizeof(monitorAddr));
//  monitorAddr.sin_family = AF_INET;
//  monitorAddr.sin_port = htons(monitorCommandServicePort_);
//
////  vector<MonitorProfile*>::iterator monitorItr = monitorProfile_.begin();
//  size_t monitorSize = monitorProfile_.size();
//  for(size_t i = 0; i < monitorSize; ++i)
//  {
//    MonitorProfile &monitorProfile = monitorProfile_[i];
//    struct hostent *hostent;
//    hostent = gethostbyname(monitorProfile.machineIP.c_str());
//    bcopy(hostent->h_addr, &monitorAddr.sin_addr.s_addr, hostent->h_length);
//    int monitorSocketFd = socket(AF_INET, SOCK_STREAM, 0);
//    if(monitorSocketFd < 0)
//    {
//      fprintf(stderr, "[%s] Create socket for sending query failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
//      continue;
//    }
//    if(connect(monitorSocketFd, (struct sockaddr *)&monitorAddr, sizeof(monitorAddr)) < 0)
//    {
//
//      pthread_rwlock_wrlock(&monitorProfileRwLock_);
//      fprintf(stderr, "[%s] Cannot connect to monitor %s, failed %d times.\n", GetCurrentTime().c_str(), monitorProfile.machineIP.c_str(), ++monitorProfile.communicationFailCount);
////      //  remove monitor that cannot be connected for 3 times
////      if(monitorProfile.communicationFailCount >= 3)
////      {
////        fprintf(stderr, "[%s] Remove monitor %s from list.\n", GetCurrentTime().c_str(), monitorProfile.machineIP.c_str());
////        if(monitorProfile_.size() == monitorSize)   //  other thread update the monitor profiles
////        {
////          printf("erase\n");
////          monitorProfile_.erase(monitorProfile_.begin() + i);
////          --monitorSize;
////          --i;
////        }
////      }
//      close(monitorSocketFd);
//      pthread_rwlock_unlock(&monitorProfileRwLock_);
//      continue;
//    }
//
//    size_t nBytesSent = send(monitorSocketFd, queryStr, strlen(queryStr), 0);
//    if(nBytesSent != strlen(queryStr))
//    {
//      fprintf(stderr, "[%s] Send failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
//      close(monitorSocketFd);
//      continue;
//    }
//    else{
//      //  successfully send query to monitor
//      pthread_rwlock_wrlock(&monitorProfileRwLock_);
//      monitorProfile.communicationFailCount = 0;
//      pthread_rwlock_unlock(&monitorProfileRwLock_);
//    }
//
//    close(monitorSocketFd);
//  }
//
//  pthread_exit(NULL);
//  return NULL;
//}

//void *Collector::_DataReceiveService(void *arg)
//{
//  struct sockaddr_in dataRecieveServiceAddr; // Server Internet address
//  //  initialize server address
//  bzero(&dataRecieveServiceAddr, sizeof(dataRecieveServiceAddr));
//  dataRecieveServiceAddr.sin_family = AF_INET;
//  dataRecieveServiceAddr.sin_addr.s_addr = htons(INADDR_ANY);
//  dataRecieveServiceAddr.sin_port = htons(dataServicePort_);
//
//  int dataReceiveServerSocketFd = socket(AF_INET, SOCK_STREAM, 0);
//  if (dataReceiveServerSocketFd < 0)
//  {
//    fprintf(stderr, "[%s] Collector data receive service creates socket failed. Reason: %s.\n",
//        GetCurrentTime().c_str(), strerror(errno));
//    exit(1);
//  }
//  else
//    fprintf(stdout, "[%s] Collector data receive service socket created...\n", GetCurrentTime().c_str());
//
//  //  bind socket and address
//  if (bind(dataReceiveServerSocketFd, (struct sockaddr*) &dataRecieveServiceAddr, sizeof(dataRecieveServiceAddr)))
//  {
//    fprintf(stderr, "[%s] Collector data receive service bind port: %d failed. Reason: %s.\n",
//        GetCurrentTime().c_str(), dataServicePort_, strerror(errno));
//    close(dataReceiveServerSocketFd);
//    exit(1);
//  }
//  else
//    fprintf(stdout, "[%s] Collector data receive service port binded to port %d...\n", GetCurrentTime().c_str(), dataServicePort_);
//
//  //  listen
//  if (listen(dataReceiveServerSocketFd, 500))
//  {
//    fprintf(stderr, "[%s] Collector data receive service listen failed. Reason: %s.\n",
//        GetCurrentTime().c_str(), strerror(errno));
//    close(dataReceiveServerSocketFd);
//    exit(1);
//  }
//  else
//    fprintf(stdout, "[%s] Collector data receive service listening on port %d...\n", GetCurrentTime().c_str(), dataServicePort_);
//
//  int count = 0;
//  while (true)
//  {
//    if(++count % 100 == 0)
//      fprintf(stdout, "[%s] Received %d requests.\n", GetCurrentTime().c_str(), count);
//    int connectionSockedFd = accept(dataReceiveServerSocketFd, NULL, 0);
//    if(connectionSockedFd < 0)
//    {
//      fprintf(stderr, "[%s] Received error request. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
//      continue;
//    }
//
//    //  create worker to receive data
//    pthread_t dataReceiveWorkerPid;
//    pthread_create(&dataReceiveWorkerPid, &threadAttr, _DataReceiveWorker, (void *)&connectionSockedFd);
//  }
//
//  close(dataReceiveServerSocketFd);
//  pthread_exit(NULL);
//  return NULL;
//}
//
//void *Collector::_DataReceiveWorker(void *arg)
//{
//  int *socketFd = (int *)arg;
//  char buffer[1024];
//  stringstream ss;
//
//  int recvRet;
//  while((recvRet = recv(*socketFd, buffer, 1024, 0)) > 0)
//  {
////    ss << buffer;
//  }
//  if(recvRet < 0)
//  {
//    fprintf(stderr, "[%s] Receive data failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
//  }
//
////  cout << "[" << ss.str() << "]" << endl;
////  utility::MetaData metaData;
////  metaData.ParseFromString(ss.str());
////  cout << "uuid:" << metaData.monitoruuid() << endl;
////  cout << "json:" << metaData.jsonstring() << endl;
//
//  close(*socketFd);
//  pthread_exit(NULL);
//  return NULL;
//}

}

int main(int argc, char *argv[])
{
  using namespace std;
  using namespace event;

  int commandPort = 32100;
  int monitorCommandPort = 32101;

  if (argc < 1)
  {
    printf("\nusage: [command-port] [monitor-cmd-port]\n");
    printf("Options:\n");
    printf("\tcommand-port\t\t\tPort number of command service. Default is 32100.\n");
    printf("\tmonitor-cmd-port\t\tCommand port for remote monitors. Default is 32101.\n");
    exit(1);
  }

  if (argc >= 2)
    commandPort = atoi(argv[2]);

  if (argc >= 3)
    monitorCommandPort = atoi(argv[3]);

  Collector collector(commandPort, monitorCommandPort);
  string testQuery1 =
      "{'query_uuid': 'uuuu-uuuu', 'query-content': 'select all from all'}";
  string testQuery2 =
      "{'query_uuid': 'aaaa-aaaa', 'query-content': '\"Hello World!\"'}";
  collector.RegisterQuery(testQuery1, 2);
  collector.RegisterQuery(testQuery2, 5);
  collector.Run();

  return 0;
}
