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

pthread_attr_t Collector::threadAttr;
int Collector::monitorCommunicationServicePort_ = 32100;  //  default port for monitor to receive commands
pthread_t Collector::subscribeExecutorPid_;
map<boost::uuids::uuid, QueryProfile> Collector::registeredQueryProfiles_;
pthread_rwlock_t Collector::registeredQueryProfileRwlock_;

//int Collector::dataServicePort_ = 32168;    //  default port number to receive data
//bool Collector::dataServiceStop_ = false;    //  data service is running by default




Collector::Collector(vector<string> vecPeerCollectorIPs, int communicationPort, int monitorCommunicationPort)
{
  monitorCommunicationServicePort_ = monitorCommunicationPort;
  //  suppress the protobuf logger
  google::protobuf::SetLogHandler(NULL);
  int ret = pthread_attr_init(&threadAttr);
  if(ret != 0)
  {
    fprintf(stderr, "[%s] Initialize thread attribute failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }
  ret = pthread_attr_setdetachstate(&threadAttr, PTHREAD_CREATE_DETACHED);
  if(ret != 0)
  {
    fprintf(stderr, "[%s] Set thread attribute failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    exit(1);
  }

  pthread_rwlock_init(&registeredQueryProfileRwlock_, NULL);

}

Collector::~Collector()
{
}

void Collector::Run()
{
//  pthread_t dataServicePid;
//  pthread_create(&dataServicePid, &threadAttr, _DataReceiveService, NULL);

  pthread_create(&subscribeExecutorPid_, NULL, _SubscribeExecutor, NULL);

//  pthread_join(dataServicePid, NULL);
  pthread_join(subscribeExecutorPid_, NULL);
}

void Collector::RegisterQuery(string &queryContent, int queryInterval)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  QueryProfile profile;
  profile.uuid = uuid;
  profile.lastCalled = -1;
  profile.queryContent = queryContent;
  profile.queryInterval = queryInterval;
  pthread_rwlock_wrlock(&registeredQueryProfileRwlock_);
  registeredQueryProfiles_.insert(make_pair<boost::uuids::uuid, QueryProfile>(uuid, profile));
  pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
}

void *Collector::_SubscribeExecutor(void *arg)
{
  while(true)
  {
    time_t curTime;
    time(&curTime);

    pthread_rwlock_rdlock(&registeredQueryProfileRwlock_);
    map<boost::uuids::uuid, QueryProfile> profileCopy = registeredQueryProfiles_;
    pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
    map<boost::uuids::uuid, QueryProfile>::iterator profileItr = profileCopy.begin();
    for(; profileItr != profileCopy.end(); ++profileItr)
    {
      QueryProfile profile = profileItr->second;
      if(profile.lastCalled == -1 || (curTime - profile.lastCalled == profile.queryInterval))   //  time is up
      {
        pthread_t workerPid;
        pthread_create(&workerPid, &threadAttr, _SubscribeExecutorWorker, (void *)profile.queryContent);
      }
    }
    ThreadSleep(1, 0);
  }

  pthread_exit(NULL);
  return NULL;
}

void *Collector::_SubscribeExecutorWorker(void *arg)
{
  char *queryContent = (char *)arg;
  struct sockaddr_in monitorCommandServiceAddr;
  bzero(&monitorCommandServiceAddr, sizeof(sockaddr_in));
  monitorCommandServiceAddr.sin_family = AF_INET;
  monitorCommandServiceAddr.sin_addr.s_addr = inet_addr(MULTICAST_GROUP);
  monitorCommandServiceAddr.sin_port = htons(MULTICAST_PORT);

  int socketFd;
  if((socketFd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
  {
      fprintf(stderr, "[%s] Create socket failed when multicast the query. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
      pthread_exit(NULL);
      return NULL;
  }

  if(sendto(socketFd, queryContent, strlen(queryContent), 0, (struct sockaddr *)&monitorCommandServiceAddr, sizeof(monitorCommandServiceAddr)) < 0)
  {
    fprintf(stderr, "[%s] Multicast failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
  }

  pthread_exit(NULL);
  return NULL;
}

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

  int commandPort = 32167;
  int dataPort = 32168;

  if(argc < 2)
  {
    printf("\nusage: collector ips [command-port] [collector-data-port]\n");
    printf("Options:\n");
    printf("\tips\t\t\t\tList of IPs of all the collectors, include the server itself, separated by ','.\n");
    printf("\tcommand-port\t\t\tPort number of command service. Default is 32100.\n");
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
    commandPort = atoi(argv[2]);

  if(argc >= 4)
    dataPort = atoi(argv[3]);

  Collector collector(vecIPs, commandPort, dataPort);
  collector.Run();

  return 0;
}
