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

pthread_attr_t Collector::threadAttr_;
pthread_t Collector::commandServicePid_;
int Collector::commandServicePort_ = 32100;

int Collector::monitorCommandServicePort_ = 32100; //  default port for monitor to receive commands
pthread_t Collector::subscribeExecutorPid_;
map<boost::uuids::uuid, QueryProfile*> Collector::registeredQueryProfiles_;
pthread_rwlock_t Collector::registeredQueryProfileRwlock_;

//int Collector::dataServicePort_ = 32168;    //  default port number to receive data
//bool Collector::dataServiceStop_ = false;    //  data service is running by default

Collector::Collector(vector<string> vecPeerCollectorIPs, int communicationPort,
    int monitorCommunicationPort)
{
  monitorCommandServicePort_ = monitorCommunicationPort;
  //  suppress the protobuf logger
  google::protobuf::SetLogHandler(NULL);
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

  pthread_rwlock_init(&registeredQueryProfileRwlock_, NULL);

}

Collector::~Collector()
{
}

void Collector::Run()
{
  //  start command service to receive commands
  pthread_create(&commandServicePid_, NULL, _CommandService, NULL);

//  pthread_t dataServicePid;
//  pthread_create(&dataServicePid, &threadAttr, _DataReceiveService, NULL);

//  pthread_create(&subscribeExecutorPid_, NULL, _SubscribeExecutor, NULL);

//  pthread_join(dataServicePid, NULL);
  pthread_join(commandServicePid_, NULL);
//  pthread_join(subscribeExecutorPid_, NULL);
}

void *Collector::_CommandService(void *arg)
{
  struct sockaddr_in commandServiceAddr;
  bzero(&commandServiceAddr, sizeof(sockaddr_in));
  commandServiceAddr.sin_family = AF_INET;
  commandServiceAddr.sin_port = htons(commandServicePort_);
  commandServiceAddr.sin_addr.s_addr = htons(INADDR_ANY);

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

  int count = 0;
  while (true)
  {
    if (++count % 100 == 0)
      fprintf(stdout, "[%s] Received %d requests.\n", GetCurrentTime().c_str(),
          count);
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
  char contentBuffer[4096];
  int nbytes = 0;
  if((nbytes = recv(*connectionSocketFd, contentBuffer, sizeof(contentBuffer), 0)) < 0)
  {
    fprintf(stderr, "[%s] Receive command failed. Reason: %s.\n", GetCurrentTime().c_str(), strerror(errno));
    close(*connectionSocketFd);
    pthread_exit(NULL);
    return NULL;
  }
  fprintf(stdout, "[%s] Receive %s.\n", GetCurrentTime().c_str(), contentBuffer);
  pthread_exit(NULL);
  return NULL;
}


void Collector::RegisterQuery(const string &queryContent, int queryInterval)
{
  boost::uuids::uuid uuid = boost::uuids::random_generator()();
  QueryProfile *profile = new QueryProfile;
  profile->uuid = uuid;
  profile->lastCalled = -1;
  profile->queryContent = queryContent.c_str();
  profile->queryInterval = queryInterval;
  pthread_rwlock_wrlock(&registeredQueryProfileRwlock_);
  registeredQueryProfiles_.insert(
      make_pair<boost::uuids::uuid, QueryProfile*>(uuid, profile));
  pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
  fprintf(stdout, "[%s] New query registered: %s.\n", GetCurrentTime().c_str(),
      profile->queryContent);
}

//void *Collector::_SubscribeExecutor(void *arg)
//{
//  while (true)
//  {
//    time_t curTime;
//    time(&curTime);
//
//    map<boost::uuids::uuid, QueryProfile*>::iterator profileItr =
//        registeredQueryProfiles_.begin();
//    for (; profileItr != registeredQueryProfiles_.end(); ++profileItr)
//    {
//      QueryProfile *profile = profileItr->second;
//      if (profile->lastCalled == -1
//          || (curTime - profile->lastCalled == profile->queryInterval)) //  time is up
//      {
//        pthread_rwlock_wrlock(&registeredQueryProfileRwlock_);
//        profile->lastCalled = curTime;
//        pthread_rwlock_unlock(&registeredQueryProfileRwlock_);
//        pthread_t workerPid;
//        pthread_create(&workerPid, &threadAttr_, _SubscribeExecutorWorker,
//            (void *) profile->queryContent);
//      }
//    }
//    ThreadSleep(1, 0);
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
  int dataPort = 32168;

  if (argc < 2)
  {
    printf("\nusage: collector ips [command-port] [collector-data-port]\n");
    printf("Options:\n");
    printf(
        "\tips\t\t\t\tList of IPs of all the collectors, include the server itself, separated by ','.\n");
    printf(
        "\tcommand-port\t\t\tPort number of command service. Default is 32100.\n");
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
    commandPort = atoi(argv[2]);

  if (argc >= 4)
    dataPort = atoi(argv[3]);

  Collector collector(vecIPs, commandPort, dataPort);
  string testQuery1 =
      "{'query_uuid': 'uuuu-uuuu', 'query-content': 'select all from all'}";
  string testQuery2 =
      "{'query_uuid': 'aaaa-aaaa', 'query-content': '\"Hello World!\"'}";
  collector.RegisterQuery(testQuery1, 1);
  collector.RegisterQuery(testQuery2, 5);
  collector.Run();

  return 0;
}
