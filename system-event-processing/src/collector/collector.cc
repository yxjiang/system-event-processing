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

int Collector::dataServicePort_ = 32168;    //  default port number to receive data
bool Collector::dataServiceStop_ = false;    //  data service is running by default
pthread_rwlock_t Collector::stopSymbolrwlock_;

Collector::Collector(vector<string> vecPeerCollectorIPs, int communicationPort, int dataPort)
{
  dataServicePort_ = dataPort;
  //  suppress the protobuf logger
  google::protobuf::SetLogHandler(NULL);

  pthread_rwlock_init(&stopSymbolrwlock_, NULL);
}

Collector::~Collector()
{
  pthread_rwlock_destroy(&stopSymbolrwlock_);
}

void Collector::Run()
{
  pthread_t dataServicePid;
  pthread_create(&dataServicePid, NULL, _DataReceiveService, NULL);

  pthread_join(dataServicePid, NULL);
}

void *Collector::_DataReceiveService(void *arg)
{
  struct sockaddr_in dataRecieveServiceAddr; // Server Internet address
  //  initialize server address
  bzero(&dataRecieveServiceAddr, sizeof(dataRecieveServiceAddr));
  dataRecieveServiceAddr.sin_family = AF_INET;
  dataRecieveServiceAddr.sin_addr.s_addr = htons(INADDR_ANY);
  dataRecieveServiceAddr.sin_port = htons(dataServicePort_);

  int dataReceiveServerSocketFd = socket(AF_INET, SOCK_STREAM, 0);
  if (dataReceiveServerSocketFd < 0)
  {
    fprintf(stderr, "[%s] Collector data receive service creates socket failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  else
    fprintf(stdout, "[%s] Collector data receive service socket created...\n", GetCurrentTime().c_str());

  //  bind socket and address
  if (bind(dataReceiveServerSocketFd, (struct sockaddr*) &dataRecieveServiceAddr, sizeof(dataRecieveServiceAddr)))
  {
    fprintf(stderr, "[%s] Collector data receive service bind port: %d failed.\n", GetCurrentTime().c_str(), dataServicePort_);
    close(dataReceiveServerSocketFd);
    exit(1);
  }
  else
    fprintf(stdout, "[%s] Collector data receive service port binded to port %d...\n", GetCurrentTime().c_str(), dataServicePort_);

  //  listen
  if (listen(dataReceiveServerSocketFd, 500))
  {
    fprintf(stderr, "[%s] Collector data receive service listen failed.\n", GetCurrentTime().c_str());
    close(dataReceiveServerSocketFd);
    exit(1);
  }
  else
    fprintf(stdout, "[%s] Collector data receive service listening on port %d...\n", GetCurrentTime().c_str(), dataServicePort_);

  while (true)
  {
    int connectionSockedFd = accept(dataReceiveServerSocketFd, NULL, 0);

    //  create worker to receive data
    pthread_t dataReceiveWorkerPid;
    pthread_create(&dataReceiveWorkerPid, NULL, _DataReceiveWorker, (void *)&connectionSockedFd);
  }

  close(dataReceiveServerSocketFd);
  return NULL;
}

void *Collector::_DataReceiveWorker(void *arg)
{
  int *socketFd = (int *)arg;
  char buffer[1024];
  stringstream ss;

  int recvRet;
  while((recvRet = recv(*socketFd, buffer, 1024, 0)) > 0)
  {
    ss << buffer;
  }
  if(recvRet < 0)
  {
    fprintf(stderr, "[%s] Receive data failed.\n", GetCurrentTime().c_str());
    perror("error");
  }

//  cout << "[" << ss.str() << "]" << endl;
  utility::MetaData metaData;
  metaData.ParseFromString(ss.str());
  cout << "uuid:" << metaData.monitoruuid() << endl;
  cout << "json:" << metaData.jsonstring() << endl;

  close(*socketFd);
  return NULL;
}

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
