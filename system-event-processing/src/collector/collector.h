/*
 * collector.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef COLLECTOR_H_
#define COLLECTOR_H_

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <pthread.h>
#include <boost/shared_ptr.hpp>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include "../common/utility.pb.h"
#include "../common/common.h"

namespace event
{

class Collector
{
public:
  Collector(std::vector<std::string> vecPeerCollectorIPs, int communicationPort, int dataPort);
  ~Collector();

  void Run();

protected:
  static void *_DataReceiveService(void *);
  static void *_DataReceiveWorker(void *);

private:
  static int dataServicePort_;
  static bool dataServiceStop_;
  static pthread_rwlock_t stopSymbolrwlock_;

};

}


#endif /* COLLECTOR_H_ */
