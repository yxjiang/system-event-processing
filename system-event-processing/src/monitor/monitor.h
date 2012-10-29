/*
 * monitor.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef MONITOR_H_
#define MONITOR_H_

namespace event
{

typedef struct
{
  Crawler *crawler;
  bool running;
  pthread_t pid;
} CrawlerStatus;

}

#endif /* MONITOR_H_ */
