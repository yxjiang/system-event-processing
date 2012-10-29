/*
 * common.h
 *
 *  Created on: Oct 28, 2012
 *      Author: yxjiang
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <ctime>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>
#include "string.h"

namespace event {

/*  sleep interval, 500 millisecond  */
const unsigned long int CRAWLER_SLEEP_TIME_NANOSEC = 500000000;

/*  sample interval, 500 millisecond */
const unsigned long int CRAWLER_SAMPLE_TIME_NANOSEC = 500000000;

/*!
 * Convert time_t data structure into string.
 * \param   timestamp    The timestamp needs to be transformed.
 * \return  The time in string format
 */
inline std::string TimeToString(const time_t &timestamp) {
  struct tm *time = gmtime(&timestamp);
  char out_buf[128];
  strftime(out_buf, 128, "%Y-%m-%d %Z %X", time);
  std::string str = out_buf;
  return str;
}

/*!
 * Get number of CPUs/
 * \return  The number of CPUs.
 */
int GetCPUCount();

/*!
 * Let the thread to sleep for a while.
 */
inline void ThreadSleep(long int sec, long int nanosec) {
  struct timespec sleep_interval;
  sleep_interval.tv_sec = sec;
  sleep_interval.tv_nsec = nanosec;
  struct timespec remain_sleep_interval;
  nanosleep(&sleep_interval, &remain_sleep_interval);
}

/*!
 * Convert the string into float.
 * \params  str The input string.
 * \params  value   The output float value.
 */
inline void StringToFloat(const std::string &str, float &value) {
  sscanf(str.c_str(), "%f", &value);
}

/*!
 * Split the string by specified delim.
 */
void Split(const std::string &s, char c, std::vector<std::string> & v);

/*!
 * Convert the number of bytes to KB.
 * \param   bytes   The number of bytes.
 * \return  The corresponding value of KB.
 */
inline double BytesToKB(int bytes) {
  return bytes / 1024;
}

/*!
 * Convert the number of bytes to MB.
 * \param   bytes   The number of bytes.
 * \return  The corresponding value of MB.
 */
inline double BytesToMB(double bytes) {
  return bytes / 1048576;
}

};

#endif /* COMMON_H_ */
