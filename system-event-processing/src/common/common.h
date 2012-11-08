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
#include <sstream>
#include <vector>
#include <boost/algorithm/string.hpp>
#include "string.h"

namespace event
{
///*  Maximum buffer size for UDP packet  */
const static unsigned int BUFFER_SIZE = 4096;
/*  sleep interval, 50 millisecond  */
const static unsigned long int CRAWLER_SLEEP_TIME_NANOSEC = 50000000;

/*  sample interval, 50 millisecond */
const static unsigned long int CRAWLER_SAMPLE_TIME_NANOSEC = 50000000;

//const static char *MULTICAST_GROUP = "225.0.0.66";
//const static int MULTICAST_PORT = 32100;

/*!
 * Convert time_t data structure into string.
 * \param   timestamp    The timestamp needs to be transformed.
 * \return  The time in string format
 */
inline std::string TimeToString(const time_t &timestamp)
{
  struct tm *time = gmtime(&timestamp);
  char out_buf[128];
  strftime(out_buf, 128, "%Y-%m-%d %Z %X", time);
  std::string str = out_buf;
  return str;
}

/*!
 * Get current time in string format.
 */
inline std::string GetCurrentTime()
{
  time_t curTime;
  time(&curTime);
  return TimeToString(curTime);
}

/*!
 * Get number of CPUs/
 * \return  The number of CPUs.
 */
int GetCPUCount();

/*!
 * Let the thread to sleep for a while.
 */
inline void ThreadSleep(long int sec, long int nanosec)
{
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
inline void StringToFloat(const std::string &str, float &value)
{
  sscanf(str.c_str(), "%f", &value);
}

/*!
 * Split the string by specified delim.
 */
void Split(const std::string &s, char c, std::vector<std::string> & v, bool trim);

inline std::string Trim(std::string &str)
{
  size_t s = str.find_first_not_of(" \n\r\t");
  size_t e = str.find_last_not_of(" \n\r\t");

  if ((std::string::npos == s) || (std::string::npos == e))
    return "";
  else
    return str.substr(s, e - s + 1);
}

/*!
 * Convert the number of bytes to KB.
 * \param   bytes   The number of bytes.
 * \return  The corresponding value of KB.
 */
inline double BytesToKB(int bytes)
{
  return bytes / 1024;
}

/*!
 * Convert the number of bytes to MB.
 * \param   bytes   The number of bytes.
 * \return  The corresponding value of MB.
 */
inline double BytesToMB(double bytes)
{
  return bytes / 1048576;
}

};

#endif /* COMMON_H_ */
