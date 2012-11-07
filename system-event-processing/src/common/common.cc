/*
 * common.cc
 *
 *  Created on: Mar 15, 2012
 *      Author: yexijiang
 */

#include "common.h"

namespace event {

using namespace std;

int GetCPUCount() {
  cpu_set_t cs;
  CPU_ZERO(&cs);
  sched_getaffinity(0, sizeof(cs), &cs);
  int count = 0;
  for (int i = 0; i < 128; i++)
    if (CPU_ISSET(i, &cs))
      count++;
  return count;
}

void Split(const std::string &s, char c, std::vector<std::string> & v, bool trim) {
  std::stringstream ss(s);
  std::string item;
  while (std::getline(ss, item, c))
  {
    if(true == trim)
      item = Trim(item);
    v.push_back(item);
  }
}

};

