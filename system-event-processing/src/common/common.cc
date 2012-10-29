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
  for (int i = 0; i < 32; i++)
    if (CPU_ISSET(i, &cs))
      count++;
  return count;
}

void Split(const std::string &s, char c, std::vector<std::string> & v) {
  int i = 0;
  int j = s.find(c);
  while (j >= 0) {
    v.push_back(s.substr(i, j - i));
    i = ++j;
    j = s.find(c, j);
    if (j < 0)
      v.push_back(s.substr(i, s.length()));
  }
}

};

