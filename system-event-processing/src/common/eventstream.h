/*
 * eventstream.h
 *
 *  Created on: Nov 5, 2012
 *      Author: yxjiang
 */

#ifndef EVENTSTREAM_H_
#define EVENTSTREAM_H_

#include "pthread.h"

namespace event
{

/*!
 * The simple implementation of stream.
 * It simply leverages read-write lock.
 */
class SimpleLockStream
{
public:
  SimpleLockStream(long bufferSize);

private:
  long bufferSize_;
  pthread_rwlock_t wrLock_;
};

}


#endif /* EVENTSTREAM_H_ */
