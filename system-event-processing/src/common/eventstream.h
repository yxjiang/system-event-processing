/*
 * eventstream.h
 *
 *  Created on: Nov 5, 2012
 *      Author: yxjiang
 */

#ifndef EVENTSTREAM_H_
#define EVENTSTREAM_H_

#include "pthread.h"
#include <list>
#include <boost/property_tree/json_parser.hpp>
#include <boost/property_tree/ptree.hpp>
#include <boost/shared_ptr.hpp>
#include "common.h"
#include <limits.h>


namespace event
{

/*
 * Use shared pointer to store the large property_tree object
 */
typedef boost::shared_ptr<boost::property_tree::ptree> SharedPtree;

/*!
 * The simple implementation of stream.
 * It simply leverages read-write lock.
 */
class SimpleLockStream
{
public:
  /*!
   * Initialize the stream.
   * \param bufferSize  The maximum size of the stream buffer.
   */
  SimpleLockStream();
  ~SimpleLockStream();

  void SetStreamBufferSize(size_t bufferSize);
  size_t GetStreamSize();
  /*!
   * Get the latest node.
   */
  boost::property_tree::ptree GetLatest();
  /*!
   * Get a snapshot of the stream. (Copy the whole stream data)
   * \return    A copy of current stream.
   */
  boost::shared_ptr<std::vector<SharedPtree> > GetStreamSnapshot();
  /*!
   * Push a new node into stream. All remove oldest node if stream is full.
   * \param tree    The property tree.
   */
  void AddData(boost::property_tree::ptree &tree);

private:
  size_t bufferSize_;
  pthread_rwlock_t wrLock_;
  pthread_rwlockattr_t wrLockAttr;
  std::list<SharedPtree> streamBuffer_; //  head is old, tail is new
};

}


#endif /* EVENTSTREAM_H_ */
