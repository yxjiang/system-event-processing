/*
 * eventstream.cc
 *
 *  Created on: Nov 6, 2012
 *      Author: yexijiang
 */

#include "eventstream.h"

using namespace std;

namespace event
{

SimpleLockStream::SimpleLockStream()
{
  int ret = pthread_rwlockattr_init(&this->wrLockAttr);
  if(ret < 0)
  {
    fprintf(stderr, "[%s] Initialize read write lock attribute failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  ret = pthread_rwlockattr_setkind_np(&this->wrLockAttr, PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
  if(ret < 0)
  {
    fprintf(stderr, "[%s] Set read write lock attribute failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  ret = pthread_rwlock_init(&this->wrLock_, &this->wrLockAttr);
  if(ret < 0)
  {
    fprintf(stderr, "[%s] Initialize read write lock failed.\n", GetCurrentTime().c_str());
    exit(1);
  }
  this->bufferSize_ = 60;
}

SimpleLockStream::~SimpleLockStream()
{
  pthread_rwlock_destroy(&this->wrLock_);
  pthread_rwlockattr_destroy(&this->wrLockAttr);
}

void SimpleLockStream::SetStreamBufferSize(size_t bufferSize)
{
  this->bufferSize_ = bufferSize_;
}

size_t SimpleLockStream::GetStreamSize()
{
  pthread_rwlock_rdlock(&this->wrLock_);
  size_t size = streamBuffer_.size();
  pthread_rwlock_unlock(&this->wrLock_);
  return size;
}

boost::property_tree::ptree SimpleLockStream::GetLatest()
{
  pthread_rwlock_rdlock(&this->wrLock_);
  boost::property_tree::ptree lastPtree = *streamBuffer_.back().get();
  pthread_rwlock_unlock(&this->wrLock_);
  return lastPtree;
}

boost::shared_ptr<std::vector<SharedPtree> > SimpleLockStream::GetStreamSnapshot()
{
  pthread_rwlock_rdlock(&this->wrLock_);
  boost::shared_ptr<vector<SharedPtree> > snapshot(new vector<SharedPtree>(streamBuffer_.begin(), streamBuffer_.end()));
  pthread_rwlock_unlock(&this->wrLock_);
  return snapshot;
}

void SimpleLockStream::AddData(boost::property_tree::ptree &tree)
{
  SharedPtree sharedPtree(new boost::property_tree::ptree(tree));
  time_t cur_time = sharedPtree->get<long int>("timestamp");
  //  ignore redundant ptree
  if (streamBuffer_.size() != 0)
  {
    time_t tail_time = streamBuffer_.back()->get<long int>("timestamp");  //  since current thread is the only writer, no lock is needed at this time
    if (cur_time == tail_time)
      return;
  }
  pthread_rwlock_wrlock(&this->wrLock_);
  if(this->streamBuffer_.size() == this->bufferSize_)
    this->streamBuffer_.pop_front();
  this->streamBuffer_.push_back(sharedPtree);
  pthread_rwlock_unlock(&this->wrLock_);
}

}


