/*
 * WREN 
 *
 * Copyright 2018 Operating Systems Laboratory EPFL
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SCC_COMMON_WAIT_HANDLE_H_
#define SCC_COMMON_WAIT_HANDLE_H_

#include <list>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <boost/utility.hpp>
#include <string>

namespace scc {

// auto reset

class WaitHandle : boost::noncopyable {
public:

  WaitHandle(const char* name) :
    Name(name), _signal(false), _cv_lock(), _cv(), _count(1) {
  }

  WaitHandle() :
    Name("unset"), _signal(false), _cv_lock(), _cv(),_count(1) {
  }

    WaitHandle(const int count) :
      Name("unset"), _signal(false), _cv_lock(), _cv(),_count(count) {
    }


  void WaitAndReset() {
    std::unique_lock<std::mutex> lk(_cv_lock);
    while (!_signal) {
      _cv.wait(lk);
    }
    _signal = false;
  }

  // return value: true -> timeout

  bool WaitAndResetWithTimeout(int us) {
    bool timeout = false;
    std::cv_status stat = std::cv_status::no_timeout;

    {
      std::unique_lock<std::mutex> lk(_cv_lock);

      if (!_signal) {
        stat = _cv.wait_for(lk, std::chrono::microseconds(us));
      }

      if (stat == std::cv_status::timeout) {
        timeout = true;
        // _signal does not change if timeout
      } else {
        timeout = false;
        _signal = false;
      }
    }

    return timeout;
  }

  void Wait() {
    std::unique_lock<std::mutex> lk(_cv_lock);
    //Beware of spurious wakeups
    //http://en.cppreference.com/w/cpp/thread/condition_variable
    while (!_signal)
      _cv.wait(lk);
  }

    void DecrementAndWaitIfNonZero(){
      std::unique_lock<std::mutex> lk(_cv_lock);
      --_count;
      if(_count >0){
        //fprintf(stdout,"Going to wait after decrementing\n");
        while(!_signal)
          _cv.wait(lk);
      }/*else{
        fprintf(stdout,"NOT Going to wait after decrementing\n");
      }*/
    }

    void WaitIfNonZero(){
      std::unique_lock<std::mutex> lk(_cv_lock);
      if(_count >0){
       // fprintf(stdout,"Going to wait w/o decrementing\n");
        while(!_signal)
          _cv.wait(lk);
      }/*else{
        fprintf(stdout,"NOT Going to wait w/o decrementing\n");
      }*/
    }

    void SetIfCountZero(){
      std::unique_lock<std::mutex> lk(_cv_lock);
      --_count;
      if(_count == 0){
        while (_signal == false) {
          _signal = true;
          _cv.notify_all();
        }
      }
    }

  void Set() {
    std::unique_lock<std::mutex> lk(_cv_lock);
    while (_signal == false) {
      _signal = true;
      _cv.notify_all();
    }
  }

  static void WaitAll(std::list<WaitHandle*>& handles) {
    std::list<WaitHandle*>::iterator it;
    for (it = handles.begin(); it != handles.end(); it++)
      (*it)->WaitAndReset();
  }

  static void WaitAll(std::vector<WaitHandle*>& handles) {
    for (unsigned int i = 0; i < handles.size(); i++) {
      handles[i]->WaitAndReset();
    }
  }
public:
  std::string Name;

private:
  bool _signal;
  std::mutex _cv_lock;
  std::condition_variable _cv;
  int _count;
};

} // namespace scc

#endif
