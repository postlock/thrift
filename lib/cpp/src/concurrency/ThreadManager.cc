#include "ThreadManager.h"
#include "Exception.h"
#include "Monitor.h"

#include <boost/shared_ptr.hpp>

#include <assert.h>
#include <queue>
#include <set>

#if defined(DEBUG)
#include <iostream>
#endif //defined(DEBUG)

namespace facebook { namespace thrift { namespace concurrency { 

using namespace boost;


/**
 * ThreadManager class
 * 
 * This class manages a pool of threads. It uses a ThreadFactory to create
 * threads.  It never actually creates or destroys worker threads, rather
 * it maintains statistics on number of idle threads, number of active threads,
 * task backlog, and average wait and service times.
 *
 * @author marc
 * @version $Id:$
 */
class ThreadManager::Impl : public ThreadManager  {

 public:
  Impl() : 
    _workerCount(0),
    _workerMaxCount(0),
    _idleCount(0),
    _state(ThreadManager::UNINITIALIZED) {}

  ~Impl() { stop(); }

  void start();

  void stop();

  const ThreadManager::STATE state() const { return _state; }

  shared_ptr<ThreadFactory> threadFactory() const {
    Synchronized s(_monitor); 
    return _threadFactory;
  }
      
  void threadFactory(shared_ptr<ThreadFactory> value) {  
    Synchronized s(_monitor);
    _threadFactory = value;
  }

  void addWorker(size_t value);
  
  void removeWorker(size_t value);
  
  size_t idleWorkerCount() const { return _idleCount; }

  size_t workerCount() const {
    Synchronized s(_monitor); 
    return _workerCount;
  }
  
  size_t pendingTaskCount() const {
    Synchronized s(_monitor); 
    return _tasks.size();
  }

  size_t totalTaskCount() const {
    Synchronized s(_monitor);   
    return _tasks.size() + _workerCount - _idleCount;
  }
  
  void add(shared_ptr<Runnable> value);

  void remove(shared_ptr<Runnable> task);

private:
  size_t _workerCount;
  size_t _workerMaxCount;
  size_t _idleCount;
  ThreadManager::STATE _state;
  shared_ptr<ThreadFactory> _threadFactory;


  friend class ThreadManager::Task;
  std::queue<shared_ptr<Task> > _tasks;
  Monitor _monitor;
  Monitor _workerMonitor;

  friend class ThreadManager::Worker;
  std::set<shared_ptr<Thread> > _workers;
  std::set<shared_ptr<Thread> > _deadWorkers;
};

class ThreadManager::Task : public Runnable {

 public:
  enum STATE {
    WAITING,
    EXECUTING,
    CANCELLED,
    COMPLETE
  };

  Task(shared_ptr<Runnable> runnable) :
    _runnable(runnable),
    _state(WAITING) {}

  ~Task() {}

  void run() {
    if (_state == EXECUTING) {
      _runnable->run();
      _state = COMPLETE;
    }
  }

 private:
  shared_ptr<Runnable> _runnable;
  friend class ThreadManager::Worker;
  STATE _state;
};

class ThreadManager::Worker: public Runnable {
  enum STATE {
    UNINITIALIZED,
    STARTING,
    STARTED,
    STOPPING,
    STOPPED
  };

 public:
  Worker(ThreadManager::Impl* manager) : 
    _manager(manager),
    _state(UNINITIALIZED),
    _idle(false) {}

  ~Worker() {}

  bool isActive() const { return _manager->_workerCount <= _manager->_workerMaxCount; }

  /**
   * Worker entry point
   *
   * As long as worker thread is running, pull tasks off the task queue and
   * execute.
   */
  void run() {
    bool active = false;  
    bool notifyManager = false;

    /**
     * Increment worker semaphore and notify manager if worker count reached
     * desired max
     *
     * Note: We have to release the monitor and acquire the workerMonitor
     * since that is what the manager blocks on for worker add/remove
     */
    {
      Synchronized s(_manager->_monitor);
      active = _manager->_workerCount < _manager->_workerMaxCount;
      if (active) {
	_manager->_workerCount++;
	notifyManager = _manager->_workerCount == _manager->_workerMaxCount;
      }
    }

    if (notifyManager) {
      Synchronized s(_manager->_workerMonitor);
      _manager->_workerMonitor.notify();
      notifyManager = false;
    }

    while (active) {
      shared_ptr<ThreadManager::Task> task;

      /**
       * While holding manager monitor block for non-empty task queue (Also
       * check that the thread hasn't been requested to stop). Once the queue
       * is non-empty, dequeue a task, release monitor, and execute. If the
       * worker max count has been decremented such that we exceed it, mark
       * ourself inactive, decrement the worker count and notify the manager
       * (technically we're notifying the next blocked thread but eventually
       * the manager will see it.
       */
      {
        Synchronized s(_manager->_monitor);
	active = isActive();
	while (active && _manager->_tasks.empty()) {
          _manager->_idleCount++;
	  _idle = true;
          _manager->_monitor.wait();
          active = isActive();
	  _idle = false;
          _manager->_idleCount--;
	}

	if (active) {
          if (!_manager->_tasks.empty()) {
            task = _manager->_tasks.front();
            _manager->_tasks.pop();
            if (task->_state == ThreadManager::Task::WAITING) {
              task->_state = ThreadManager::Task::EXECUTING;
	    }
	  }
	} else {
	  _idle = true;  
	  _manager->_workerCount--;
          notifyManager = _manager->_workerCount == _manager->_workerMaxCount;
	}
      }
      
      if (task != NULL) {
	if (task->_state == ThreadManager::Task::EXECUTING) {
	  try {
            task->run();
          } catch(...) {
            // XXX need to log this
	  }
	}
      }
    }
    
    {
      Synchronized s(_manager->_workerMonitor);    
      _manager->_deadWorkers.insert(this->thread());
      if (notifyManager) {
        _manager->_workerMonitor.notify();
      }
    }
    
    return;
  }
  
  private:
    ThreadManager::Impl* _manager;
    friend class ThreadManager::Impl;
    STATE _state;
    bool _idle;
};


  void ThreadManager::Impl::addWorker(size_t value) {
  std::set<shared_ptr<Thread> > newThreads;
  for (size_t ix = 0; ix < value; ix++) {
    class ThreadManager::Worker;     
    shared_ptr<ThreadManager::Worker> worker = shared_ptr<ThreadManager::Worker>(new ThreadManager::Worker(this));
    newThreads.insert(_threadFactory->newThread(worker));
  }

  {
    Synchronized s(_monitor);
    _workerMaxCount+= value;
    _workers.insert(newThreads.begin(), newThreads.end());
  }
  
  for (std::set<shared_ptr<Thread> >::iterator ix = newThreads.begin(); ix != newThreads.end(); ix++) {
    shared_ptr<ThreadManager::Worker> worker = dynamic_pointer_cast<ThreadManager::Worker, Runnable>((*ix)->runnable());
    worker->_state = ThreadManager::Worker::STARTING;
    (*ix)->start();
  }

  {
    Synchronized s(_workerMonitor); 
    while (_workerCount != _workerMaxCount) {
      _workerMonitor.wait();
    }
  }
}

void ThreadManager::Impl::start() {

  if (_state == ThreadManager::STOPPED) {
    return;
  }

  {
    Synchronized s(_monitor); 
    if (_state == ThreadManager::UNINITIALIZED) {
      if (_threadFactory == NULL) {
        throw InvalidArgumentException();
      }
      _state = ThreadManager::STARTED;
      _monitor.notifyAll();
    }

    while (_state == STARTING) {
      _monitor.wait();
    }
  }
}

void ThreadManager::Impl::stop() {
  bool doStop = false;
  if (_state == ThreadManager::STOPPED) {
    return;
  }

  {
    Synchronized s(_monitor); 
    if (!_state != ThreadManager::STOPPING && _state != ThreadManager::STOPPED) {
      doStop = true;
      _state = ThreadManager::STOPPING;
    }
  }

  if (doStop) {
    removeWorker(_workerCount);
    _state = ThreadManager::STOPPING;
  }

  // XXX 
  // should be able to block here for transition to STOPPED since we're no
  // using shared_ptrs
}
  
void ThreadManager::Impl::removeWorker(size_t value) {
  std::set<shared_ptr<Thread> > removedThreads;
  {
    Synchronized s(_monitor); 
    if (value > _workerMaxCount) {
      throw InvalidArgumentException();
    }

    _workerMaxCount-= value;

    if (_idleCount < value) {
      for (size_t ix = 0; ix < _idleCount; ix++) {
	_monitor.notify();
      }
    } else {
      _monitor.notifyAll();
    }
  }

  {
    Synchronized s(_workerMonitor); 

    while (_workerCount != _workerMaxCount) {
      _workerMonitor.wait();
    }

    for (std::set<shared_ptr<Thread> >::iterator ix = _deadWorkers.begin(); ix != _deadWorkers.end(); ix++) {
      _workers.erase(*ix);
    }
    
    _deadWorkers.clear();
  }
}
  
void ThreadManager::Impl::add(shared_ptr<Runnable> value) {
    Synchronized s(_monitor); 

    if (_state != ThreadManager::STARTED) {
      throw IllegalStateException();
    }

    _tasks.push(shared_ptr<ThreadManager::Task>(new ThreadManager::Task(value)));

    // If idle thread is available notify it, otherwise all worker threads are
    // running and will get around to this task in time.
    if (_idleCount > 0) {
      _monitor.notify();
    }
  }

void ThreadManager::Impl::remove(shared_ptr<Runnable> task) {
  Synchronized s(_monitor); 
  if (_state != ThreadManager::STARTED) {
    throw IllegalStateException();
  }
}

class SimpleThreadManager : public ThreadManager::Impl {

public:
  SimpleThreadManager(size_t workerCount=4) : 
    _workerCount(workerCount),
    _firstTime(true) {
  }

  void start() {
    ThreadManager::Impl::start();
    addWorker(_workerCount);
  }

private:
  const size_t _workerCount;
  bool _firstTime;
  Monitor _monitor;
};


shared_ptr<ThreadManager> ThreadManager::newThreadManager() {
  return shared_ptr<ThreadManager>(new ThreadManager::Impl());
}

shared_ptr<ThreadManager> ThreadManager::newSimpleThreadManager(size_t count) {
  return shared_ptr<ThreadManager>(new SimpleThreadManager(count));
}

}}} // facebook::thrift::concurrency
