
#if defined(POSIX_THREADS)
#   include "pthread.h"
#   define Mutex pthread_mutex_t
#   define make_mutex(mutex) pthread_mutex_init (mutex, NULL)
#   define lock(mutex) pthread_mutex_lock (mutex)
#   define unlock(mutex) pthread_mutex_unlock (mutex)
#   define Key pthread_key_t
#   define make_tsd(key) pthread_keycreate (key, (void *) 0)
#   define get_tsd(key,ptr) pthread_getspecific (key, (void *) (ptr))
#   define set_tsd(key,ptr) pthread_setspecific (key, ptr)
#elif defined(SOLARIS_THREADS)
#   include "thread.h"
#   define Mutex mutex_t
#   define make_mutex(mutex) mutex_init (mutex, USYNC_THREAD, 0)
#   define lock(mutex) mutex_lock (mutex)
#   define unlock(mutex) mutex_unlock (mutex)
#   define Key thread_key_t
#   define make_tsd(key) thr_keycreate (key, (void *) 0)
#   define get_tsd(key,ptr) thr_getspecific (key, (void *) (ptr))
#   define set_tsd(key,ptr) thr_setspecific (key, ptr)
/* define exit(status) thr_exit ((void *) (status)) */
#else
#   error Compile with either POSIX_THREADS or SOLARIS_THREADS defined.
#endif
