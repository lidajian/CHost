#ifndef DEF_H
#define DEF_H

#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Debug output
#ifndef _DEBUG
#define D(x)
#define DSS(x)
#else
#define D(x) perror(x)
#define DSS(x) \
std::ostringstream tss; \
tss << x; \
D(tss.str().c_str())
#endif

// Error output
#ifndef _ERROR
#define E(x)
#define ESS(x)
#else
#define E(x) perror(x)
#define ESS(x) \
std::ostringstream tss; \
tss << x; \
E(tss.str().c_str())
#endif

// Informative output
#ifndef _SUGGEST
#define I(x)
#define ISS(x)
#else
#define I(x) puts(x)
#define ISS(x) \
std::ostringstream tss; \
tss << x; \
I(tss.str().c_str())
#endif

// Puts (puts anyway)
#define P(x) puts(x)
#define PSS(x) \
std::ostringstream tss; \
tss << x; \
P(tss.str().c_str())

// Definition of object id
#define ID_INVALID 0
#define ID_INTEGER '\x1'
#define ID_STRING '\x2'

// RPC symbols
#define CALL_MASTER 'M'
#define CALL_WORKER 'W'
#define CALL_POLL 'P'

// Success/Fail symbols
#define RES_SUCCESS 0
#define RES_FAIL '\x1'

#define INVALID_SOCKET (~0)
#define INVALID (~0)

// Ports
#define STREAMMANAGER_PORT 8711
#define SERVER_PORT 8712

// Macro functions
#define MIN_VAL(a, b) ((a < b) ? a : b)
#define MAX_VAL(a, b) ((a > b) ? a : b)
#define IS_ESCAPER(c) (c == '\r' || c == '\n')
#define LENGTH_CONST_CHAR_ARRAY(s) (sizeof(s) - 1) // exclude '\0'

// Constants
#define TEMP_DIR "/.CHost"
#define IPCONFIG_FILE "/ipconfig"
#define JOB_FILE "/job"
#define LOCALHOST "127.0.0.1"

#define RANDOM_FILE_NAME_LENGTH 8
#define DEFAULT_MAX_DATA_SIZE 1000000
#define MERGE_SORT_WAY 16
#define OPEN_FILESTREAM_RETRY_INTERVAL 60 // seconds
#define CONNECTION_RETRY_INTERVAL 1 // seconds
#define ACCEPT_TIMEOUT 5 // seconds
#define MAX_CONNECTION_ATTEMPT 15
#define BUFFER_SIZE 1024
#define DATA_BLOCK_SIZE 65536
#define THREAD_POOL_SIZE 4

// Enable epoll on Linux
#ifdef __gnu_linux__
#define __CH_EPOLL__
#endif

// Enable kqueue on FreeBSD
#ifdef __MACH__
#define __CH_KQUEUE__
#endif

// Enable kqueue on FreeBSD
#ifdef __FreeBSD__
#define __CH_KQUEUE__
#endif

// Include header for epoll
#if defined (__CH_EPOLL__)

#include <sys/epoll.h>

// Include header for kqueue
#elif defined (__CH_KQUEUE__)

#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

// Include header for select
#else

#include <sys/select.h>

#endif

typedef std::vector<std::pair<size_t, std::string> > ipconfig_t;

#endif
