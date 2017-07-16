#ifndef DEF_H
#define DEF_H

typedef unsigned int uint;

#include <sstream>
#include <string>
#include <vector>

/********************************************
 ************** User Defined ****************
********************************************/


// Folders
constexpr char TEMP_DIR[]                   = "/.CHost";
constexpr char IPCONFIG_FILE[]              = "/ipconfig";
constexpr char JOB_FILE[]                   = "/job";

// Machine specific
constexpr size_t RANDOM_FILE_NAME_LENGTH    = 8;
constexpr size_t RANDOM_JOB_NAME_LENGTH     = 5;
constexpr size_t DEFAULT_MAX_DATA_SIZE      = 1000000;
constexpr size_t MERGE_SORT_WAY             = 16;
constexpr uint MAX_CONNECTION_ATTEMPT       = 15;
constexpr size_t SPLIT_SIZE                 = 65536;
constexpr size_t THREAD_POOL_SIZE           = 4;
constexpr uint NUM_MAPPER                   = 4;

// Ports (Uniform within clusters)
constexpr unsigned short STREAMMANAGER_PORT = 8711;
constexpr unsigned short SERVER_PORT        = 8712;

/********************************************
 ************** Do not modify ***************
********************************************/

typedef std::vector<std::pair<size_t, std::string> > ipconfig_t;

constexpr char LOCALHOST[]                  = "127.0.0.1";

#define INVALID_SOCKET (~0)
#define INVALID (~0)
#define BUFFER_SIZE 1024
#define CONNECTION_RETRY_INTERVAL 1 // seconds
#define ACCEPT_TIMEOUT 5 // seconds
#define RECEIVE_TIMEOUT 5 // seconds

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
#define E(x) puts(x)
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

// Macro functions
#define MIN(a, b) ((a < b) ? a : b)

#endif
