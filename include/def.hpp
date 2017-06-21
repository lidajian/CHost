#ifndef DEF_H
#define DEF_H

#include <iostream>
#include <string>
#include <vector>

// debug output
#ifndef _DEBUG
#define D(x)
#else
#define D(x) std::cout << x
#endif

// log output
#ifndef _LOG
#define L(x)
#else
#define L(x) std::cout << x
#endif

#define ID_INVALID 0
#define ID_INTEGER '\x1'
#define ID_STRING '\x2'

#define CALL_MASTER 'M'
#define CALL_WORKER 'W'
#define CALL_POLL 'P'

#define RES_SUCCESS 0
#define RES_FAIL '\x1'

#define INVALID_SOCKET (~0)
#define INVALID (~0)

#define STREAMMANAGER_PORT 8711
#define SERVER_PORT 8712
#define MAX_CONNECTION_ATTEMPT 15

#define BUFFER_SIZE 1024
#define DATA_BLOCK_SIZE 65536
#define MIN_VAL(a, b) ((a < b) ? a : b)
#define IS_ESCAPER(c) (c == '\r' || c == '\n')
#define LENGTH_CONST_CHAR_ARRAY(s) (sizeof(s) - 1) // exclude '\0'

#define TEMP_DIR "/.CHost"
#define IPCONFIG_FILE "/ipconfig"
#define JOB_FILE "/job"

#define RANDOM_FILE_NAME_LENGTH 8
#define DEFAULT_MAX_DATA_SIZE 1000000
#define MERGE_SORT_WAY 16
#define OPEN_FILESTREAM_RETRY_INTERVAL 60 // seconds
#define CONNECTION_RETRY_INTERVAL 1 // seconds
#define ACCEPT_TIMEOUT 5 // seconds

typedef std::vector<std::pair<size_t, std::string> > ipconfig_t;

#endif
