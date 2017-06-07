#ifndef DEF_H
#define DEF_H

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
#define ID_LONG '\x2'
#define ID_STRING '\x3'

#define CALL_SERVER 'S'
#define CALL_CLIENT 'C'
#define CALL_POLL 'P'
#define CALL_END 'E'

#define RES_SUCCESS 0
#define RES_FAIL '\x1'

#define INVALID_SOCKET (-1)
#define INVALID (-1)

#define STREAMMANAGER_PORT 8711
#define SERVER_PORT 8712
#define MAX_CONNECTION_ATTEMPT 50

#define BUFFER_SIZE 1024
#define DATA_BLOCK_SIZE 65536
#define MIN_VAL(a, b) ((a < b) ? a : b)

#define IPCONFIG_FILE "/ipconfig"
#define JOB_FILE "/job"

#endif
