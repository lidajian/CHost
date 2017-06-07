SRC_SERVER = ./src/cHadoopServer.cpp
SRC_STARTER = ./src/cHadoopStarter.cpp

INC_DIR = ./include
BUILD_PREFIX = ./bin

CXX = g++
CFLAGS += -D _DEBUG -D _LOG -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread -ldl

all: cHadoopServer cHadoopStarter

cHadoopServer: $(SRC_SERVER) $(INC_DIR)/*.hpp
	mkdir -p $(BUILD_PREFIX)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o ./bin/$@ $(SRC_SERVER)

cHadoopStarter: $(SRC_STARTER) $(INC_DIR)/*.hpp
	mkdir -p $(BUILD_PREFIX)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o ./bin/$@ $(SRC_STARTER)

.PHONY: clean
clean:
	rm -rf bin/
