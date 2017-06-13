INC_DIR = ../include
JOB_CPP = wordcount.cpp
BUILD_PREFIX = ../bin

CXX = g++
CFLAGS += -D _LOG -Wall -std=c++11 -I$(INC_DIR) -D _DEBUG
LDFLAGS += -shared -fPIC

all: job

job: $(JOB_CPP) $(INC_DIR)/*.hpp
	mkdir -p $(BUILD_PREFIX)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $(JOB_CPP)

.PHONY: clean
clean:
	rm -rf $(BUILD_PREFIX)/job
