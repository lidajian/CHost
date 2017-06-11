INC_DIR = ../include
JOB_CPP = wordcount.cpp

CXX = g++
CFLAGS += -D _LOG -Wall -std=c++11 -I$(INC_DIR)# -D _DEBUG
LDFLAGS += -shared -fPIC

all: job

job: $(JOB_CPP) $(INC_DIR)/*.hpp
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ $(JOB_CPP)

.PHONY: clean
clean:
	rm -rf job
