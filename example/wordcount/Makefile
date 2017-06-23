INC_DIR = ../../include
JOB_CPP = wordcount.cpp
BUILD_PREFIX = ../../bin
OBJ = wordcount

CXX = g++
CFLAGS += -D _SUGGEST -D _ERROR -Wall -std=c++11 -I$(INC_DIR)# -D _DEBUG
LDFLAGS += -shared -fPIC

$(OBJ): $(JOB_CPP) $(INC_DIR)/*.hpp
	mkdir -p $(BUILD_PREFIX)
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $(BUILD_PREFIX)/$@ $(JOB_CPP)

.PHONY: clean
clean:
	rm -rf $(BUILD_PREFIX)/$(OBJ)
	@(rmdir $(BUILD_PREFIX) 2> /dev/null; exit 0)
