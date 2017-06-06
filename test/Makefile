INC_DIR = ../include

CXX = g++
CFLAGS += -D _DEBUG -D _LOG -Wall -std=c++11 -I$(INC_DIR)
LDFLAGS += -lpthread

all: test_sm

test_sm: test_sm.cpp $(INC_DIR)/*.hpp
	$(CXX) $(CFLAGS) $(LDFLAGS) -o $@ test_sm.cpp

.PHONY: clean
clean:
	rm -rf test_sm $(OBJ_TEST_SM) test_sm.dSYM
