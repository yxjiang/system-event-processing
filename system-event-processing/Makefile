#Makefile

# Created on: Mar 21, 2012
#     Author: yxjiang
INCLUDE := -I/usr/local/include/boost

FLAG := -Wall

LIBS := -L/usr/lib/x86-64-linux-gnu -lpthread

CRAWLER_SRC := src/monitor/crawler.cc
MANAGER_SRC := src/monitor/manager.cc

COMMON_SRC := src/common/common.cc
COMMON_OBJ := common.o
COMMON_TEST_SRC := test/common/common_test.cc
COMMON_TEST_OBJ := common-test.o

CRAWLER_OBJ := crawler.o
MANAGER_OBJ := manager.o

COLLECTOR_SRC := src/collector/collector.cc
COLLECTOR_OBJ := collector.o


monitor: $(CRAWLER_SRC) $(MANAGER_SRC) $(COMMON_SRC)
	g++ $(INCLUDE) $(FLAG) -c $(COMMON_SRC) -o $(COMMON_OBJ)
	g++ $(INCLUDE) $(FLAG) -c $(CRAWLER_SRC) -o $(CRAWLER_OBJ)
	g++ $(INCLUDE) $(FLAG) -c $(MANAGER_SRC) -o $(MANAGER_OBJ)
	g++ $(INCLUDE) $(FLAG) $(CRAWLER_OBJ) $(MANAGER_OBJ) $(COMMON_OBJ) $(LIBS) -o monitor

collector: $(COLLECTOR_SRC) $(COMMON_SRC)
	g++ $(INCLUDE) $(FLAG) -c $(COMMON_SRC) -o $(COMMON_OBJ)
	g++ $(FLAG) -c $(COLLECTOR_SRC) -o $(COLLECTOR_OBJ)
	g++ $(FLAG) $(COLLECTOR_OBJ) $(COMMON_OBJ) $(LIBS) -o collector

common-test: $(COMMON_TEST_SRC) $(COMMON_SRC)
	g++ $(FLAG) -c $(COMMON_SRC) -o $(COMMON_OBJ)
	g++ $(FLAG) -c $(COMMON_TEST_SRC) -o $(COMMON_TEST_OBJ)
	g++ $(FLAG) $(COMMON_OBJ) $(COMMON_TEST_OBJ) -o common-test

clean:
	rm *.o
	rm monitor collector common-test
