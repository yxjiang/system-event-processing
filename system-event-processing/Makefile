#Makefile

# Created on: Mar 21, 2012
#     Author: yxjiang
INCLUDE := 

FLAG := -Wall

LIBS := -L/usr/lib/x86-64-linux-gnu -L/usr/local/lib -lpthread -lprotobuf

UTILITY_SRC := src/common/utility.pb.cc
UTILITY_OBJ := utility.o

CRAWLER_SRC := src/monitor/crawler.cc
MONITOR_SRC := src/monitor/monitor.cc

COMMON_SRC := src/common/common.cc
COMMON_OBJ := common.o
COMMON_TEST_SRC := test/common/common_test.cc
COMMON_TEST_OBJ := common-test.o

STREAM_SRC := src/common/eventstream.cc
STREAM_OBJ := eventstream.o

CRAWLER_OBJ := crawler.o
MONITOR_OBJ := monitor.o

COLLECTOR_SRC := src/collector/collector.cc
COLLECTOR_OBJ := collector.o

common: $(COMMON_SRC)
	g++ $(INCLUDE) $(FLAG) -c $(COMMON_SRC) -o $(COMMON_OBJ)

crawler: $(CRAWLER_SRC) common
	g++ $(INCLUDE) $(FLAG) -c $(CRAWLER_SRC) -o $(CRAWLER_OBJ)
	
eventstream: $(STREAM_SRC) common
	g++ $(INCLUDE) $(FLAG) -c $(STREAM_SRC) -o $(STREAM_OBJ)
	
monitor: crawler $(MONITOR_SRC) common eventstream
	g++ $(INCLUDE) $(FLAG) -c $(UTILITY_SRC) -o $(UTILITY_OBJ)
	g++ $(INCLUDE) $(FLAG) -c $(MONITOR_SRC) -o $(MONITOR_OBJ)
	g++ $(INCLUDE) $(FLAG) $(CRAWLER_OBJ) $(STREAM_OBJ) $(MONITOR_OBJ) $(COMMON_OBJ) $(UTILITY_OBJ) $(LIBS) -o monitor

collector: $(COLLECTOR_SRC) common
	g++ $(INCLUDE) $(FLAG) -c $(UTILITY_SRC) -o $(UTILITY_OBJ)
	g++ $(FLAG) -c $(COLLECTOR_SRC) -o $(COLLECTOR_OBJ)
	g++ $(FLAG) $(COLLECTOR_OBJ) $(COMMON_OBJ) $(UTILITY_OBJ) $(LIBS) -o collector

common-test: $(COMMON_TEST_SRC) $(COMMON_SRC)
	g++ $(FLAG) -c $(COMMON_SRC) -o $(COMMON_OBJ)
	g++ $(FLAG) -c $(COMMON_TEST_SRC) -o $(COMMON_TEST_OBJ)
	g++ $(FLAG) $(COMMON_OBJ) $(COMMON_TEST_OBJ) -o common-test

clean:
	rm *.o
	rm monitor collector common-test
